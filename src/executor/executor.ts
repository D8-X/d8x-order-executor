import {
  PerpetualDataHandler,
  OrderExecutorTool,
  Order,
  ZERO_ORDER_ID,
  ORDER_TYPE_MARKET,
  SmartContractOrder,
  ZERO_ADDRESS,
  LimitOrderBook,
} from "@d8x/perpetuals-sdk";
import { ContractTransaction, Wallet, utils } from "ethers";
import { providers } from "ethers";
import { Redis } from "ioredis";
import { constructRedis, executeWithTimeout, sleep } from "../utils";
import {
  BotStatus,
  ExecutorConfig,
  TradeMsg,
  ExecuteOrderCommand,
} from "../types";
import { ExecutorMetrics } from "./metrics";
import { getTxRevertReason, sendTxRevertedMessage } from "./reverts";
import Distributor from "./distributor";

export default class Executor {
  // objects
  private providers: providers.StaticJsonRpcProvider[];
  private bots: { api: OrderExecutorTool; busy: boolean }[];
  private redisSubClient: Redis;

  // parameters
  private treasury: string;
  private privateKey: string[];
  private config: ExecutorConfig;

  // gas limit tuning
  private originalGasLimit: number;
  private gasLimitIncreaseFactor = 1.25;
  protected gasLimitIncreaseCounter: number = 0;

  // state
  private q: Set<ExecuteOrderCommand> = new Set();
  private locked: Set<string> = new Set();
  private lastCall: number = 0;
  private timesTried: Map<string, number> = new Map();
  private trash: Set<string> = new Set();

  protected metrics: ExecutorMetrics;

  protected distributor: Distributor | undefined;

  constructor(
    pkTreasury: string,
    pkLiquidators: string[],
    config: ExecutorConfig
  ) {
    this.metrics = new ExecutorMetrics();
    this.metrics.start();

    this.treasury = pkTreasury;
    this.privateKey = pkLiquidators;
    this.config = config;
    this.originalGasLimit = this.config.gasLimit;
    this.redisSubClient = constructRedis("executorSubClient");
    this.providers = this.config.rpcExec.map(
      (url) => new providers.StaticJsonRpcProvider(url)
    );

    const sdkConfig = PerpetualDataHandler.readSDKConfig(this.config.sdkConfig);

    // Use price feed endpoints from user specified config
    if (this.config.priceFeedEndpoints.length > 0) {
      sdkConfig.priceFeedEndpoints = this.config.priceFeedEndpoints;
      console.log(
        "Using user specified price feed endpoints",
        sdkConfig.priceFeedEndpoints
      );
    } else {
      console.warn(
        "No price feed endpoints specified in config. Using default endpoints from SDK.",
        sdkConfig.priceFeedEndpoints
      );
    }

    this.bots = this.privateKey.map((pk) => ({
      api: new OrderExecutorTool(sdkConfig, pk),
      busy: false,
    }));
  }
  /**
   * Attempts to connect to the blockchain using all given RPC providers until one works.
   * An error is thrown if none of the providers works.
   */
  public async initialize() {
    // Create a proxy instance to access the blockchain
    let success = false;
    let i = Math.floor(Math.random() * this.providers.length);
    let tried = 0;
    // try all providers until one works, reverts otherwise
    // console.log(`${new Date(Date.now()).toISOString()}: initializing ...`);
    while (
      !success &&
      i < this.providers.length &&
      tried <= this.providers.length
    ) {
      i = (i + 1) % this.providers.length;
      tried++;
      const results = await Promise.allSettled(
        // createProxyInstance attaches the given provider to the object instance
        this.bots.map((bot) => bot.api.createProxyInstance(this.providers[i]))
      );
      success = results.every((r) => r.status === "fulfilled");
    }
    if (!success) {
      throw new Error("critical: all RPCs are down");
    }

    // Subscribe to relayed events
    await this.redisSubClient.subscribe(
      "block",
      "TradeEvent",
      "Restart",
      (err, count) => {
        if (err) {
          console.log(
            `${new Date(
              Date.now()
            ).toISOString()}: redis subscription failed: ${err}`
          );
          process.exit(1);
        }
      }
    );
    console.log({
      info: "initialized",
      rpcUrl: this.config.rpcExec[i],
      time: new Date(Date.now()).toISOString(),
    });
  }

  // Add given msg to execution queue. Should be called directly from
  // distributor
  public async ExecuteOrder(msg: ExecuteOrderCommand) {
    this.q.add(msg);
    await this.execute();
  }

  /**
   * Subscribes to liquidation opportunities and attempts to liquidate.
   */
  public async run(): Promise<void> {
    // consecutive responses
    let [busy, errors, success, msgs] = [0, 0, 0, 0];
    console.log({
      info: "running",
      time: new Date(Date.now()).toISOString(),
    });
    return new Promise<void>((resolve, reject) => {
      setInterval(async () => {
        await this.execute();
      }, this.config.executeIntervalSecondsMax * 1_000);

      setInterval(async () => {
        const trash = [...this.trash];
        this.trash = new Set();
        for (const digest of trash) {
          for (const order of this.q) {
            if (order.digest === digest) {
              this.q.delete(order);
            }
          }
          this.locked.delete(digest);
        }
      }, 60 * 60 * 8 * 1_000);

      this.redisSubClient.on("message", async (channel, msg) => {
        switch (channel) {
          case "block": {
            if (+msg % 1000 == 0) {
              console.log(
                JSON.stringify(
                  {
                    busy: busy,
                    errors: errors,
                    success: success,
                    msgs: msgs,
                    time: new Date(Date.now()).toISOString(),
                  },
                  undefined,
                  "  "
                )
              );
            }
            break;
          }

          case "TradeEvent": {
            const { digest }: TradeMsg = JSON.parse(msg);
            this.locked.add(digest.toLowerCase());
            this.trash.add(digest.toLowerCase());
            break;
          }

          case "Restart": {
            process.exit(0);
          }
        }
      });
    });
  }

  // Checks if onchainOrder has all its dependencies resolved and order
  // dependency information is loaded.
  private checkOrderDependenciesResolved(onchainOrder: Order): boolean {
    if (onchainOrder.parentChildOrderIds) {
      if (
        onchainOrder.parentChildOrderIds[0] == ZERO_ORDER_ID &&
        !this.distributor?.openOrders.has(onchainOrder.parentChildOrderIds[1])
      ) {
        return true;
      }
    }

    // Market order is allowed to not have dependencies loaded. This might
    // happen on xlayer as the data is not available immediately. Other orders
    // should not be executed if dependencies are not loaded.
    if (onchainOrder.type == ORDER_TYPE_MARKET) {
      return true;
    }

    // All other orders must have their dependencies fetched.
    return false;
  }

  // getOrderByIdWithDependencies loads order with its dependencies from the
  // smart contract
  private async getOrderByIdWithDependencies(
    symbol: string,
    digest: string,
    selectedExecutorTool: OrderExecutorTool
  ): Promise<Order | undefined> {
    // We can't bundle retrieval of orderbook sc and order in one go from
    // getOrderById, so therefore we do this twice here.
    const ob = selectedExecutorTool.getOrderBookContract(symbol);
    const order = await selectedExecutorTool.getOrderById(symbol, digest);

    // Make sure dependencies are fetched after order is fetched to introduce a
    // slight 1 network call delay (xlayer chain problem)
    const deps = await ob.orderDependency(digest);
    if (order && deps) {
      order.parentChildOrderIds = [deps[0], deps[1]];
    }

    return order;
  }

  /**
   * Executes an order using a given bot.
   * @param botIdx Index of wallet used
   * @param symbol Perpetual symbol
   * @param digest Order digest/id
   * @param onChain true if order was seen on-chain
   * @returns true if execution does not revert
   */
  private async executeOrderByBot(
    botIdx: number,
    symbol: string,
    digest: string
  ) {
    digest = digest.toLowerCase();
    if (this.bots[botIdx].busy || this.locked.has(digest)) {
      return this.trash.has(digest) ? BotStatus.Ready : BotStatus.Busy;
    }
    // lock
    this.bots[botIdx].busy = true;
    this.locked.add(digest);

    // fetch order from sc, including the dependencies
    const onChainOrder = await this.getOrderByIdWithDependencies(
      symbol,
      digest,
      this.bots[botIdx].api
    );

    const onChainTS = (() => {
      if (onChainOrder != undefined && onChainOrder.quantity > 0) {
        return onChainOrder.submittedTimestamp;
      }
    })();

    if (!onChainTS) {
      console.log({
        reason: "order not found",
        symbol: symbol,
        digest: digest,
        time: new Date(Date.now()).toISOString(),
      });
      this.bots[botIdx].busy = false;
      if (!this.trash.has(digest)) {
        this.locked.delete(digest);
      }
      return BotStatus.PartialError;
    }

    if (!this.checkOrderDependenciesResolved(onChainOrder!)) {
      console.log({
        reason: "unresolved dependencies",
        symbol: symbol,
        digest: digest,
        time: new Date(Date.now()).toISOString(),
      });
      this.bots[botIdx].busy = false;
      if (!this.trash.has(digest)) {
        this.locked.delete(digest);
      }
      return BotStatus.PartialError;
    }

    // check oracles
    const oracleTS = await this.bots[botIdx].api
      .fetchPriceSubmissionInfoForPerpetual(symbol)
      .then((px) => Math.min(...px.submission.timestamps));
    if (oracleTS < onChainTS) {
      // let oracle cache expire before trying
      console.log({
        reason: "outdated off-chain oracle(s)",
        symbol: symbol,
        digest: digest,
        time: new Date(Date.now()).toISOString(),
      });
      // bot can continue
      this.bots[botIdx].busy = false;
      // order stays locked for another second
      await sleep(1_000);
      if (!this.trash.has(digest)) {
        this.locked.delete(digest);
      }
      return BotStatus.PartialError;
    }

    // submit txn
    console.log({
      info: "submitting txn...",
      symbol: symbol,
      executor: this.bots[botIdx].api.getAddress(),
      digest: digest,
      oracleTimestamp: oracleTS,
      time: new Date(Date.now()).toISOString(),
    });
    let tx: ContractTransaction;
    try {
      const feeData = await this.providers[
        Math.floor(Math.random() * this.providers.length)
      ].getFeeData();
      tx = await this.bots[botIdx].api.executeOrders(
        symbol,
        [digest],
        this.config.rewardsAddress,
        undefined,
        {
          gasLimit: this.config.gasLimit,
          gasPrice: feeData.gasPrice
            ? feeData.gasPrice.mul(110).div(100)
            : undefined,
          maxFeePerGas: feeData.maxFeePerGas
            ? feeData.maxFeePerGas.mul(110).div(100)
            : undefined,
        }
      );
      console.log({
        info: "txn accepted",
        symbol: symbol,
        orderBook: tx.to,
        executor: tx.from,
        digest: digest,
        gasLimit: `${utils.formatUnits(tx.gasLimit, "wei")} wei`,
        hash: tx.hash,
        time: new Date(Date.now()).toISOString(),
      });
    } catch (e: any) {
      // didn't make it on-chain - handle it (possibly re-throw error)
      const error = e?.toString();
      const addr = this.bots[botIdx].api.getAddress();
      console.log({
        info: "txn rejected",
        reason: error,
        symbol: symbol,
        executor: addr,
        digest: digest,
        time: new Date(Date.now()).toISOString(),
      });

      switch (true) {
        case error.includes("insufficient funds"):
          this.metrics.incrementInsufficientFunds();
          this.locked.delete(digest);
          await this.fundWallets([addr]);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;
        case error.includes("order not found"):
          this.metrics.incrementOrderNotFound();
          // the order stays locked: if we're here it was on chain at some
          // point, so now it's gone
          this.trash.add(digest);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;
        case error.includes("gas price too low"):
          this.metrics.incrementGasPriceTooLow();
          // it happens sometimes
          await sleep(1_000);
          this.locked.delete(digest);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;
        case error.includes("intrinsic gas too low"):
          this.metrics.incrementGasPriceTooLow();
          // this can happen on arbitrum, attempt to rerun the tx with increased
          // gas limit
          this.config.gasLimit *= this.gasLimitIncreaseFactor;
          this.gasLimitIncreaseCounter++;
          console.log("intrinsic gas too low, increasing gas limit", {
            new_gas_limit: this.config.gasLimit,
          });
          this.locked.delete(digest);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;

        default:
          // something else, prob rpc
          throw e;
      }
    }

    // confirm execution
    try {
      const receipt = await executeWithTimeout(tx.wait(), 10_000, "timeout");
      console.log({
        info: "txn confirmed",
        symbol: symbol,
        orderBook: receipt.to,
        executor: receipt.from,
        digest: digest,
        block: receipt.blockNumber,
        gasUsed: `${utils.formatUnits(receipt.gasUsed, "wei")} wei`,
        hash: receipt.transactionHash,
        time: new Date(Date.now()).toISOString(),
      });
      this.metrics.incrementOrderExecutionConfirmations();

      if (this.gasLimitIncreaseCounter > 0) {
        this.config.gasLimit = this.originalGasLimit;
        this.gasLimitIncreaseCounter = 0;
      }

      // order was executed
      this.bots[botIdx].busy = false;
      this.locked.add(digest);
      this.trash.add(digest);
      return BotStatus.Ready;
    } catch (e: any) {
      // could not confirm
      const error = e?.toString();
      const addr = this.bots[botIdx].api.getAddress();
      console.log({
        info: "txn not confirmed",
        reason: error,
        symbol: symbol,
        executor: addr,
        digest: digest,
        time: new Date(Date.now()).toISOString(),
      });
      this.metrics.incrementOrderExecutionFailedConfirmations();

      // Send message to slack whenever there is a revert reason that interests
      // us
      const p =
        this.providers[Math.floor(Math.random() * this.providers.length)];
      sendTxRevertedMessage(getTxRevertReason(tx, p), tx.hash, digest, symbol);

      // check if order is gone
      let ordr = await this.bots[botIdx].api.getOrderById(symbol, digest);
      if (ordr != undefined && ordr.quantity > 0) {
        // order is still on chain - maybe still processing, so wait and check again,
        // then unlock if it hasn't been trashed
        console.log({
          info: "order is still on-chain",
          symbol: symbol,
          executor: addr,
          digest: digest,
          time: new Date(Date.now()).toISOString(),
        });
        await sleep(10_000);
        this.bots[botIdx].busy = false;
        ordr = await this.bots[botIdx].api.getOrderById(symbol, digest);
        if (ordr != undefined && ordr.quantity > 0) {
          if (!this.trash.has(digest)) {
            this.locked.delete(digest);
          }
          return BotStatus.Error;
        }
      }
      this.bots[botIdx].busy = false;
      // order is gone, relock to be safe
      this.locked.add(digest);
      this.trash.add(digest);
      console.log({
        info: "order is gone",
        symbol: symbol,
        executor: addr,
        digest: digest,
        time: new Date(Date.now()).toISOString(),
      });
      return BotStatus.Ready;
    }
  }

  /**
   * Execute orders in q
   */
  public async execute() {
    if (this.q.size < 1) {
      return { busy: 0, partial: 0, error: 0, ok: 0 };
    }

    if (
      Date.now() - this.lastCall <
      this.config.executeIntervalSecondsMin * 1_000
    ) {
      return { busy: this.bots.length, partial: 0, error: 0, ok: 0 };
    }

    this.lastCall = Date.now();
    let attempts = 0;
    const q = [...this.q];
    const responses = { busy: 0, partial: 0, error: 0, ok: 0 };
    const executed: Promise<BotStatus>[] = [];
    for (const msg of q) {
      let { symbol, digest } = msg;
      digest = digest.toLowerCase();
      if (this.locked.has(digest) || this.trash.has(digest)) {
        continue;
      }
      for (let i = 0; i < this.bots.length; i++) {
        const bot = this.bots[i];
        if (!bot.busy) {
          // msg will be attempted by this bot
          attempts++;
          this.q.delete(msg);
          executed.push(this.executeOrderByBot(i, symbol, digest));
        }
      }
    }
    // send txns
    const results = await executeWithTimeout(
      Promise.allSettled(executed),
      30_000
    );
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.status === "fulfilled") {
        // successes += result.value ? 1 : 0;
        if (result.value == BotStatus.Ready) {
          responses.ok++;
        } else if (result.value == BotStatus.Error) {
          responses.error++;
        } else if (result.value == BotStatus.PartialError) {
          responses.partial++;
        } else {
          // }if(result.value == BotStatus.Busy) {
          responses.busy++;
        }
      } else {
        throw new Error(`uncaught error: ${result.reason.toString()}`);
      }
    }

    return responses;
  }

  public async fundWallets(addressArray: string[]) {
    const provider =
      this.providers[Math.floor(Math.random() * this.providers.length)];
    const treasury = new Wallet(this.treasury, provider);
    const gasPriceWei = await provider.getGasPrice();
    // min balance should cover 1e7 gas
    const minBalance = gasPriceWei.mul(this.config.gasLimit * 5);
    for (let addr of addressArray) {
      const botBalance = await provider.getBalance(addr);
      const treasuryBalance = await provider.getBalance(treasury.address);
      console.log({
        treasuryAddr: treasury.address,
        treasuryBalance: utils.formatUnits(treasuryBalance),
        botAddress: addr,
        botBalance: utils.formatUnits(botBalance),
        minBalance: utils.formatUnits(minBalance),
        needsFunding: botBalance.lt(minBalance),
      });
      if (botBalance.lt(minBalance)) {
        // transfer twice the min so it doesn't transfer every time
        const transferAmount = minBalance.mul(2).sub(botBalance);
        if (transferAmount.lt(treasuryBalance)) {
          console.log({
            info: "transferring funds...",
            to: addr,
            transferAmount: utils.formatUnits(transferAmount),
          });
          const tx = await treasury.sendTransaction({
            to: addr,
            value: transferAmount,
          });
          await tx.wait();
          console.log({
            transferAmount: utils.formatUnits(transferAmount),
            txn: tx.hash,
          });
        } else {
          throw new Error(
            `insufficient balance in treasury (${utils.formatUnits(
              treasuryBalance
            )}); send at least ${utils.formatUnits(transferAmount)} to ${
              treasury.address
            }`
          );
        }
      }
    }
  }

  public setDistributor(distributor: Distributor) {
    this.distributor = distributor;
  }
}
