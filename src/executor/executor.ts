import { PerpetualDataHandler, OrderExecutorTool } from "@d8x/perpetuals-sdk";
import { ContractTransaction, Wallet, utils } from "ethers";
import { providers } from "ethers";
import { Redis } from "ioredis";
import { constructRedis, executeWithTimeout, sleep } from "../utils";
import { BotStatus, ExecutorConfig, ExecuteOrderMsg, TradeMsg } from "../types";

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
  private q: Set<string> = new Set();
  private locked: Set<string> = new Set();
  private lastCall: number = 0;
  private timesTried: Map<string, number> = new Map();
  private trash: Set<string> = new Set();

  constructor(
    pkTreasury: string,
    pkLiquidators: string[],
    config: ExecutorConfig
  ) {
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
      "ExecuteOrder",
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
          this.q.delete(digest);
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
          case "ExecuteOrder": {
            const prevCount = this.q.size;
            this.q.add(msg);
            msgs += this.q.size > prevCount ? 1 : 0;
            const res = await this.execute();
            busy += res.busy;
            errors += res.error;
            success += res.ok;
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

    const isOnChain = await this.bots[botIdx].api
      .getOrderById(symbol, digest)
      .then((ordr) => {
        if (ordr != undefined && ordr.quantity > 0) {
          return true;
        }
        return false;
      });

    if (!isOnChain) {
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

    // submit txn
    console.log({
      info: "submitting txn...",
      symbol: symbol,
      executor: this.bots[botIdx].api.getAddress(),
      digest: digest,
      time: new Date(Date.now()).toISOString(),
    });
    let tx: ContractTransaction;
    try {
      tx = await this.bots[botIdx].api.executeOrders(
        symbol,
        [digest],
        this.config.rewardsAddress,
        undefined,
        {
          gasLimit: this.config.gasLimit,
          gasPrice: await this.providers[
            Math.floor(Math.random() * this.providers.length)
          ].getGasPrice(),
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
          this.locked.delete(digest);
          await this.fundWallets([addr]);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;
        case error.includes("order not found"):
          // the order stays locked: if we're here it was on chain at some
          // point, so now it's gone
          this.trash.add(digest);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;
        case error.includes("gas price too low"):
          // it happens sometimes
          await sleep(1_000);
          this.locked.delete(digest);
          this.bots[botIdx].busy = false;
          return BotStatus.PartialError;
        case error.includes("intrinsic gas too low"):
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
      let { symbol, digest }: ExecuteOrderMsg = JSON.parse(msg);
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
}
