import { PerpetualDataHandler, OrderExecutorTool } from "@d8x/perpetuals-sdk";
import { ContractTransaction, Wallet, utils } from "ethers";
import { providers } from "ethers";
import { Redis } from "ioredis";
import { constructRedis, executeWithTimeout, sleep } from "../utils";
import { BotStatus, ExecutorConfig, ExecuteOrderMsg } from "../types";

export default class Executor {
  // objects
  private providers: providers.StaticJsonRpcProvider[];
  private bots: { api: OrderExecutorTool; busy: boolean }[];
  private redisSubClient: Redis;

  // parameters
  private treasury: string;
  private privateKey: string[];
  private config: ExecutorConfig;

  // state
  private q: Set<string> = new Set();
  private locked: Set<string> = new Set();
  private lastCall: number = 0;

  constructor(
    pkTreasury: string,
    pkLiquidators: string[],
    config: ExecutorConfig
  ) {
    this.treasury = pkTreasury;
    this.privateKey = pkLiquidators;
    this.config = config;
    this.redisSubClient = constructRedis("executorSubClient");
    this.providers = this.config.rpcExec.map(
      (url) => new providers.StaticJsonRpcProvider(url)
    );
    this.bots = this.privateKey.map((pk) => ({
      api: new OrderExecutorTool(
        PerpetualDataHandler.readSDKConfig(this.config.sdkConfig),
        pk
      ),
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
      // console.log(`trying provider ${i} ... `);
      const results = await Promise.allSettled(
        // createProxyInstance attaches the given provider to the object instance
        this.bots.map((bot) => bot.api.createProxyInstance(this.providers[i]))
      );
      success = results.every((r) => r.status === "fulfilled");
      i = (i + 1) % this.providers.length;
      tried++;
    }
    if (!success) {
      throw new Error("critical: all RPCs are down");
    }

    // Subscribe to relayed events
    // console.log(`${new Date(Date.now()).toISOString()}: subscribing to account streamer...`);
    await this.redisSubClient.subscribe(
      "block",
      "ExecuteOrder",
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
        //  else {
        //   console.log(`${new Date(Date.now()).toISOString()}: redis subscription success - ${count} active channels`);
        // }
      }
    );
  }

  /**
   * Subscribes to liquidation opportunities and attempts to liquidate.
   */
  public async run(): Promise<void> {
    // consecutive responses
    let [busy, errors, success, msgs] = [0, 0, 0, 0];

    return new Promise<void>((resolve, reject) => {
      setInterval(async () => {
        if (
          Date.now() - this.lastCall >
          this.config.executeIntervalSecondsMax
        ) {
          await this.execute().catch(() => {});
        }
      });

      this.redisSubClient.on("message", async (channel, msg) => {
        switch (channel) {
          // case "block": {
          //   if (+msg % 1000 == 0) {
          //     console.log(
          //       JSON.stringify({ busy: busy, errors: errors, success: success, msgs: msgs }, undefined, "  ")
          //     );
          //   }
          //   break;
          // }
          case "ExecuteOrder": {
            const prevCount = this.q.size;
            this.q.add(msg);
            msgs += this.q.size > prevCount ? 1 : 0;
            const res = await this.execute().catch(() => {});
            if (res == BotStatus.Busy) {
              busy++;
            } else if (res == BotStatus.PartialError) {
              errors++;
            } else if (res == BotStatus.Error) {
              // throw new Error(`error`);
              console.log("error");
            } else {
              // res == BotStatus.Ready
              success++;
            }
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
   * Orders not found on chain are never tried.
   * Orders tried that revert with not found error, are never tried again.
   * @param botIdx Index of wallet used
   * @param symbol Perpetual symbol
   * @param digest Order digest/id
   * @param onChain true if order was seen on-chain
   * @returns true if excution txn does not revert
   */
  private async executeOrderByBot(
    botIdx: number,
    symbol: string,
    digest: string,
    onChain: boolean
  ) {
    digest = digest.toLowerCase();
    if (this.bots[botIdx].busy || this.locked.has(digest)) {
      return false;
    }
    // lock
    this.bots[botIdx].busy = true;
    this.locked.add(digest);

    // check if order is on-chain - stop if not/unable to check
    const isOpen =
      onChain ||
      (await this.bots[botIdx].api
        .getOrderById(symbol, digest)
        .then((ordr) => ordr != undefined && ordr.quantity > 0)
        .catch(() => false));

    if (!isOpen) {
      console.log({
        info: "order not found",
        symbol: symbol,
        executor: this.bots[botIdx].api.getAddress(),
        digest: digest,
      });
      this.bots[botIdx].busy = false;
      this.locked.delete(digest);
      return false;
    }

    // submit txn
    console.log({
      info: "submitting txn...",
      symbol: symbol,
      executor: this.bots[botIdx].api.getAddress(),
      digest: digest,
    });
    let tx: ContractTransaction;
    try {
      tx = await this.bots[botIdx].api.executeOrders(
        symbol,
        [digest],
        this.config.rewardsAddress,
        undefined,
        {
          gasLimit: 2_000_000,
          splitTx: false,
        }
      );
    } catch (e: any) {
      console.log({
        info: "txn rejected",
        reason: e.toString(),
        symbol: symbol,
        executor: this.bots[botIdx].api.getAddress(),
        digest: digest,
      });
      this.bots[botIdx].busy = false;
      this.locked.delete(digest);
      return false;
    }
    console.log({
      info: "txn accepted",
      symbol: symbol,
      orderBook: tx.to,
      executor: tx.from,
      digest: digest,
      gas: `${utils.formatUnits(tx.gasLimit, "gwei")} gwei`,
      hash: tx.hash,
    });

    // confirm execution
    try {
      const receipt = await tx.wait();
      console.log({
        info: "txn confirmed",
        symbol: symbol,
        orderBook: receipt.to,
        executor: receipt.from,
        digest: digest,
        block: receipt.blockNumber,
        gas: utils.formatEther(receipt.cumulativeGasUsed),
        hash: receipt.transactionHash,
      });
    } catch (e: any) {
      const error = e?.toString() || "";
      console.log({
        info: "txn reverted",
        reason: error,
        symbol: symbol,
        executor: this.bots[botIdx].api.getAddress(),
        digest: digest,
      });
      // unlock order (if it exists)
      if (!error.includes("order not found")) {
        this.locked.delete(digest);
      }
      const bot = this.bots[botIdx].api.getAddress();

      if (error.includes("insufficient funds for intrinsic transaction cost")) {
        try {
          await this.fundWallets([bot]);
        } catch (e: any) {
          console.log(`failed to fund bot ${bot}`);
        }
      }
    }
    // unlock bot
    this.bots[botIdx].busy = false;
    return true;
  }

  /**
   * Execute orders in q
   */
  public async execute() {
    if (
      Date.now() - this.lastCall <
      this.config.executeIntervalSecondsMin * 1_000
    ) {
      return BotStatus.Busy;
    }

    this.lastCall = Date.now();
    let attempts = 0;
    let successes = 0;
    const q = [...this.q];

    if (q.length == 0) {
      return BotStatus.Ready;
    }

    const executed: Promise<boolean>[] = [];
    for (const msg of q) {
      const { symbol, digest, onChain }: ExecuteOrderMsg = JSON.parse(msg);
      if (this.locked.has(digest)) {
        continue;
      }
      for (let i = 0; i < this.bots.length; i++) {
        const bot = this.bots[i];
        if (!bot.busy) {
          // msg will be attempted by this bot
          attempts++;
          this.q.delete(msg);
          executed.push(this.executeOrderByBot(i, symbol, digest, onChain));
        }
      }
    }
    // send txns
    const results = await Promise.allSettled(executed);
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.status === "fulfilled") {
        successes += result.value ? 1 : 0;
      } else {
        console.log(`uncaught error: ${result.reason.toString()}`);
      }
    }

    // return cases:
    if (successes == 0 && attempts == this.bots.length) {
      // all bots are down, either rpc or px service issue
      return BotStatus.Error;
    } else if (attempts == 0 && q.length > 0) {
      // did not try anything
      return BotStatus.Busy;
    } else if (successes == 0 && attempts > 0) {
      // tried something but it didn't work
      return BotStatus.PartialError;
    } else if (successes < attempts) {
      // some attempts worked, others failed
      return BotStatus.PartialError;
    } else {
      // everything worked or nothing happend
      return BotStatus.Ready;
    }
  }

  public async fundWallets(addressArray: string[]) {
    const provider =
      this.providers[Math.floor(Math.random() * this.providers.length)];
    const treasury = new Wallet(this.treasury, provider);
    // min balance should cover 1e7 gas
    const minBalance = utils.parseUnits(
      `${this.config.maxGasPriceGWei * 1e7}`,
      "gwei"
    );
    for (let addr of addressArray) {
      const botBalance = await provider.getBalance(addr);
      const treasuryBalance = await provider.getBalance(treasury.address);
      console.log({
        treasuryAddr: treasury.address,
        treasuryBalance: utils.formatUnits(treasuryBalance),
        botAddress: addr,
        botBalance: utils.formatUnits(botBalance),
        minBalance: utils.formatUnits(minBalance),
      });
      if (botBalance.lt(minBalance)) {
        // transfer twice the min so it doesn't transfer every time
        const transferAmount = minBalance.mul(2).sub(botBalance);
        if (transferAmount.lt(treasuryBalance)) {
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
            `insufficient balance in treasury ${utils.formatUnits(
              treasuryBalance
            )}; send funds to ${treasury.address}`
          );
        }
      }
    }
  }
}
