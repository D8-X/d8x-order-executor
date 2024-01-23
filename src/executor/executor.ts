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
          await this.execute();
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
            const res = await this.execute();
            if (res == BotStatus.Busy) {
              busy++;
            } else if (res == BotStatus.PartialError) {
              errors++;
            } else if (res == BotStatus.Error) {
              throw new Error(`error`);
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

    const txns: {
      tx: Promise<ContractTransaction>;
      botIdx: number;
      symbol: string;
      digest: string;
      trader: string;
    }[] = [];
    for (const msg of q) {
      const { symbol, digest, trader, onChain }: ExecuteOrderMsg =
        JSON.parse(msg);
      if (this.locked.has(`${symbol}:${digest}`)) {
        console.log("not trying order", symbol, digest, "because it's locked");
        continue;
      }
      let assignedTx = false;
      for (let i = 0; i < this.bots.length && !assignedTx; i++) {
        const bot = this.bots[i];
        if (!bot.busy) {
          // msg will be attempted by this bot
          attempts++;
          this.q.delete(msg);
          bot.busy = true;
          assignedTx = true;

          const executeProm = executeWithTimeout(
            bot.api.executeOrders(
              symbol,
              [digest],
              this.config.rewardsAddress,
              undefined,
              {
                gasLimit: 2_000_000,
                splitTx: false,
              }
            ),
            30_000,
            "timeout"
          );
          this.locked.add(`${symbol}:${digest}`);
          txns.push({
            tx: onChain
              ? executeProm
              : bot.api
                  .getOrderById(symbol, digest)
                  .then((ordr) => {
                    console.log(ordr);
                    return ordr
                      ? executeProm
                      : Promise.reject("order not found");
                  })
                  .catch((e) => Promise.reject("order status unknown")),
            botIdx: i,
            symbol: symbol,
            digest: digest,
            trader: trader,
          });
        }
      }
      if (!assignedTx) {
        // all busy
        break;
      }
    }
    // send txns
    const results =
      (await Promise.allSettled(txns.map(({ tx }) => tx)).catch((e) =>
        console.log("should not be here")
      )) || [];

    const confirmations: Promise<void>[] = [];
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.status === "fulfilled") {
        successes++;
        console.log({
          info: "txn submitted",
          symbol: txns[i].symbol,
          orderBook: result.value.to,
          executor: result.value.from,
          trader: txns[i].trader,
          digest: txns[i].digest,
          block: result.value.blockNumber,
          gas: utils.formatEther(result.value.gasLimit),
          hash: result.value.hash,
        });
        confirmations.push(
          executeWithTimeout(
            result.value
              .wait()
              .then((res) => {
                // mined -> unlock bot and order
                this.locked.delete(`${txns[i].symbol}:${txns[i].digest}`);
                this.bots[txns[i].botIdx].busy = false;
                console.log({
                  info: "txn confirmed",
                  symbol: txns[i].symbol,
                  orderBook: res.to,
                  executor: res.from,
                  trader: txns[i].trader,
                  digest: txns[i].digest,
                  block: res.blockNumber,
                  gas: utils.formatEther(res.cumulativeGasUsed),
                  hash: res.transactionHash,
                });
              })
              .catch(() => {
                // status unknown - wait and then unlock
                console.log("txn confirmation failed");
                sleep(5_000).then();
              })
              .finally(() => {
                console.log({
                  info: "unlocked",
                  digest: txns[i].digest,
                  bot: this.bots[txns[i].botIdx].api.getAddress(),
                });
                this.locked.delete(`${txns[i].symbol}:${txns[i].digest}`);
                this.bots[txns[i].botIdx].busy = false;
              }),
            30_000,
            "timeout"
          )
        );
      } else {
        console.log({
          info: "txn rejected",
          symbol: txns[i].symbol,
          executor: this.bots[txns[i].botIdx].api.getAddress(),
          trader: txns[i].trader,
          digest: txns[i].digest,
        });
        // txn was not picked up or timed-out
        const bot = this.bots[txns[i].botIdx].api.getAddress();
        let prom: Promise<void>;
        const error = result.reason?.toString() ?? "";
        if (
          error.includes("insufficient funds for intrinsic transaction cost")
        ) {
          // not enough gas for txn cost -> unlock order immediately, fund, then unlock bot
          console.log({ info: "insufficient funds", bot: bot });
          this.locked.delete(`${txns[i].symbol}:${txns[i].digest}`);
          prom = this.fundWallets([bot]).then(() => {
            this.bots[txns[i].botIdx].busy = false;
          });
        } else if (error.includes("timeout")) {
          // timeout -> wait 5 more seconds then unlock order and bot
          prom = sleep(5_000).then(() => {
            this.locked.delete(`${txns[i].symbol}:${txns[i].digest}`);
            this.bots[txns[i].botIdx].busy = false;
          });
        } else {
          // other errors, usually rpc -> resume immediately
          if (!error.includes("gas price too low")) {
            console.log(
              `txn to execute ${txns[i].symbol} order ${txns[i].digest} was rejected with reason:`,
              result.reason
            );
          } else {
            console.log("gas price too low");
          }
          this.locked.delete(`${txns[i].symbol}:${txns[i].digest}`);
          this.bots[txns[i].botIdx].busy = false;
          prom = Promise.resolve();
        }
        confirmations.push(prom);
      }
    }

    // return cases:
    let res: BotStatus;
    if (successes == 0 && attempts == this.bots.length) {
      // all bots are down, either rpc or px service issue
      res = BotStatus.Error;
    } else if (attempts == 0 && q.length > 0) {
      // did not try anything
      res = BotStatus.Busy;
    } else if (successes == 0 && attempts > 0) {
      // tried something but it didn't work
      res = BotStatus.PartialError;
    } else if (successes < attempts) {
      // some attempts worked, others failed
      res = BotStatus.PartialError;
    } else {
      // everything worked or nothing happend
      if (attempts > 0) {
      }
      res = BotStatus.Ready;
    }

    (await Promise.allSettled(confirmations)).map((_result, i) => {
      // just in case, unlock all used bots and orders
      this.locked.delete(`${txns[i].symbol}:${txns[i].digest}`);
      this.bots[txns[i].botIdx].busy = false;
    });

    return res;
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
