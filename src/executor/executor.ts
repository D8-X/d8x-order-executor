import {
  MarketData,
  PerpetualDataHandler,
  OrderExecutorTool,
  Order,
  PriceFeedSubmission,
  SmartContractOrder,
  ORDER_TYPE_MARKET,
  ZERO_ORDER_ID,
  BUY_SIDE,
  SELL_SIDE,
  OrderStatus,
  MarginAccount,
  CLOSED_SIDE,
} from "@d8x/perpetuals-sdk";
import { BigNumber, ethers } from "ethers";
import { emitWarning } from "process";
import { GasInfo, GasPriceV2, OrderBundle, ExecutorConfig } from "../types";
import { Redis } from "ioredis";
import { constructRedis, executeWithTimeout } from "../utils";
import RPCManager from "./rpcManager";

export default class Executor {
  // objects
  private provider: ethers.providers.Provider | undefined;
  private mktData: MarketData | undefined;
  private orTool: OrderExecutorTool[] | undefined;
  private redisSubClient: Redis;
  private rpcManager: RPCManager;

  // parameters
  private symbol: string;
  private earningsAddr: string;
  private privateKey: string[];
  private config: ExecutorConfig;
  private moduloTS: number; // primarily submit orders with createdTimestamp divisible by moduloTS (if defined), or all orders (if undefined)
  private residualTS: number;
  private perpetualId: number | undefined;

  // constants
  private NON_EXECUTION_WAIT_TIME_MS = 30_000; // maximal time we leave for owner-peer to execute an executable order
  private MAX_OUTOFSYNC_SECONDS: number = 10; // publish times must be within 10 seconds of each other, or submission will fail on-chain
  private SPLIT_PX_UPDATE: boolean = false; // needed on zk because of performance issues on Polygon side
  private REFRESH_INTERVAL_MS: number; // how often to force a refresh of all orders, in miliseconds
  private EXECUTE_INTERVAL_MS: number;

  // state
  private openOrders: OrderBundle[] = new Array<OrderBundle>();
  private newOrders: OrderBundle[] = new Array<OrderBundle>();
  private removedOrders: Set<string> = new Set<string>();
  private orderIds: Set<string> = new Set();
  private isExecuting: boolean = false;
  private isRefreshing: boolean = false;
  private hasQueue: boolean = false;
  private peerNonExecutionTimestampMS: Map<string, number>; // orderId -> timestamp
  private lastRefreshTime: number = 0;
  private lastExecuteOrdersCall: number = 0;
  private obAddr: string | undefined;
  private blockNumber: number;
  private hasOffChainOrders: boolean = false;
  private lastOffChainOrder: number = Infinity;
  private priceIds: string[] | undefined;
  private traders: Map<string, MarginAccount> = new Map();

  constructor(
    privateKey: string | string[],
    symbol: string,
    config: ExecutorConfig,
    moduloTS: number,
    residualTS: number,
    earningsAddr: string
  ) {
    this.privateKey = typeof privateKey == "string" ? [privateKey] : privateKey;
    this.symbol = symbol;
    this.earningsAddr = earningsAddr;
    this.moduloTS = moduloTS;
    this.residualTS = residualTS;
    this.config = config;
    this.EXECUTE_INTERVAL_MS = this.config.executeIntervalSeconds * 1_000;
    this.REFRESH_INTERVAL_MS = this.config.refreshOrdersSeconds * 1_000;
    this.peerNonExecutionTimestampMS = new Map<string, number>();
    this.redisSubClient = constructRedis("ExecutorListener");
    this.blockNumber = 0;
    this.rpcManager = new RPCManager(this.config.RPC);
  }

  private log(x: any) {
    console.log(
      JSON.stringify({
        time: new Date(Date.now()).toISOString(),
        timestamp: Date.now() / 1000,
        latestBlock: this.blockNumber,
        symbol: this.symbol,
      }),
      x
    );
  }

  /**
   *
   * @param provider HTTP Provider
   */
  public async initialize() {
    this.provider = new ethers.providers.StaticJsonRpcProvider(
      await this.rpcManager.getRPC()
    );
    // infer config from provider
    const chainId = (await this.provider!.getNetwork()).chainId;
    const config = PerpetualDataHandler.readSDKConfig(chainId);
    // override default price feed endpoint (if an override is given)
    if (
      this.config.priceFeedEndpoints &&
      this.config.priceFeedEndpoints?.length > 0
    ) {
      config.priceFeedEndpoints = this.config.priceFeedEndpoints;
    }

    // MarketData (read only, no authentication needed)
    this.mktData = new MarketData(config);
    this.orTool = this.privateKey.map(
      (pk) => new OrderExecutorTool(config, pk)
    );

    // Connect
    this.log(
      `connecting to proxy with read-only access: ${config.proxyAddr} (chain id ${config.chainId})...`
    );
    await this.mktData.createProxyInstance(this.provider);

    this.log(`connecting to order book with write access...`);
    await Promise.all(
      this.orTool.map(async (obj) =>
        obj.createProxyInstance(
          new ethers.providers.StaticJsonRpcProvider(
            await this.rpcManager.getRPC()
          )
        )
      )
    );

    // Create contracts
    try {
      this.perpetualId = this.mktData.getPerpIdFromSymbol(this.symbol);
    } catch (e) {
      // no such perpetual - exit gracefully without restart
      console.log(`Perpetual ${this.symbol} not found - bot not running`);
      process.exit(0);
    }

    // fetch lob address
    this.obAddr = await this.mktData
      .getReadOnlyProxyInstance()
      .getOrderBookAddress(this.perpetualId);

    // Subscribe to blockchain events
    this.log(`subscribing to blockchain streamer...`);
    await this.redisSubClient.subscribe(
      "switch-mode",
      "block",
      "PerpetualLimitOrderCreated",
      "ExecutionFailed",
      "PerpetualLimitOrderCancelled",
      "Trade",
      (err, count) => {
        if (err) {
          this.log(`subscription failed: ${err}`);
          process.exit(1);
        } else {
          this.log(`subscribed to ${count} active channels`);
        }
      }
    );
    // initialization complete
    this.log({
      perpetualId: this.perpetualId,
      orderBook: this.obAddr,
      proxy: config.proxyAddr,
      earnings: this.earningsAddr,
      servers: this.moduloTS,
      serverIndex: this.residualTS,
      wallets: this.orTool.map((ot) => ot.getAddress()),
    });
    // Fetch orders
    this.refreshOpenOrders()
      .then(() => this.executeOrders(true))
      .then(() => this.executeOrders(false));
  }

  /**
   * Given the number of servers running and the index of the current one,
   * determines if an order is assigned to us
   * @param orderBundle Order bundle
   * @returns True if order id is assigned to this server
   */
  private isMyOrder(orderBundle: OrderBundle): boolean {
    return (
      BigNumber.from(orderBundle.id.slice(0, 8)).toNumber() % this.moduloTS ==
      this.residualTS
    );
  }

  /**
   * Returns how late an order execution is
   * @param orderBundle Order bundle
   * @returns
   */
  private overdueForMS(orderBundle: OrderBundle): number {
    let ts = this.peerNonExecutionTimestampMS.get(orderBundle.id);
    let tsNow = Date.now();
    if (ts == undefined) {
      // ts =
      //   Math.max(tsNow / 1_000, orderBundle.order.executionTimestamp ?? 0, orderBundle.order.submittedTimestamp ?? 0) *
      //   1_000;
      ts =
        orderBundle.order.executionTimestamp &&
        orderBundle.order.executionTimestamp > 0
          ? 1_000 * orderBundle.order.executionTimestamp
          : tsNow;
      this.peerNonExecutionTimestampMS.set(orderBundle.id, ts);
      // this.log(`${orderBundle.order.type} order ${orderBundle.id} recorded at ${ts}`);
    }
    return tsNow - (ts + this.NON_EXECUTION_WAIT_TIME_MS);
  }

  /**
   * Determines if an order does not have any parent-child constraints before it can be executed
   * @param order Order
   * @returns
   */
  private isSingleMarketOrder(order: Order) {
    return (
      order.type == ORDER_TYPE_MARKET &&
      (order.parentChildOrderIds == undefined ||
        (order.parentChildOrderIds[0] == ZERO_ORDER_ID &&
          order.parentChildOrderIds[1] == ZERO_ORDER_ID))
    );
  }

  /**
   * Listen to events for a number of blocks; requires initialize() first
   * @param maxBlocks number of blocks we will listen to event handlers
   * @returns void
   */
  public async run(): Promise<void> {
    // listen to blocks
    if (this.mktData == undefined || this.orTool == undefined) {
      throw Error("objects not initialized");
    }

    let numBlocks = -1;

    return new Promise<void>(async (resolve, reject) => {
      setInterval(async () => {
        // should check if anything can be executed every minute +- 10 sec
        if (
          !this.hasQueue &&
          Date.now() - this.lastExecuteOrdersCall < this.EXECUTE_INTERVAL_MS
        ) {
          return;
        }
        await this.executeOrders(true);
        await this.executeOrders(false);
      }, 10_000);

      setInterval(async () => {
        // checks that we refresh all orders every hour +- 10 sec
        if (Date.now() - this.lastRefreshTime < this.REFRESH_INTERVAL_MS) {
          return;
        }
        this.refreshOpenOrders();
      }, 10_000);

      // setInterval(async () => {
      //   // tries to execute frequently if the broker signed an order,
      //   // until the order is found on chain or it's been long enough
      //   if (this.hasOffChainOrders && Date.now() - this.lastOffChainOrder < 60_000) {
      //     this.log(`last off-chain order: ${Math.round((Date.now() - this.lastOffChainOrder) / 1000)}s ago`);
      //     await this.executeOrders();
      //   }
      // }, 1_000);

      this.redisSubClient.on("message", async (channel: any, msg: string) => {
        switch (channel) {
          case "switch-mode": {
            // listener changed mode: something failed so refresh orders
            this.refreshOpenOrders();
            break;
          }
          case "block": {
            numBlocks++;
            // new block - track
            this.blockNumber = Number(msg);
            if (!this.isExecuting) {
              this.moveNewOrdersToOrders();
            }
            if (
              this.hasOffChainOrders &&
              Date.now() - this.lastOffChainOrder < 60_000
            ) {
              // this.log(`last off-chain order: ${Math.round((Date.now() - this.lastOffChainOrder) / 1000)}s ago`);
              await this.executeOrders(true);
            }
            if (numBlocks % 500 == 0) {
              const mktOrders = this.openOrders.filter(
                (ob) => ob.order?.type == ORDER_TYPE_MARKET
              ).length;
              this.log(
                JSON.stringify({
                  marketOrders: mktOrders,
                  totalOrders: this.openOrders.length,
                })
              );
              if (mktOrders > 0) {
                await this.executeOrders(true).then(() => {
                  this.executeOrders(false);
                });
              }
            }
            break;
          }
          case "PerpetualLimitOrderCreated": {
            const { perpetualId, trader, order, digest, fromEvent } =
              JSON.parse(msg);
            if (this.perpetualId == perpetualId) {
              let ts = Math.floor(Date.now() / 1_000);
              this.log(`adding order ${digest} from trader ${trader}`);
              // addOrder can trigger an early call to execute if it's a market order
              await this.addOrder(trader, digest, order, ts, fromEvent);
              this.lastOffChainOrder = Date.now();
            }
            break;
          }
          case "ExecutionFailed": {
            // if it failed because of "cancel delay required";
            //  --> order stays in the book
            // else:
            //  --> order is removed from the book
            const { perpetualId, digest, reason } = JSON.parse(msg);
            if (this.perpetualId == perpetualId) {
              let ts = Math.floor(Date.now() / 1_000);
              this.log(`execution of ${digest} failed with reason ${reason}`);
              if (reason != "cancel delay required") {
                this.removeOrder(digest, ts);
              } else {
                // unlock to try again
                this.unlockOrder(digest);
              }
            }
            break;
          }
          case "PerpetualLimitOrderCancelled": {
            const { perpetualId, digest } = JSON.parse(msg);
            if (this.perpetualId == perpetualId) {
              let ts = Math.floor(Date.now() / 1_000);
              this.log(`order ${digest} has been cancelled`);
              this.removeOrder(digest, ts);
            }
            break;
          }
          case "Trade": {
            const { perpetualId, digest, traderAddr } = JSON.parse(msg);
            if (this.perpetualId == perpetualId) {
              let ts = Math.floor(Date.now() / 1_000);
              this.log(`order ${digest} has been executed`);
              this.removeOrder(digest, ts);
              this.updateAccount(traderAddr);
            }
            break;
          }
          default: {
            this.log(`unrecognized message ${msg} on channel ${channel}`);
            break;
          }
        }
      });
    });
  }

  private async updateAccount(trader: string) {
    const acct = await this.mktData!.positionRisk(trader, this.symbol);
    this.traders.set(trader, acct[0]);
  }

  private unlockOrder(id: string) {
    // this.openOrders.forEach((o) => {
    //   if (o.id.toLocaleLowerCase() == id.toLocaleLowerCase()) {
    //     o.isLocked = false;
    //   }
    // });
    for (const o of this.openOrders) {
      if (o.id.toLocaleLowerCase() === id.toLocaleLowerCase()) {
        o.isLocked = false;
        return;
      }
    }
  }

  private lockOrder(id: string) {
    // this.openOrders.forEach((o) => {
    //   if (o.id.toLocaleLowerCase() == id.toLocaleLowerCase()) {
    //     o.isLocked = true;
    //   }
    // });
    for (const o of this.openOrders) {
      if (o.id.toLocaleLowerCase() === id.toLocaleLowerCase()) {
        o.isLocked = true;
        return;
      }
    }
  }

  /**
   * Get order and add it to the array of open orders
   * Orders are added to newOrder array to not interfere with openOrders
   * used in  executeOrders()
   * @param digest order id
   */
  private async addOrder(
    fromTrader: string,
    digest: string,
    scOrder: SmartContractOrder,
    ts: number,
    onChain: boolean
  ) {
    if (this.orTool == undefined) {
      throw Error("orTool not defined");
    }
    let order: Order | undefined;
    if (onChain) {
      order = this.orTool[0].smartContractOrderToOrder(scOrder);
      if (order == undefined) {
        emitWarning(`order ${digest} not found on-chain - not adding`);
        return;
      }
    } else {
      this.hasOffChainOrders = true;
    }

    this.orderIds.add(digest);
    let orderBundle = {
      id: digest,
      order: order,
      isLocked: false,
      ts: ts,
    } as OrderBundle;
    let idx = this.openOrders.findIndex((b) => b.id == digest);
    if (idx >= 0) {
      this.openOrders[idx].order = onChain
        ? order!
        : this.openOrders[idx].order;
      this.openOrders[idx].onChain = onChain || this.openOrders[idx].onChain;
    } else {
      let jdx = this.newOrders.findIndex((b) => b.id == digest);
      if (jdx >= 0) {
        this.newOrders[jdx].order = onChain
          ? order!
          : this.newOrders[jdx].order;
        this.newOrders[jdx].onChain = onChain || this.newOrders[jdx].onChain;
      } else {
        this.newOrders.push(orderBundle);
      }
    }
    if (!this.traders.has(fromTrader)) {
      const acct = await this.mktData!.positionRisk(fromTrader, this.symbol);
      this.traders.set(fromTrader, acct[0]);
    }

    if (order && this.isSingleMarketOrder(order)) {
      if (this.isExecuting) {
        this.hasQueue = true;
      } else {
        this.log(`${onChain ? "event" : "ws"} triggered execution`);
        await this.executeOrders(true);
      }
    }
  }

  public removeOrder(digest: string, ts: number) {
    this.removedOrders.add(digest);
    this.orderIds.delete(digest);
  }

  /**
   * Copy new orders to order array and delete reference in newOrders
   */
  private moveNewOrdersToOrders() {
    for (let k = this.newOrders.length - 1; k >= 0; k--) {
      this.log(
        `moved new ${this.newOrders[k].order?.type} order to open orders, id ${this.newOrders[k].id}`
      );
      // remove if it exists
      let newOb = this.newOrders[k];
      this.openOrders = this.openOrders
        .filter((ob) => {
          return newOb.id != ob.id;
        })
        .reverse();
      // append
      this.openOrders.push(newOb);
      // remove from new orders
      this.newOrders.pop();
    }
    // remove
    this.openOrders = this.openOrders.filter((ob) => {
      const remove = this.removedOrders.has(ob.id);
      if (remove) {
        this.log(`removing order ${ob.id} from open orders`);
      }
      return !this.removedOrders.has(ob.id);
    });
    this.removedOrders = new Set<string>();
    this.newOrders = [];
    this.hasQueue = this.openOrders.some(
      (ob) => ob.order?.type === ORDER_TYPE_MARKET
    );
  }

  /**
   * Reset open orders-array; refresh all open orders
   */
  public async refreshOpenOrders() {
    if (this.isExecuting) {
      // return without updating refresh time
      return;
    }
    this.log(`refreshing orders`);

    // const totalOrders = await this.orTool[0].numberOfOpenOrders(this.symbol, {
    //   rpcURL: await this.rpcManager.getRPC(),
    // });
    const allOrders = await this.orTool![0].getAllOpenOrders(this.symbol);
    this.isExecuting = true;
    this.openOrders = [];
    this.orderIds = new Set();
    this.newOrders = [];
    this.removedOrders = new Set<string>();
    this.lastRefreshTime = Date.now();

    const ts = Math.round(Date.now() / 1000);
    const orders = allOrders[0].reverse();
    const orderIds = allOrders[1].reverse();
    const traders = allOrders[2].reverse();
    for (let k = 0; k < orders.length; k++) {
      if (orders[k].deadline == undefined || orders[k].deadline! > ts) {
        this.openOrders.push({
          id: orderIds[k],
          order: orders[k],
          isLocked: false,
          ts: orders[k].submittedTimestamp ?? 0,
          onChain: true,
          traderAddr: traders[k],
        });
        this.orderIds.add(orderIds[k]);
      }
    }
    this.isExecuting = false;
    this.log(
      `${this.orderIds.size} open orders, ${
        this.openOrders.filter(
          (o) => o.order === undefined || o.order?.type == ORDER_TYPE_MARKET
        ).length
      } of which are market orders or unknown type`
    );
  }

  /**
   * Check that max(t) - min (t) <= threshold
   * @param timestamps Array of timestamps
   * @returns True if the timestamps are sufficiently close to each other
   */
  private checkSubmissionsInSync(timestamps: number[]): boolean {
    let gap = Math.max(...timestamps) - Math.min(...timestamps);
    if (gap > this.MAX_OUTOFSYNC_SECONDS) {
      this.log(`feed submissions not synced: ${timestamps}, gap = ${gap}`);
      return false;
    }
    return true;
  }

  private async _isExecutable(
    submission: {
      submission: PriceFeedSubmission;
      pxS2S3: [number, number | undefined];
    },
    ts: number,
    marketOnly: boolean
  ) {
    let hasOffChainOrders = false;
    for (let orderBundle of this.openOrders) {
      if (!orderBundle.onChain) {
        // this.log(
        //   `order ${orderBundle.id} on-chain status unknown - checking...`
        // );
        const thisOrder = await this.orTool![0].getOrderById(
          this.symbol,
          orderBundle.id
        );
        if (thisOrder != undefined) {
          this.log(
            `order ${orderBundle.id} found on-chain - execution proceeds`
          );
          orderBundle.onChain = true;
          orderBundle.order = thisOrder;
        } else {
          hasOffChainOrders = true;
          // this.log(`order ${orderBundle.id} is NOT on-chain - won't execute`);
        }
      }
    }
    // this.hasOffChainOrders = orders.some((orderBundle) => !orderBundle.onChain);
    this.hasOffChainOrders = hasOffChainOrders;
    // determine which orders could be executed now
    const isExecutable = this.openOrders.map(
      (orderBundle: OrderBundle, idx: number) => {
        // is this order currently locked or not yet on-chain
        if (
          orderBundle.isLocked ||
          !orderBundle.onChain ||
          (orderBundle.order?.type === ORDER_TYPE_MARKET) !== marketOnly
        ) {
          // if ((orderBundle.order.type === ORDER_TYPE_MARKET) !== marketOnly) {
          //   this.log(
          //     `order ${orderBundle.id} is market during conditional execution, or limit during market execution`
          //   );
          // } else {
          //   this.log(`order ${orderBundle.id} is locked or not on-chain`);
          // }
          return false;
        }
        if (
          (orderBundle.order.submittedTimestamp ?? Infinity) +
            this.config.executeDelaySeconds >
          Math.max(...submission.submission.timestamps)
        ) {
          // too early
          this.log(`order ${orderBundle.id} is too early`);
          return false;
        }
        if (
          Math.max(
            orderBundle.order.executionTimestamp ?? 0,
            orderBundle.order.submittedTimestamp ?? 0
          ) > Math.min(...submission.submission.timestamps)
        ) {
          const timeLimit = Math.max(
            orderBundle.order.executionTimestamp ?? 0,
            orderBundle.order.submittedTimestamp ?? 0
          );
          const oracleTime = Math.min(...submission.submission.timestamps);
          this.log(
            `oracle time older than order limit time: ${oracleTime} < ${timeLimit}`
          );
          return false;
        }
        const overdueMS = this.overdueForMS(orderBundle);
        const isMine = this.isMyOrder(orderBundle);
        // is this order not ours and not overdue?
        if (!isMine && overdueMS < 0) {
          return false;
        }
        // is it a market order without parents (so no need to check conditions)?
        if (this.isSingleMarketOrder(orderBundle.order)) {
          if (overdueMS > 0) {
            if (isMine) {
              this.log(
                `OWN ${orderBundle.order.type} order ${
                  orderBundle.id
                }, late for ${
                  (overdueMS + this.NON_EXECUTION_WAIT_TIME_MS) / 1_000
                } seconds`
              );
            } else {
              this.log(
                `PEER ${orderBundle.order.type} order ${
                  orderBundle.id
                }, late for ${
                  (overdueMS + this.NON_EXECUTION_WAIT_TIME_MS) / 1_000
                } seconds`
              );
              this.peerNonExecutionTimestampMS.delete(orderBundle.id);
            }
          }
          return overdueMS > 0;
        }
        // it's not an unconditional market order, so the conditions should be checked
        return undefined;
      }
    );
    // if anything is undetermined, we check the blockchain for prices
    const ordersToCheck = {
      orders: new Array<Order>(),
      ids: new Array<string>(),
      idxInOrders: new Array<number>(),
    };
    if (isExecutable.some((x) => x == undefined)) {
      let midPrice = await this.mktData!.getPerpetualMidPrice(this.symbol);
      for (let i = 0; i < isExecutable.length; i++) {
        if (isExecutable[i] == undefined) {
          if (
            this.openOrders[i].onChain &&
            this.openOrders[i].order?.reduceOnly
          ) {
            if (this.traders.has(this.openOrders[i].traderAddr)) {
              const side = this.traders.get(
                this.openOrders[i].traderAddr
              )?.side;
              isExecutable[i] =
                side !== CLOSED_SIDE && side !== this.openOrders[i].order.side;
            } else {
              isExecutable[i] = false;
            }
          } else if (
            (this.openOrders[i].order?.side == BUY_SIDE &&
              midPrice < this.openOrders[i].order?.limitPrice!) ||
            (this.openOrders[i].order?.side == SELL_SIDE &&
              midPrice > this.openOrders[i].order?.limitPrice!)
          ) {
            ordersToCheck.orders.push(this.openOrders[i].order);
            ordersToCheck.ids.push(this.openOrders[i].id);
            ordersToCheck.idxInOrders.push(i);
            // isExecutable[i] = await this.orTool![i % this.orTool!.length].isTradeable(
            //   this.openOrders[i].order,
            //   this.openOrders[i].id,
            //   ts,
            //   submission.pxS2S3,
            //   { rpcURL: await this.rpcManager.getRPC() }
            // );
          } else {
            isExecutable[i] = false;
          }
        }
      }
      if (ordersToCheck.ids.length > 0) {
        const executableBatch = await this.orTool![0].isTradeableBatch(
          ordersToCheck.orders,
          ordersToCheck.ids,
          ts,
          [
            submission.pxS2S3[0],
            submission.pxS2S3[1] ?? 0,
            submission.submission.isMarketClosed[0],
            submission.submission.isMarketClosed[1],
          ],
          { rpcURL: await this.rpcManager.getRPC() }
        );
        executableBatch.forEach((val, j) => {
          isExecutable[ordersToCheck.idxInOrders[j]] = val;
        });
      }
    }
    return isExecutable;
  }

  /**
   * execute collected orders. Removes executed or cancelled orders from list
   * @returns statistics for execution
   */
  public async executeOrders(marketOnly: boolean): Promise<{
    numOpen: number;
    numExecuted: number;
    numTraded: number;
  }> {
    if (this.orTool == undefined) {
      throw Error("objects not initialized");
    }
    this.moveNewOrdersToOrders();
    if (this.isExecuting) {
      return {
        numOpen: this.openOrders.length,
        numExecuted: -1,
        numTraded: 0,
      };
    }
    let numExecuted = 0;
    let numTraded = 0;
    let isExecutable: (boolean | undefined)[];
    let submission: {
      submission: PriceFeedSubmission;
      pxS2S3: [number, number | undefined];
    };
    this.lastExecuteOrdersCall = Date.now();
    try {
      //lock
      this.isExecuting = true;
      // get price submission
      submission = await this.orTool[0].fetchPriceSubmissionInfoForPerpetual(
        this.symbol
      );
      if (
        submission.submission.isMarketClosed.some((x) => x) ||
        !this.checkSubmissionsInSync(submission.submission.timestamps)
      ) {
        this.isExecuting = false;
        return {
          numOpen: this.openOrders.length,
          numExecuted: numExecuted,
          numTraded: numTraded,
        };
      }
      let ts = Math.floor(Date.now() / 1_000);
      isExecutable = await this._isExecutable(submission, ts, marketOnly);
    } catch (e: any) {
      // these are read only, if they fail for any reason we stop to force a network change
      this.log(`RPC error`);
      this.isExecuting = false;
      throw e;
    }

    if (
      isExecutable == undefined ||
      isExecutable.length == 0 ||
      !isExecutable.some((x) => x)
    ) {
      this.isExecuting = false;
      return {
        numOpen: this.openOrders.length,
        numExecuted: 0,
        numTraded: 0,
      };
    }
    if (this.config.gasStation !== undefined && this.config.gasStation !== "") {
      try {
        // check gas price
        const gasInfo = await fetch(this.config.gasStation)
          .then((res) => res.json())
          .then((info) => info as GasInfo);
        const gasPrice =
          typeof gasInfo.safeLow == "number"
            ? gasInfo.safeLow
            : (gasInfo.safeLow as GasPriceV2).maxfee;
        if (gasPrice > this.config.maxGasPriceGWei) {
          // if the lowest we need to pay is higher than the max allowed, we cannot proceed
          this.log(
            `gas price is too high: ${gasPrice} > ${this.config.maxGasPriceGWei} (low/market/high) = (${gasInfo.safeLow}/${gasInfo.standard}/${gasInfo.fast}) gwei, target max = ${this.config.maxGasPriceGWei} gwei)`
          );
          this.isExecuting = false;
          return {
            numOpen: this.openOrders.length,
            numExecuted: 0,
            numTraded: 0,
          };
        }
      } catch (e) {
        this.log("could not fetch gas price");
      }
    }

    try {
      // try to execute all executable ones we can handle in our executor tools
      let executeIds: Map<number, string[]> = new Map(); // executor idx -> order ids
      let executeIdxInOpenOrders: Array<number> = [];
      for (
        let k = 0;
        k < this.openOrders.length &&
        numExecuted < this.orTool.length * this.config.maxExecutionBatchSize;
        k++
      ) {
        if (isExecutable[k]) {
          numExecuted++;
          let refIdx = numExecuted % this.orTool.length;
          if (executeIds.get(refIdx) == undefined) {
            executeIds.set(refIdx, [this.openOrders[k].id]);
          } else {
            executeIds.get(refIdx)!.push(this.openOrders[k].id);
          }
          // will try to execute
          this.log(
            `${this.openOrders[k].order.type} order ${this.openOrders[k].id} assigned to bot #${refIdx} in this batch:\n${this.openOrders[k].order.side} ${this.openOrders[k].order.quantity} @ ${this.openOrders[k].order.limitPrice}`
          );
          executeIdxInOpenOrders.push(k);
          this.openOrders[k].isLocked = true;
        }
      }

      let txArray: ethers.ContractTransaction[] = [];
      try {
        let promiseArray: Array<Promise<ethers.ContractTransaction>> = [];
        for (let idx = 0; idx < this.orTool!.length; idx++) {
          let ids = executeIds.get(idx);
          if (ids !== undefined && ids.length > 0) {
            const ot = this.orTool![idx];
            this.log(`bot: ${idx}, addr ${ot.getAddress()}, ids: ${ids}`);
            promiseArray.push(
              ot.executeOrders(
                this.symbol,
                ids,
                this.earningsAddr,
                submission.submission,
                {
                  gasLimit: 2_000_000 + 1_000_000 * (ids.length - 1),
                  nonce: ot.getTransactionCount(),
                  rpcURL: await this.rpcManager.getRPC(),
                  splitTx: this.SPLIT_PX_UPDATE,
                }
              )
            );
          }
        }

        if (promiseArray.length > 0) {
          this.log(`submitting txns...`);
          txArray = await Promise.all(promiseArray);
          try {
            txArray.map((tx) =>
              this.log({
                hash: tx.hash,
                nonce: tx.nonce,
                from: tx.from,
                value: tx.value
                  ? `${ethers.utils.formatUnits(tx.value, "wei")} wei`
                  : undefined,
                gasPrice: tx.gasPrice
                  ? `${ethers.utils.formatUnits(tx.gasPrice!, "gwei")} gwei`
                  : undefined,
                gasLimit: tx.gasLimit
                  ? `${tx.gasLimit.toString()} gas`
                  : undefined,
              })
            );
          } catch (e) {
            console.error(e);
          }
          this.log("txns submitted");
        }
      } catch (e: any) {
        this.log(`error submitting txns`);
        // txn may have still made it, so we wait a bit and check before throwing the error
        setTimeout(async () => {
          // check order status
          for (let idx = 0; idx < this.orTool!.length; idx++) {
            const ids = executeIds.get(idx);
            if (ids && ids.length > 0) {
              const status = await this.mktData!.getOrdersStatus(
                this.symbol,
                ids
              );
              for (let i = 0; i < status.length; i++) {
                if (status[i] != OrderStatus.OPEN) {
                  this.removedOrders.add(ids[i]);
                } else {
                  // definitely not executed - error was real, rethrow unless it's just gas price
                  if ((e?.body as string)?.includes("gas price too low")) {
                    this.openOrders.forEach((ob) => {
                      if (ids.indexOf(ob.id) >= 0) {
                        // in this txn: unlock
                        ob.isLocked = false;
                      }
                    });
                  } else {
                    throw e;
                  }
                }
              }
            }
          }
        }, 10_000);
      }
      // release lock on the executor tool - then we check status and release lock on orders below
      this.isExecuting = false;
      // wait for all requests to go through and determine what was executed
      for (let idx = 0; idx < txArray.length; idx++) {
        let receipt: ethers.ContractReceipt;
        try {
          // console.log("waiting for block...");
          receipt = await executeWithTimeout(
            txArray[idx].wait(),
            60_000,
            "txn receipt timeout"
          );
          if (receipt.status != 1) {
            this.log(`receipt indicates txn failed: ${txArray[idx].hash}`);
            // transaction reverted, orders are probably still on the order book
            // unless someone executed them, but then events will take care of it before next round
            // which ids were executed by this bot?
            const ids = new Set(executeIds.get(idx));
            this.openOrders.forEach((ob) => {
              if (ids.has(ob.id)) {
                // in this txn: unlock
                ob.isLocked = false;
              }
            });
          } else {
            // leave locked, events will take care of it
            this.log(`receipt indicates txn success: ${txArray[idx].hash}`);
            const ids = new Set(executeIds.get(idx));
            this.openOrders = this.openOrders.filter((ob) => !ids.has(ob.id));
            this.newOrders = this.newOrders.filter((ob) => !ids.has(ob.id));
          }
        } catch (e) {
          // verifying txn failed - this is fine, events/regular refresh will remove or unlock as needed
          this.log(
            `could not fetch txn receipt: ${txArray[idx].hash} - checking status on-chain`
          );
          const ids = new Set(executeIds.get(idx));
          for (const id of ids) {
            const orderStatus = await this.mktData?.getOrderStatus(
              this.symbol,
              id
            );
            if (orderStatus === OrderStatus.OPEN) {
              // order is still open - unlock it
              this.openOrders.forEach((ob) => {
                if (ob.id === id) {
                  ob.isLocked = false;
                }
              });
            }
          }
          console.error(e);
        }
      }
    } catch (e: any) {
      this.log(`error in executeOrders:\n`);
      throw e;
    }
    this.isExecuting = false;
    return {
      numOpen: this.openOrders.length,
      numExecuted: numExecuted,
      numTraded: numTraded,
    };
  }
}
