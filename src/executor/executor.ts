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
  Multicall3__factory,
  MULTICALL_ADDRESS,
  LimitOrderBook__factory,
  PERP_STATE_STR,
  ZERO_ADDRESS,
} from "@d8x/perpetuals-sdk";
import { BigNumber, ethers } from "ethers";
import { emitWarning } from "process";
import { GasInfo, GasPriceV2, OrderBundle, ExecutorConfig } from "../types";
import { Redis } from "ioredis";
import { constructRedis, executeWithTimeout } from "../utils";
import RPCManager from "./rpcManager";
import { IClientOrder } from "@d8x/perpetuals-sdk/dist/esm/contracts/LimitOrderBook";
import { formatUnits, parseUnits } from "@ethersproject/units";
import { StaticJsonRpcProvider } from "@ethersproject/providers";

export default class Executor {
  // objects
  private provider: ethers.providers.Provider | undefined;
  private recountProvider: StaticJsonRpcProvider[];
  private mktData: MarketData | undefined;
  private orTool: OrderExecutorTool[] | undefined;
  private redisSubClient: Redis;
  private rpcManager: RPCManager;
  private rpcManagerRecount: RPCManager;

  // parameters
  private earningsAddr: string;
  private privateKey: string[];
  private config: ExecutorConfig;
  private moduloTS: number; // primarily submit orders with createdTimestamp divisible by moduloTS (if defined), or all orders (if undefined)
  private residualTS: number;

  // constants
  private NON_EXECUTION_WAIT_TIME_MS = 40_000; // maximal time we leave for owner-peer to execute an executable order
  private MAX_OUTOFSYNC_SECONDS: number = 10; // publish times must be within 10 seconds of each other, or submission will fail on-chain
  private SPLIT_PX_UPDATE: boolean = false; // needed on zk because of performance issues on Polygon side
  private REFRESH_INTERVAL_MS: number; // how often to force a refresh of all orders, in miliseconds
  private EXECUTE_INTERVAL_MS: number;
  private RECOUNT_INTERVAL_MS: number;

  // state
  // private openOrders: OrderBundle[] = new Array<OrderBundle>();
  private newOrders: Map<number, OrderBundle[]> = new Map();
  private removedOrders: Set<string> = new Set();
  private isExecuting: boolean = false;
  private hasQueue: boolean = false;
  private peerNonExecutionTimestampMS: Map<string, number>; // orderId -> timestamp
  private lastRefreshTime: number = 0;
  private lastExecuteOrdersCall: number = 0;
  private blockNumber: number;
  private hasOffChainOrders: Map<number, boolean> = new Map(); // perpId => any orders received via broker ws?
  private lastOffChainOrder: number = Infinity;
  private traders: Map<string, MarginAccount[]> = new Map(); // traderAddr => margin accounts (all perps)

  private symbols: string[] = [];
  private perpetualIds: number[] = [];
  private orderBooks: string[] = []; // order book addresses
  private orders: Map<number, OrderBundle[]> = new Map(); // perp id => all (open) order bundles in order book
  private orderCount: Map<string, BigNumber> = new Map(); // symbol => count of all orders ever posted to order book
  private currentRecountProvider: number = 0;

  constructor(
    privateKey: string | string[],
    config: ExecutorConfig,
    moduloTS: number,
    residualTS: number,
    earningsAddr: string
  ) {
    this.privateKey = typeof privateKey == "string" ? [privateKey] : privateKey;
    this.earningsAddr = earningsAddr;
    this.moduloTS = moduloTS;
    this.residualTS = residualTS;
    this.config = config;
    this.EXECUTE_INTERVAL_MS = this.config.executeIntervalSeconds * 1_000;
    this.REFRESH_INTERVAL_MS = this.config.refreshOrdersSeconds * 1_000;
    this.RECOUNT_INTERVAL_MS = this.config.recountIntervalSeconds * 1_000;
    this.peerNonExecutionTimestampMS = new Map<string, number>();
    this.redisSubClient = constructRedis("ExecutorListener");
    this.blockNumber = 0;
    this.rpcManager = new RPCManager(this.config.executeRPC);
    this.rpcManagerRecount = new RPCManager(this.config.recountRPC);
    this.recountProvider = this.config.recountRPC.map(
      (url) =>
        new StaticJsonRpcProvider(url, {
          name: "",
          chainId: this.config.chainId,
        })
    );
  }

  private log(x: any) {
    console.log(
      JSON.stringify({
        time: new Date(Date.now()).toISOString(),
        timestamp: Date.now() / 1000,
        latestBlock: this.blockNumber,
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
    await this.updateExchangeInfo();

    await Promise.all(
      this.orTool.map(async (obj) => obj.createProxyInstance(this.provider))
    );

    // Fetch orders
    await this.refreshAllOrders();

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
      proxy: config.proxyAddr,
      earnings: this.earningsAddr,
      botCount: this.moduloTS,
      botIndex: this.residualTS,
      wallets: this.orTool.map((ot) => ot.getAddress()),
    });
    // // execute if needed, then wait for events
    // await this.executeOrders(true);
    // await this.executeOrders(false);
  }

  /**
   * Given the number of servers running and the index of the current one,
   * determines if an order is assigned to us
   * @param orderBundle Order bundle
   * @returns True if order id is assigned to this server
   */
  private isMyOrder(orderId: string): boolean {
    return (
      BigNumber.from(orderId.slice(0, 8)).toNumber() % this.moduloTS ==
      this.residualTS
    );
  }

  /**
   * Returns how late an order execution is
   * @param orderBundle Order bundle
   * @returns
   */
  private overdueForMS({ id, order }: { id: string; order: Order }): number {
    let ts = this.peerNonExecutionTimestampMS.get(id);
    let tsNow = Date.now();
    if (ts == undefined) {
      // ts =
      //   Math.max(tsNow / 1_000, orderBundle.order.executionTimestamp ?? 0, orderBundle.order.submittedTimestamp ?? 0) *
      //   1_000;
      ts =
        order.executionTimestamp && order.executionTimestamp > 0
          ? 1_000 * order.executionTimestamp
          : tsNow;
      this.peerNonExecutionTimestampMS.set(id, ts);
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
      for (const symbol of this.symbols) {
        setInterval(async () => {
          // should check if anything can be executed every minute +- 10 sec
          if (
            !this.hasQueue &&
            Date.now() - this.lastExecuteOrdersCall < this.EXECUTE_INTERVAL_MS
          ) {
            return;
          }
          await this.executeOrders(symbol, false);
        }, 10_000);
      }

      setInterval(async () => {
        // checks that we refresh all orders every hour +- 10 sec
        if (Date.now() - this.lastRefreshTime < this.REFRESH_INTERVAL_MS) {
          return;
        }
        await this.refreshAllOrders();
      }, 10_000);

      setInterval(async () => {
        try {
          await this.recount();
        } catch (e) {
          console.log(e);
          this.currentRecountProvider =
            (this.currentRecountProvider + 1) % this.recountProvider.length;
        }
      }, this.RECOUNT_INTERVAL_MS);

      this.redisSubClient.on("message", async (channel: any, msg: string) => {
        switch (channel) {
          case "switch-mode": {
            // listener changed mode: something failed so refresh orders
            await this.refreshAllOrders();
            break;
          }
          case "block": {
            numBlocks++;
            // new block - track
            this.blockNumber = Number(msg);
            for (let i = 0; i < this.symbols.length; i++) {
              if (this.hasOffChainOrders.get(this.perpetualIds[i])) {
                this.log(`execution attempt (broker sent order)`);
                await this.executeOrders(this.symbols[i], true);
              }
            }

            // if (numBlocks % 100 == 0) {
            //   this.log(
            //     JSON.stringify({
            //       marketOrders: this.openOrders.filter(
            //         (ob) => ob.order.type == ORDER_TYPE_MARKET
            //       ).length,
            //       totalOrders: this.openOrders.length,
            //     })
            //   );
            // }
            break;
          }
          case "PerpetualLimitOrderCreated": {
            const { perpetualId, trader, order, digest, fromEvent } =
              JSON.parse(msg);
            let ts = Math.floor(Date.now() / 1_000);
            this.log(
              `order received via ${
                fromEvent ? "event" : "broker"
              }: ${digest} from trader ${trader}`
            );
            // addOrder can trigger an early call to execute if it's a market order
            await this.addOrder(
              perpetualId,
              trader,
              digest,
              order,
              ts,
              fromEvent
            );

            break;
          }
          case "ExecutionFailed": {
            // if it failed because of "cancel delay required";
            //  --> order stays in the book
            // else:
            //  --> order is removed from the book
            const { perpetualId, digest, reason } = JSON.parse(msg);

            let ts = Math.floor(Date.now() / 1_000);
            this.log(`execution of ${digest} failed with reason ${reason}`);
            if (reason != "cancel delay required") {
              this.removeOrder(digest, ts);
            } else {
              // unlock to try again
              this.unlockOrder(perpetualId, digest);
            }

            break;
          }
          case "PerpetualLimitOrderCancelled": {
            const { digest } = JSON.parse(msg);
            let ts = Math.floor(Date.now() / 1_000);
            this.log(`order ${digest} has been cancelled`);
            this.removeOrder(digest, ts);

            break;
          }
          case "Trade": {
            const { perpetualId, digest, traderAddr } = JSON.parse(msg);

            let ts = Math.floor(Date.now() / 1_000);
            this.log(`order ${digest} has been executed`);
            this.removeOrder(digest, ts);
            this.updateAccount(traderAddr);
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
    const acct = await this.mktData!.positionRisk(trader);
    this.traders.set(trader, acct);
  }

  private unlockOrder(perpetualId: number, orderId: string) {
    const orders = this.orders.get(perpetualId)!;
    for (const o of orders) {
      if (o.id.toLocaleLowerCase() === orderId.toLocaleLowerCase()) {
        o.isLocked = false;
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
    perpetualId: number,
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
      this.hasOffChainOrders.set(perpetualId, true);
    }

    let orderBundle = {
      id: digest,
      order: order,
      isLocked: false,
      ts: ts,
    } as OrderBundle;

    let bundles = this.orders.get(perpetualId)!;
    let idx =
      bundles === undefined ? -1 : bundles.findIndex((b) => b.id === digest);

    if (idx >= 0) {
      bundles[idx].order = onChain ? order! : bundles[idx].order;
      bundles[idx].onChain = onChain || bundles[idx].onChain;
    } else {
      if (!this.newOrders.has(perpetualId)) {
        this.newOrders.set(perpetualId, []);
      }
      const newOrders = this.newOrders.get(perpetualId)!;
      let jdx = newOrders.findIndex((b) => b.id == digest);
      if (jdx >= 0) {
        newOrders[jdx].order = onChain
          ? order!
          : this.newOrders.get(perpetualId)![jdx].order;
        newOrders[jdx].onChain = onChain || newOrders[jdx].onChain;
      } else {
        newOrders.push(orderBundle);
      }
    }
    if (!this.traders.has(fromTrader)) {
      const acct = await this.mktData!.positionRisk(fromTrader);
      this.traders.set(fromTrader, acct);
    }

    if (order && this.isSingleMarketOrder(order)) {
      if (this.isExecuting) {
        this.hasQueue = true;
      } else {
        this.log(`${onChain ? "event" : "ws"} triggered execution`);
        await this.executeOrders(order.symbol, true);
      }
    }
  }

  public removeOrder(digest: string, ts: number) {
    this.removedOrders.add(digest);
  }

  /**
   * Copy new orders to order array and delete reference in newOrders
   */
  private moveNewOrdersToOrders() {
    let hasQueue = false;
    for (const perpId of this.perpetualIds) {
      const newOrders = this.newOrders.get(perpId);
      if (newOrders === undefined) {
        continue;
      }
      for (let k = newOrders.length - 1; k >= 0; k--) {
        this.log(
          `moving new ${newOrders[k].order?.type} order to open orders, id ${newOrders[k].id}`
        );
        // remove if it exists
        let newOb = newOrders[k];
        const orders = this.orders.get(perpId)!;
        const idxInOpenOrders = orders.findIndex((b) => b.id === newOb.id);
        if (idxInOpenOrders < 0) {
          orders.push(newOb);
        } else {
          orders[idxInOpenOrders] = newOb;
        }
        // remove from new orders
        newOrders.pop();
        // remove other orders from open orders
        this.orders.set(
          perpId,
          orders.filter((ob) => !this.removedOrders.has(ob.id))
        );
      }
      hasQueue =
        hasQueue ||
        this.orders
          .get(perpId)!
          .some((x) => x.order?.type === ORDER_TYPE_MARKET);
    }

    this.removedOrders = new Set<string>();
    this.newOrders = new Map();
    this.hasQueue = hasQueue;
  }

  private async recount() {
    // this.log("recount");
    const obI = LimitOrderBook__factory.createInterface();
    const multicall = Multicall3__factory.connect(
      MULTICALL_ADDRESS,
      this.recountProvider[this.currentRecountProvider]
    );
    const calls = this.symbols.map((symbol) => ({
      target: this.mktData!.getOrderBookContract(symbol).address,
      allowFailure: false,
      callData: obI.encodeFunctionData("numberOfAllDigests"),
    }));
    // order count in all perps (all - irrespective of deletion)
    const res = await multicall.callStatic.aggregate3(calls);
    const orderCounts = res.map(
      ({ returnData }) =>
        obI.decodeFunctionResult(
          "numberOfAllDigests",
          returnData
        )[0] as BigNumber
    );
    for (let i = 0; i < this.symbols.length; i++) {
      const curCount =
        this.orderCount.get(this.symbols[i]) ?? BigNumber.from(0);
      this.orderCount.set(this.symbols[i], orderCounts[i]);
      if (curCount.lt(orderCounts[i]) && !this.isExecuting) {
        // new order - refresh and execute in perp
        this.log("recount triggered refresh + execution");
        await this.refreshPerpetualOrders(this.symbols[i]);
        await this.executeOrders(this.symbols[i], true);
      }
    }
  }

  private async updateExchangeInfo() {
    const md = this.mktData!;
    // exchange info
    const info = await md.exchangeInfo();
    this.symbols = info.pools
      .filter(({ isRunning }) => isRunning)
      .map(({ poolSymbol, perpetuals }) =>
        perpetuals
          .filter(({ state }) => state == "NORMAL")
          .map(
            ({ baseCurrency, quoteCurrency }) =>
              `${baseCurrency}-${quoteCurrency}-${poolSymbol}`
          )
      )
      .flat();
    this.perpetualIds = info.pools
      .filter(({ isRunning }) => isRunning)
      .map(({ poolSymbol, perpetuals }) =>
        perpetuals.filter(({ state }) => state == "NORMAL").map(({ id }) => id)
      )
      .flat();
    this.orderBooks = this.symbols.map(
      (symbol) => md.getOrderBookContract(symbol).address
    );
    const obI = LimitOrderBook__factory.createInterface();
    const multicall = Multicall3__factory.connect(
      MULTICALL_ADDRESS,
      this.provider!
    );
    const calls = this.symbols.map((symbol) => ({
      target: this.mktData!.getOrderBookContract(symbol).address,
      allowFailure: false,
      callData: obI.encodeFunctionData("numberOfAllDigests"),
    }));
    // order count in all perps (all - irrespective of deletion)
    const res = await multicall.callStatic.aggregate3(calls);
    res.forEach(({ success, returnData }, idx) => {
      const count = obI.decodeFunctionResult(
        "numberOfAllDigests",
        returnData
      )[0] as BigNumber;
      this.orderCount.set(this.symbols[idx], count);
    });
  }

  private async refreshAllOrders() {
    this.log("refreshing all orders");
    const md = this.mktData!;
    const obI = LimitOrderBook__factory.createInterface();
    const multicall = Multicall3__factory.connect(
      MULTICALL_ADDRESS,
      this.provider!
    );
    const calls = this.symbols.map((symbol) => ({
      target: md.getOrderBookContract(symbol).address,
      allowFailure: false,
      callData: obI.encodeFunctionData("orderCount"),
    }));
    // order count in all perps (open only)
    const res = await multicall.callStatic.aggregate3(calls);
    const orderCounts = res.map(
      ({ success, returnData }) =>
        obI.decodeFunctionResult("orderCount", returnData)[0] as number
    );

    const calls2 = this.symbols.map((symbol, i) => ({
      target: md.getOrderBookContract(symbol).address,
      allowFailure: true,
      callData: obI.encodeFunctionData("pollLimitOrders", [
        ZERO_ORDER_ID,
        orderCounts[i],
      ]),
    }));

    const res2 = await multicall.callStatic.aggregate3(calls2);
    const ts = Date.now();
    const orders = res2
      .map(({ success, returnData }) =>
        success
          ? obI.decodeFunctionResult("pollLimitOrders", returnData)
          : [[], []]
      )
      .map((decoded) => {
        const scOrders = decoded[0] as IClientOrder.ClientOrderStructOutput[];
        const orderIds = decoded[1] as string[];
        return orderIds.map((id, j) => {
          return {
            id: id,
            order: md.smartContractOrderToOrder(scOrders[j]),
            onChain: true,
            ts: ts,
            isLocked: false,
            traderAddr: scOrders[j].traderAddr,
          } as OrderBundle;
        });
      });
    orders.forEach((orderBundles, i) => {
      this.orders.set(this.perpetualIds[i], orderBundles);
    });
    this.lastRefreshTime = ts;
  }

  /**
   * Reset open orders-array; refresh all open orders
   */
  public async refreshPerpetualOrders(symbol: string) {
    this.log(`refreshing ${symbol} orders`);
    if (this.orTool == undefined) {
      throw Error("orTool not defined");
    }
    const perpId = this.mktData!.getPerpIdFromSymbol(symbol);
    // let openOrders = new Array<OrderBundle>();
    // this.openOrders = [];
    this.orders.set(perpId, []);
    // let newOrderIds = new Set<string>();
    // this.orderIds = new Set();

    // this.newOrders = [];
    this.newOrders.set(perpId, []);
    this.removedOrders = new Set<string>();

    this.lastRefreshTime = Date.now();
    const totalOrders = await this.orTool[0].numberOfOpenOrders(symbol, {
      rpcURL: await this.rpcManager.getRPC(),
    });
    const allOrders = await this.orTool[0].pollLimitOrders(
      symbol,
      totalOrders,
      undefined,
      {
        rpcURL: await this.rpcManager.getRPC(),
      }
    );
    const ts = Math.round(Date.now() / 1000);
    const orders = allOrders[0];
    const orderIds = allOrders[1];
    const traders = allOrders[2];
    for (let k = 0; k < orders.length; k++) {
      if (orders[k].deadline == undefined || orders[k].deadline! > ts) {
        this.orders.get(perpId)!.push({
          id: orderIds[k],
          order: orders[k],
          isLocked: false,
          ts: orders[k].submittedTimestamp ?? 0,
          onChain: true,
          traderAddr: traders[k],
        });
        // this.orderIds.add(orderIds[k]);
      }
    }
    this.perpetualIds.forEach((id) => {
      this.log(
        `${this.orders.get(id)!.length} open orders, ${
          this.orders.get(id)!.filter((o) => o.order.type == ORDER_TYPE_MARKET)
            .length
        } of which are market orders`
      );
    });
  }

  private async refreshOffChainStatus() {
    let ordersToCheck: { perpId: number; orderId: string }[] = [];
    for (const perpId of this.perpetualIds) {
      let hasOffChainOrders = false;
      for (let orderBundle of this.orders.get(perpId)!) {
        if (!orderBundle.onChain) {
          if (Date.now() > orderBundle.ts + 60_000) {
            this.log(
              `order ${orderBundle.id} off-chain for over a minute - ignoring`
            );
          } else {
            this.log(
              `order ${orderBundle.id} on-chain status unknown - checking...`
            );
            ordersToCheck.push({ perpId: perpId, orderId: orderBundle.id });
            hasOffChainOrders = true;
          }
        }
      }
      this.hasOffChainOrders.set(perpId, hasOffChainOrders);
    }

    if (ordersToCheck.length < 1) {
      return;
    }
    const multicall = Multicall3__factory.connect(
      MULTICALL_ADDRESS,
      this.provider!
    );
    const obI = LimitOrderBook__factory.createInterface();
    const calls = ordersToCheck.map(({ perpId, orderId }) => ({
      target: this.orderBooks[perpId],
      allowFailure: false,
      callData: obI.encodeFunctionData("orderOfDigest", [orderId]),
    }));
    const res = await multicall.callStatic.aggregate3(calls);
    const orders = res.map(({ returnData }) => {
      const scOrder = obI.decodeFunctionResult("orderOfDigest", returnData)[0];
      return {
        traderAddr: scOrder.traderAddr,
        order: this.mktData!.smartContractOrderToOrder(scOrder),
      };
    });
    ordersToCheck.forEach(({ perpId, orderId }, idx) => {
      const bundles = this.orders.get(perpId)!;
      for (let i = 0; i < bundles.length; i++) {
        if (bundles[i].id === orderId) {
          bundles[i].onChain = orders[idx].traderAddr !== ZERO_ADDRESS;
          bundles[i].order = orders[idx].order;
          bundles[i].ts = Date.now();
        }
      }
    });
  }

  private async getExecutableOffChain(market: boolean) {
    let executable: OrderBundle[] = [];
    for (let i = 0; i < this.perpetualIds.length; i++) {
      const perpId = this.perpetualIds[i];
      const symbol = this.symbols[i];
      const submission =
        await this.mktData!.fetchPriceSubmissionInfoForPerpetual(symbol);
      const bundles = this.orders.get(perpId)!;
      const executableIds: string[] = [];
      bundles.forEach(({ id, order, onChain, isLocked }) => {
        if (
          !isLocked &&
          onChain &&
          (order.type === ORDER_TYPE_MARKET) === market &&
          Math.max(...submission.submission.timestamps) -
            Math.min(...submission.submission.timestamps) <=
            this.MAX_OUTOFSYNC_SECONDS &&
          order.submittedTimestamp! + this.config.executeDelaySeconds >
            Math.max(...submission.submission.timestamps) &&
          order.executionTimestamp <=
            Math.min(...submission.submission.timestamps)
        ) {
          const overdueMS = this.overdueForMS({ id, order });
          const isMine = this.isMyOrder(id);
          // is this order not ours and not overdue?
          if (!isMine && overdueMS < 0) {
            return;
          }
          // is it a market order without parents (so no need to check conditions)?
          if (this.isSingleMarketOrder(order)) {
            if (overdueMS > 0) {
              if (isMine) {
                this.log(
                  `OWN ${order.type} order ${id}, late for ${
                    (overdueMS + this.NON_EXECUTION_WAIT_TIME_MS) / 1_000
                  } seconds`
                );
              } else {
                this.log(
                  `PEER ${order.type} order ${id}, late for ${
                    (overdueMS + this.NON_EXECUTION_WAIT_TIME_MS) / 1_000
                  } seconds`
                );
                this.peerNonExecutionTimestampMS.delete(id);
              }
            }
            if (overdueMS > 0) {
              executableIds.push(id);
            }
          }
          // it's not an unconditional market order, so the conditions should be checked
          return undefined;
        }
      });
    }
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

  private async getExecutableMask(
    symbol: string,
    submission: {
      submission: PriceFeedSubmission;
      pxS2S3: [number, number | undefined];
    },
    ts: number,
    marketOnly: boolean
  ) {
    await this.refreshOffChainStatus();
    const perpId = this.mktData!.getPerpIdFromSymbol(symbol);
    const orders = this.orders.get(perpId)!;
    // determine which orders could be executed now
    const isExecutable = orders.map((orderBundle: OrderBundle, idx: number) => {
      // is this order currently locked or not yet on-chain
      if (
        orderBundle.isLocked ||
        !orderBundle.onChain ||
        (orderBundle.order.type === ORDER_TYPE_MARKET) !== marketOnly
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
        orderBundle.order.submittedTimestamp! +
          this.config.executeDelaySeconds >
        Math.max(...submission.submission.timestamps)
      ) {
        // too early
        // this.log(`order ${orderBundle.id} is too early`);
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
      const isMine = this.isMyOrder(orderBundle.id);
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
    });

    // if anything is undetermined, we check the blockchain for prices
    const ordersToCheck = {
      orders: [] as Order[],
      ids: [] as string[],
      idxInOrders: [] as number[],
    };
    if (isExecutable.some((x) => x == undefined)) {
      let midPrice = await this.mktData!.getPerpetualMidPrice(symbol);
      for (let i = 0; i < isExecutable.length; i++) {
        if (isExecutable[i] == undefined) {
          if (orders[i].onChain && orders[i].order?.reduceOnly) {
            if (this.traders.has(orders[i].traderAddr)) {
              const side = this.traders
                .get(orders[i].traderAddr)
                ?.find((acct) => acct.symbol === symbol)?.side;
              isExecutable[i] =
                side !== undefined &&
                side !== CLOSED_SIDE &&
                side !== orders[i].order.side;
            } else {
              isExecutable[i] = false;
            }
          } else if (
            (orders[i].order.side == BUY_SIDE &&
              midPrice < orders[i].order.limitPrice!) ||
            (orders[i].order.side == SELL_SIDE &&
              midPrice > orders[i].order.limitPrice!)
          ) {
            ordersToCheck.orders.push(orders[i].order);
            ordersToCheck.ids.push(orders[i].id);
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

  private async checkGasPrice() {
    const gasPrice = await this.provider!.getGasPrice();
    if (gasPrice > parseUnits(this.config.maxGasPriceGWei.toString(), "gwei")) {
      this.log(
        `gas price is too high: ${formatUnits(gasPrice, "gwei")} gwei > ${
          this.config.maxGasPriceGWei
        } gwei`
      );
      return false;
    }
    return true;
  }

  /**
   * execute collected orders. Removes executed or cancelled orders from list
   * @returns statistics for execution
   */
  public async executeOrders(
    symbol: string,
    marketOnly: boolean
  ): Promise<{
    numOpen: number;
    numExecuted: number;
    numTraded: number;
  }> {
    if (this.orTool == undefined) {
      throw Error("objects not initialized");
    }
    this.moveNewOrdersToOrders();
    const perpId = this.mktData!.getPerpIdFromSymbol(symbol);
    const orders = this.orders.get(perpId)!;
    if (this.isExecuting) {
      return {
        numOpen: orders.length,
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
        symbol
      );
      if (
        submission.submission.isMarketClosed.some((x) => x) ||
        !this.checkSubmissionsInSync(submission.submission.timestamps)
      ) {
        this.isExecuting = false;
        return {
          numOpen: orders.length,
          numExecuted: numExecuted,
          numTraded: numTraded,
        };
      }
      let ts = Math.floor(Date.now() / 1_000);
      isExecutable = await this.getExecutableMask(
        symbol,
        submission,
        ts,
        marketOnly
      );
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
        numOpen: orders.length,
        numExecuted: 0,
        numTraded: 0,
      };
    }

    if (!(await this.checkGasPrice())) {
      this.isExecuting = false;
      return {
        numOpen: orders.length,
        numExecuted: numExecuted,
        numTraded: numTraded,
      };
    }

    try {
      // try to execute all executable ones we can handle in our executor tools
      let executeIds: Map<number, string[]> = new Map(); // executor idx -> order ids
      let executeIdxInOpenOrders: number[] = [];
      for (
        let k = 0;
        k < this.orders.get(perpId)!.length &&
        numExecuted < this.orTool.length * this.config.maxExecutionBatchSize;
        k++
      ) {
        if (isExecutable[k]) {
          let refIdx = numExecuted % this.orTool.length;
          if (executeIds.get(refIdx) == undefined) {
            executeIds.set(refIdx, [this.orders.get(perpId)![k].id]);
          } else {
            executeIds.get(refIdx)!.push(this.orders.get(perpId)![k].id);
          }
          numExecuted++;
          // will try to execute
          this.log(
            `${orders[k].order.type} order ${orders[k].id} assigned to bot #${refIdx} in this batch:\n${orders[k].order.side} ${orders[k].order.quantity} @ ${orders[k].order.limitPrice}`
          );
          executeIdxInOpenOrders.push(k);
          orders[k].isLocked = true;
        }
      }

      let txArray: ethers.ContractTransaction[] = [];
      try {
        let promiseArray: Promise<ethers.ContractTransaction>[] = [];
        for (let idx = 0; idx < this.orTool!.length; idx++) {
          let ids = executeIds.get(idx);
          if (ids !== undefined && ids.length > 0) {
            const ot = this.orTool![idx];
            this.log(`bot: ${idx}, addr ${ot.getAddress()}, ids: ${ids}`);
            promiseArray.push(
              ot.executeOrders(
                symbol,
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
        // now we can release the main lock
        this.isExecuting = false;
      } catch (e: any) {
        this.log(`error submitting txns`);
        // txn may have still made it, so we wait a bit and check before throwing the error
        setTimeout(async () => {
          // check order status
          for (let idx = 0; idx < this.orTool!.length; idx++) {
            const ids = executeIds.get(idx);
            if (ids && ids.length > 0) {
              const status = await this.mktData!.getOrdersStatus(symbol, ids);
              for (let i = 0; i < status.length; i++) {
                if (status[i] != OrderStatus.OPEN) {
                  this.removedOrders.add(ids[i]);
                } else {
                  // definitely not executed - error was real
                  throw e;
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
            orders.forEach((ob) => {
              if (ids.has(ob.id)) {
                // in this txn: unlock
                ob.isLocked = false;
              }
            });
          } else {
            // leave locked, events will take care of it
            this.log(`receipt indicates txn success: ${txArray[idx].hash}`);
            const ids = new Set(executeIds.get(idx));
            this.orders.set(
              perpId,
              orders.filter((ob) => !ids.has(ob.id))
            );
            const newOrders = this.newOrders.get(perpId);
            if (newOrders !== undefined) {
              this.newOrders.set(
                perpId,
                newOrders.filter((ob) => !ids.has(ob.id))
              );
            }
          }
        } catch (e) {
          // verifying txn failed - this is fine, events/regular refresh will remove or unlock as needed
          this.log(
            `could not fetch txn receipt: ${txArray[idx].hash} - checking status on-chain`
          );
          const ids = new Set(executeIds.get(idx));
          for (const id of ids) {
            const orderStatus = await this.mktData?.getOrderStatus(symbol, id);
            if (orderStatus === OrderStatus.OPEN) {
              // order is still open - unlock it
              orders.forEach((ob) => {
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
      numOpen: orders.length,
      numExecuted: numExecuted,
      numTraded: numTraded,
    };
  }
}
