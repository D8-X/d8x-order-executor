import {
  MarketData,
  PerpetualDataHandler,
  PriceFeedSubmission,
  ABK64x64ToFloat,
  COLLATERAL_CURRENCY_QUOTE,
  Multicall3__factory,
  MULTICALL_ADDRESS,
  Multicall3,
  Order,
  LimitOrderBook,
  ORDER_TYPE_MARKET,
  ORDER_TYPE_STOP_LIMIT,
  ORDER_TYPE_STOP_MARKET,
  BUY_SIDE,
  ORDER_TYPE_LIMIT,
  ZERO_ORDER_ID,
} from "@d8x/perpetuals-sdk";
import { providers } from "ethers";
import { Redis } from "ioredis";
import { constructRedis } from "../utils";
import {
  BrokerOrderMsg,
  ExecutionFailedMsg,
  ExecutorConfig,
  OrderBundle,
  PerpetualLimitOrderCancelledMsg,
  PerpetualLimitOrderCreatedMsg,
  Position,
  TradeMsg,
  UpdateMarginAccountMsg,
  UpdateMarkPriceMsg,
  UpdateUnitAccumulatedFundingMsg,
} from "../types";
import {
  IPerpetualOrder,
  PerpStorage,
} from "@d8x/perpetuals-sdk/dist/esm/contracts/IPerpetualManager";
import { IClientOrder } from "@d8x/perpetuals-sdk/dist/esm/contracts/LimitOrderBook";

export default class Distributor {
  // objects
  private md: MarketData;
  private redisSubClient: Redis;
  private redisPubClient: Redis;
  private providers: providers.StaticJsonRpcProvider[];

  // state
  private lastRefreshTime: Map<string, number> = new Map();
  private openPositions: Map<string, Map<string, Position>> = new Map(); // symbol => (trader => Position)
  private openOrders: Map<string, Map<string, OrderBundle>> = new Map(); // symbol => (digest => order bundle)
  private brokerOrders: Map<string, Map<string, number>> = new Map(); // symbol => (digest => received ts)
  private pxSubmission: Map<
    string,
    { submission: PriceFeedSubmission; pxS2S3: [number, number] }
  > = new Map(); // symbol => px submission
  private markPremium: Map<string, number> = new Map();
  private midPremium: Map<string, number> = new Map();
  private unitAccumulatedFunding: Map<string, number> = new Map();
  private messageSentAt: Map<string, number> = new Map();
  private pricesFetchedAt: Map<string, number> = new Map();
  public ready: boolean = false;

  // static info
  private config: ExecutorConfig;
  private isQuote: Map<string, boolean> = new Map();
  private symbols: string[] = [];
  private maintenanceRate: Map<string, number> = new Map();

  // constants

  // publish times must be within 10 seconds of each other, or submission will fail on-chain
  private MAX_OUTOFSYNC_SECONDS: number = 10;

  constructor(config: ExecutorConfig) {
    this.config = config;
    this.redisSubClient = constructRedis("commanderSubClient");
    this.redisPubClient = constructRedis("commanderPubClient");
    this.providers = this.config.rpcWatch.map(
      (url) => new providers.StaticJsonRpcProvider(url)
    );
    this.md = new MarketData(
      PerpetualDataHandler.readSDKConfig(config.sdkConfig)
    );
  }

  /**
   * Connects to the blockchain choosing a random RPC.
   * If none of the RPCs work, it sleeps before crashing.
   */
  public async initialize() {
    // Create a proxy instance to access the blockchain
    let success = false;
    let i = 0;
    this.providers = this.providers.sort(() => Math.random() - 0.5);
    while (!success && i < this.providers.length) {
      const results = (
        await Promise.allSettled([
          this.md.createProxyInstance(this.providers[i]),
        ])
      )[0];
      success = results.status === "fulfilled";
      i++;
    }
    if (!success) {
      console.log(
        `${new Date(
          Date.now()
        ).toISOString()}: all rpcs are down ${this.config.rpcWatch.join(", ")}`
      );
    }

    const info = await this.md.exchangeInfo();

    this.symbols = info.pools
      .filter(({ isRunning }) => isRunning)
      .map((pool) =>
        pool.perpetuals.map(
          (perpetual) =>
            `${perpetual.baseCurrency}-${perpetual.quoteCurrency}-${pool.poolSymbol}`
        )
      )
      .flat();

    for (const symbol of this.symbols) {
      // static info
      this.maintenanceRate.set(
        symbol,
        this.md.getPerpetualStaticInfo(symbol).maintenanceMarginRate
      );
      this.isQuote.set(
        symbol,
        this.md.getPerpetualStaticInfo(symbol).collateralCurrencyType ==
          COLLATERAL_CURRENCY_QUOTE
      );
      // price info
      this.pxSubmission.set(
        symbol,
        await this.md.fetchPriceSubmissionInfoForPerpetual(symbol)
      );
      // mark premium, accumulated funding per BC unit
      const perpState = await this.md
        .getReadOnlyProxyInstance()
        .getPerpetual(this.md.getPerpIdFromSymbol(symbol));
      this.markPremium.set(
        symbol,
        ABK64x64ToFloat(perpState.currentMarkPremiumRate.fPrice)
      );
      // mid premium = mark premium only at initialization time, will be updated with events
      this.midPremium.set(
        symbol,
        ABK64x64ToFloat(perpState.currentMarkPremiumRate.fPrice)
      );

      this.unitAccumulatedFunding.set(
        symbol,
        ABK64x64ToFloat(perpState.fUnitAccumulatedFunding)
      );
      // "preallocate" trader set
      this.openPositions.set(symbol, new Map());
      this.openOrders.set(symbol, new Map());
      this.brokerOrders.set(symbol, new Map());
      // dummy values
      this.lastRefreshTime.set(symbol, 0);
    }

    // Subscribe to blockchain events
    // console.log(`${new Date(Date.now()).toISOString()}: subscribing to blockchain event streamer...`);
    await this.redisSubClient.subscribe(
      "block",
      "UpdateMarkPriceEvent",
      "UpdateMarginAccountEvent",
      "UpdateUnitAccumulatedFundingEvent",
      "TradeEvent",
      "ExecutionFailedEvent",
      "PerpetualLimitOrderCreatedEvent",
      "PerpetualLimitOrderCancelledEvent",
      "BrokerOrderCreatedEvent",
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
        // else {
        //   console.log(`${new Date(Date.now()).toISOString()}: redis subscription success - ${count} active channels`);
        // }
      }
    );

    this.ready = true;
  }

  private log(obj: Object | string) {
    if (typeof obj == "string") {
      console.log(obj);
    } else {
      console.log(JSON.stringify(obj));
    }
  }

  private requireReady() {
    if (!this.ready) {
      throw new Error("not ready: await distributor.initialize()");
    }
  }

  /**
   * Listen to events for a number of blocks; requires initialize() first
   * @param maxBlocks number of blocks we will listen to event handlers
   * @returns void
   */
  public async run(): Promise<void> {
    this.requireReady();
    return new Promise<void>(async (resolve, reject) => {
      // fetch all accounts
      setInterval(async () => {
        if (
          Date.now() - Math.min(...this.lastRefreshTime.values()) <
          this.config.refreshOrdersIntervalSecondsMax * 1_000
        ) {
          return;
        }
        await this.refreshAllOpenOrders();
      }, 10_000);

      this.redisSubClient.on("message", async (channel, msg) => {
        switch (channel) {
          case "block": {
            for (const symbol of this.symbols) {
              this.checkOrders(symbol);
            }
            if (
              Date.now() - Math.min(...this.lastRefreshTime.values()) >
              this.config.refreshOrdersIntervalSecondsMax * 1_000
            ) {
              this.refreshAllOpenOrders();
            }
            break;
          }

          case "UpdateMarginAccountEvent": {
            const account: UpdateMarginAccountMsg = JSON.parse(msg);
            this.updatePosition({
              address: account.traderAddr,
              perpetualId: account.perpetualId,
              positionBC: account.positionBC,
              cashCC: account.cashCC,
              lockedInQC: account.lockedInQC,
              unpaidFundingCC: 0,
            });
            break;
          }

          case "UpdateMarkPriceEvent": {
            const { symbol, markPremium, midPremium }: UpdateMarkPriceMsg =
              JSON.parse(msg);
            this.markPremium.set(symbol, markPremium);
            this.midPremium.set(symbol, midPremium);
            break;
          }

          case "UpdateUnitAccumulatedFundingEvent": {
            const {
              symbol,
              unitAccumulatedFundingCC,
            }: UpdateUnitAccumulatedFundingMsg = JSON.parse(msg);
            this.unitAccumulatedFunding.set(symbol, unitAccumulatedFundingCC);
            break;
          }

          case "PerpetualLimitOrderCreatedEvent": {
            const {
              symbol,
              digest,
              trader,
              order,
            }: PerpetualLimitOrderCreatedMsg = JSON.parse(msg);
            this.addOrder(symbol, trader, digest, order);
            await this.checkOrders(symbol);
            break;
          }

          case "PerpetualLimitOrderCancelledEvent": {
            const { symbol, digest }: PerpetualLimitOrderCancelledMsg =
              JSON.parse(msg);
            this.removeOrder(symbol, digest, "removing cancelled order");
            break;
          }

          case "TradeEvent": {
            const { symbol, digest, trader }: TradeMsg = JSON.parse(msg);
            this.removeOrder(symbol, digest, "removing executed order", trader);
            break;
          }

          case "ExecutionFailedEvent": {
            const { symbol, digest, trader, reason }: ExecutionFailedMsg =
              JSON.parse(msg);
            if (reason != "cancel delay required") {
              this.removeOrder(symbol, digest, reason, trader);
            }
            break;
          }

          case "BrokerOrderCreatedEvent": {
            const { symbol, traderAddr, digest }: BrokerOrderMsg =
              JSON.parse(msg);
            this.addOrder(symbol, traderAddr, digest, undefined);
            this.brokerOrders.get(symbol)!.set(digest, Date.now());
            await this.checkOrders(symbol);
            break;
          }

          case "Restart": {
            process.exit(0);
          }
        }
      });
      await this.refreshAllOpenOrders();
    });
  }

  private addOrder(
    symbol: string,
    trader: string,
    digest: string,
    order?: Order
  ) {
    if (!this.openOrders.has(symbol)) {
      this.openOrders.set(symbol, new Map());
    }
    if (order != undefined || !this.openOrders.get(symbol)?.has(digest)) {
      this.openOrders.get(symbol)!.set(digest, {
        trader: trader,
        digest: digest,
        order: order,
        symbol: symbol,
      });
      console.log({
        info: "order added",
        symbol: symbol,
        trader: trader,
        digest: digest,
        onChain: order !== undefined,
      });
    }
  }

  private removeOrder(
    symbol: string,
    digest: string,
    reason?: string,
    trader?: string
  ) {
    this.openOrders.get(symbol)?.delete(digest);
    console.log({
      info: reason,
      symbol: symbol,
      trader: trader,
      digest: digest,
    });
  }

  private updatePosition(position: Position) {
    const symbol = this.md.getSymbolFromPerpId(position.perpetualId)!;
    if (!this.openPositions.has(symbol)) {
      this.openPositions.set(symbol, new Map());
    }
    if (position.positionBC !== 0) {
      this.openPositions.get(symbol)!.set(position.address, position);
    } else {
      this.openPositions.get(symbol)!.delete(position.address);
    }
  }

  /**
   * Refresh open orders, in parallel over perpetuals
   */
  private async refreshAllOpenOrders() {
    await Promise.allSettled(
      this.symbols.map((symbol) => this.refreshOpenOrders(symbol))
    );
  }

  private async refreshOpenOrders(symbol: string) {
    this.requireReady();
    if (
      Date.now() - (this.lastRefreshTime.get(symbol) ?? 0) <
      this.config.refreshOrdersIntervalSecondsMin * 1_000
    ) {
      return;
    }
    const chunkSize1 = 2 ** 8; // for orders
    const rpcProviders = this.config.rpcWatch.map(
      (url) => new providers.StaticJsonRpcProvider(url)
    );
    let providerIdx = Math.floor(Math.random() * rpcProviders.length);
    this.lastRefreshTime.set(symbol, Date.now());

    let tsStart = Date.now();

    const numOpenOrders = (
      await this.md.getOrderBookContract(symbol)!.orderCount()
    ).toNumber();

    // fetch orders
    const promises: Promise<{
      orders: IClientOrder.ClientOrderStructOutput[];
      orderHashes: string[];
      submittedTs: number[];
    }>[] = [];
    for (let i = 0; i < numOpenOrders; i += chunkSize1) {
      promises.push(
        this.md!.getOrderBookContract(symbol)
          .connect(rpcProviders[providerIdx])
          .pollRange(i, chunkSize1)
      );
      providerIdx = (providerIdx + 1) % rpcProviders.length;
    }

    if (!this.openOrders.has(symbol)) {
      this.openOrders.set(symbol, new Map<string, OrderBundle>());
    }
    const orderBundles: Map<string, OrderBundle> = new Map();
    for (let i = 0; i < promises.length; i += rpcProviders.length) {
      try {
        const chunks = await Promise.allSettled(
          promises.slice(i, i + rpcProviders.length)
        );
        for (const result of chunks) {
          if (result.status === "fulfilled") {
            const { orders, orderHashes, submittedTs } = result.value;
            for (let j = 0; j < orders.length; j++) {
              if (orderHashes[j] == ZERO_ORDER_ID) {
                continue;
              }
              const bundle = {
                symbol: symbol,
                trader: orders[j].traderAddr,
                digest: orderHashes[j],
                order: this.md!.smartContractOrderToOrder({
                  ...orders[j],
                  iPerpetualId: this.md!.getPerpIdFromSymbol(symbol),
                  executorAddr: this.config.rewardsAddress,
                  submittedTimestamp: submittedTs[j],
                } as IPerpetualOrder.OrderStruct),
              };
              orderBundles.set(orderHashes[j], bundle);
            }
          }
        }
      } catch (e) {
        console.log(
          `${symbol} ${new Date(Date.now()).toISOString()}: error`,
          e
        );
      }
    }
    this.openOrders.set(symbol, orderBundles);

    const orderArray = [...orderBundles.values()];
    const numOrders = {
      market: orderArray.filter(
        ({ order }) => order?.type === ORDER_TYPE_MARKET
      ).length,
      limit: orderArray.filter(({ order }) => order?.type === ORDER_TYPE_LIMIT)
        .length,
      stopMarket: orderArray.filter(
        ({ order }) => order?.type === ORDER_TYPE_STOP_MARKET
      ).length,
      stopLimit: orderArray.filter(
        ({ order }) => order?.type === ORDER_TYPE_STOP_LIMIT
      ).length,
      offChain: orderArray.filter(({ order }) => order == undefined).length,
    };
    if (orderBundles.size > 0) {
      // found some orders, report
      console.log({
        info: "open orders",
        symbol: symbol,
        time: new Date(Date.now()).toISOString(),
        ...numOrders,
        waited: `${Date.now() - tsStart} ms`,
      });
    }

    await this.refreshAccounts(symbol);
  }

  private async refreshAccounts(symbol: string) {
    const chunkSize2 = 2 ** 8; // for margin accounts
    const perpId = this.md.getPerpIdFromSymbol(symbol)!;
    const proxy = this.md.getReadOnlyProxyInstance();
    const rpcProviders = this.config.rpcWatch.map(
      (url) => new providers.StaticJsonRpcProvider(url)
    );
    let providerIdx = Math.floor(Math.random() * rpcProviders.length);
    this.lastRefreshTime.set(symbol, Date.now());
    const promises2: Promise<Multicall3.ResultStructOutput[]>[] = [];
    const addressChunks: string[][] = [];
    const multicall = Multicall3__factory.connect(
      MULTICALL_ADDRESS,
      rpcProviders[providerIdx]
    );
    const traderList = [...this.openOrders.get(symbol)!.values()].map(
      ({ trader }) => trader
    );
    for (let i = 0; i < traderList.length; i += chunkSize2) {
      const addressChunk = traderList.slice(i, i + chunkSize2);
      const calls: Multicall3.Call3Struct[] = addressChunk.map((addr) => ({
        allowFailure: true,
        target: proxy.address,
        callData: proxy.interface.encodeFunctionData("getMarginAccount", [
          perpId,
          addr,
        ]),
      }));
      promises2.push(
        multicall
          .connect(rpcProviders[providerIdx])
          .callStatic.aggregate3(calls)
      );
      addressChunks.push(addressChunk);
      providerIdx = (providerIdx + 1) % rpcProviders.length;
    }

    let tsStart = Date.now();
    for (let i = 0; i < promises2.length; i += rpcProviders.length) {
      try {
        const addressChunkBin = addressChunks.slice(i, i + rpcProviders.length);
        const accountChunk = await Promise.allSettled(
          promises2.slice(i, i + rpcProviders.length)
        );
        accountChunk.map((results, j) => {
          if (results.status === "fulfilled") {
            const addressChunk = addressChunkBin[j];
            results.value.map((result, k) => {
              if (result.success) {
                const account = proxy.interface.decodeFunctionResult(
                  "getMarginAccount",
                  result.returnData
                )[0] as PerpStorage.MarginAccountStructOutput;
                const position: Position = {
                  perpetualId: perpId,
                  address: addressChunk[k],
                  positionBC: ABK64x64ToFloat(account.fPositionBC),
                  cashCC: ABK64x64ToFloat(account.fCashCC),
                  lockedInQC: ABK64x64ToFloat(account.fLockedInValueQC),
                  unpaidFundingCC: 0,
                };
                position.unpaidFundingCC =
                  position.positionBC *
                  (this.unitAccumulatedFunding.get(symbol)! -
                    ABK64x64ToFloat(account.fUnitAccumulatedFundingStart));
                this.updatePosition(position);
              }
            });
          }
        });
      } catch (e) {
        console.log("Error fetching account chunk (RPC?)");
      }
    }
    if (this.openPositions.get(symbol)!.size > 0) {
      // found something, report
      console.log({
        info: "traders",
        symbol: symbol,
        time: new Date(Date.now()).toISOString(),
        count: this.openPositions.get(symbol)!.size,
        waited: `${Date.now() - tsStart} ms`,
      });
    }
  }

  /**
   * Checks if any accounts can be liquidated and publishes them via redis.
   * No RPC calls are made here, only price service
   * @param symbol Perpetual symbol
   * @returns number of accounts that can be liquidated
   */
  private async checkOrders(symbol: string) {
    this.requireReady();

    const orders = this.openOrders.get(symbol)!;
    if (orders.size == 0) {
      return;
    }

    try {
      if (
        Date.now() - (this.pricesFetchedAt.get(symbol) ?? 0) >
        this.config.fetchPricesIntervalSecondsMin * 1_000
      ) {
        const newPxSubmission =
          await this.md.fetchPriceSubmissionInfoForPerpetual(symbol);
        if (
          !this.checkSubmissionsInSync(newPxSubmission.submission.timestamps)
        ) {
          return false;
        }
        this.pxSubmission.set(symbol, newPxSubmission);
      }
    } catch (e) {
      console.log("error fetching from price service:");
      console.log(e);
      return false;
    }

    const curPx = this.pxSubmission.get(symbol)!;
    const ordersSent: Set<string> = new Set();
    const removeOrders: string[] = [];
    for (const [digest, orderBundle] of orders) {
      if (this.isExecutable(orderBundle, curPx.pxS2S3)) {
        const command = {
          symbol: symbol,
          digest: digest,
          trader: orderBundle.trader,
          onChain: orderBundle.order !== undefined,
        };

        // send command
        const msg = JSON.stringify(command);
        if (
          Date.now() - (this.messageSentAt.get(msg) ?? 0) >
          this.config.executeIntervalSecondsMin * 500
        ) {
          // log if market to see real time action
          if (
            orderBundle.order == undefined ||
            orderBundle.order?.type === ORDER_TYPE_MARKET
          ) {
            console.log({ info: "execute", ...command });
          }
          // console.log({ level: "execute", ...command });
          this.messageSentAt.set(msg, Date.now());
          ordersSent.add(msg);
          await this.redisPubClient.publish("ExecuteOrder", msg);
        }
        // remove stale broker orders
        if (
          orderBundle.order == undefined &&
          Date.now() - (this.brokerOrders.get(symbol)?.get(digest) ?? 0) >
            60_000
        ) {
          removeOrders.push(digest);
        }
      } else if (orderBundle.order?.type === ORDER_TYPE_MARKET) {
        console.log({
          info: "market order not executable",
          pxS2S3: curPx.pxS2S3,
          ...orderBundle,
        });
      }
    }
    for (const digest of removeOrders) {
      this.openOrders.get(symbol)?.delete(digest);
      this.brokerOrders.get(symbol)?.delete(digest);
    }
    return ordersSent.size > 0;
  }

  private isExecutable(
    order: OrderBundle,
    pxS2S3: [number, number | undefined]
  ) {
    if (!order.order) {
      return true;
    }
    if (order.order.executionTimestamp > Date.now() / 1_000) {
      return false;
    }

    const traderPos = this.openPositions
      .get(order.symbol)
      ?.get(order.trader)?.positionBC;
    const isBuy = order.order.side == BUY_SIDE;
    if (
      order.order.reduceOnly &&
      (traderPos == undefined ||
        (traderPos > 0 && isBuy) ||
        (traderPos < 0 && !isBuy))
    ) {
      return false;
    }

    if (order.order.parentChildOrderIds) {
      if (
        order.order.parentChildOrderIds[0] == ZERO_ORDER_ID &&
        this.openOrders
          .get(order.symbol)
          ?.has(order.order.parentChildOrderIds[1])
      ) {
        return false;
      }
    }

    const markPrice = pxS2S3[0] * (1 + this.markPremium.get(order.symbol)!);
    const midPrice = pxS2S3[0] * (1 + this.midPremium.get(order.symbol)!);
    const limitPrice = order.order.limitPrice;
    const triggerPrice = order.order.stopPrice;

    switch (order.order.type) {
      case ORDER_TYPE_MARKET:
        return true;

      case ORDER_TYPE_LIMIT:
        return (
          limitPrice != undefined &&
          ((isBuy && midPrice < limitPrice) ||
            (!isBuy && midPrice > limitPrice))
        );

      case ORDER_TYPE_STOP_MARKET:
        return (
          triggerPrice != undefined &&
          ((isBuy && markPrice > triggerPrice) ||
            (!isBuy && markPrice < triggerPrice))
        );

      case ORDER_TYPE_STOP_LIMIT:
        return (
          triggerPrice != undefined &&
          limitPrice != undefined &&
          ((isBuy && markPrice > triggerPrice) ||
            (!isBuy && markPrice < triggerPrice)) &&
          ((isBuy && midPrice < limitPrice) ||
            (!isBuy && midPrice > limitPrice))
        );

      default:
        break;
    }
    return undefined;
  }

  /**
   * Check that max(t) - min (t) <= threshold
   * @param timestamps Array of timestamps
   * @returns True if the timestamps are sufficiently close to each other
   */
  private checkSubmissionsInSync(timestamps: number[]): boolean {
    let gap = Math.max(...timestamps) - Math.min(...timestamps);
    if (
      gap > this.MAX_OUTOFSYNC_SECONDS &&
      Math.min(...timestamps) >= Math.floor(Date.now() / 1_000 - 5)
    ) {
      return false;
    }
    return true;
  }
}
