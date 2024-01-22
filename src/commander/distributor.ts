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
  private openPositions: Map<string, Map<string, Position>> = new Map();
  private openOrders: Map<string, Map<string, OrderBundle>> = new Map(); // symbol => (digest => order bundle)
  private pxSubmission: Map<
    string,
    { submission: PriceFeedSubmission; pxS2S3: [number, number] }
  > = new Map();
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
      "ExecutionFailedEvent",
      "PerpetualLimitOrderCreatedEvent",
      "PerpetualLimitOrderCancelledEvent",
      "BrokerOrderCreatedEvent",
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
              Date.now() - Math.min(...this.lastRefreshTime.values()) <
              this.config.refreshOrdersIntervalSecondsMax * 1_000
            ) {
              return;
            }
            await this.refreshAllOpenOrders();
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
            this.updateOrder(symbol, trader, digest, order);
          }

          case "PerpetualLimitOrderCancelledEvent": {
            const { symbol, digest }: PerpetualLimitOrderCancelledMsg =
              JSON.parse(msg);
            this.openOrders.get(symbol)?.delete(digest);
          }

          case "ExecutionFailedEvent": {
            const { symbol, digest, reason }: ExecutionFailedMsg =
              JSON.parse(msg);
            if (reason != "cancel delay required") {
              this.openOrders.get(symbol)?.delete(digest);
            }
          }

          case "BrokerOrderCreatedEvent": {
            const { symbol, traderAddr, digest }: BrokerOrderMsg =
              JSON.parse(msg);
            this.updateOrder(symbol, traderAddr, digest, undefined);
          }
        }
      });
      await this.refreshAllOpenOrders();
    });
  }

  private updateOrder(
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
    }
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

    let tsStart: number;
    console.log(`${symbol}: fetching number of accounts ... `);
    tsStart = Date.now();

    const numOpenOrders = (
      await this.md.getOrderBookContract(symbol)!.orderCount()
    ).toNumber();
    console.log({
      symbol: symbol,
      time: new Date(Date.now()).toISOString(),
      activeAccounts: numOpenOrders,
    });

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
        tsStart = Date.now();
        const chunks = await Promise.allSettled(
          promises.slice(i, i + rpcProviders.length)
        );
        for (const result of chunks) {
          if (result.status === "fulfilled") {
            const { orders, orderHashes, submittedTs } = result.value;
            for (let j = 0; j < orders.length; j++) {
              orderBundles.set(orderHashes[j], {
                symbol: symbol,
                trader: orders[j].traderAddr,
                digest: orderHashes[j],
                order: this.md!.smartContractOrderToOrder({
                  ...orders[j],
                  executorAddr: this.config.rewardsAddress,
                  submittedTimestamp: submittedTs[j],
                } as IPerpetualOrder.OrderStruct),
              });
            }
          }
        }
      } catch (e) {
        console.log(`${symbol} ${new Date(Date.now()).toISOString()}: error`);
      }
    }
    this.openOrders.set(symbol, orderBundles);
    console.log({
      symbol: symbol,
      time: new Date(Date.now()).toISOString(),
      orders: orderBundles.size,
      waited: `${Date.now() - tsStart} ms`,
    });

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
    console.log({
      symbol: symbol,
      time: new Date(Date.now()).toISOString(),
      accounts: this.openPositions.get(symbol)!.size,
      waited: `${Date.now() - tsStart} ms`,
    });
  }

  /**
   * Checks if any accounts can be liquidated and publishes them via redis.
   * No RPC calls are made here, only price service
   * @param symbol Perpetual symbol
   * @returns number of accounts that can be liquidated
   */
  private async checkOrders(symbol: string) {
    this.requireReady();
    if (
      Date.now() - (this.pricesFetchedAt.get(symbol) ?? 0) <
      this.config.fetchPricesIntervalSecondsMin * 1_000
    ) {
      return undefined;
    }
    const orders = this.openOrders.get(symbol)!;
    if (orders.size == 0) {
      return;
    }
    try {
      const newPxSubmission =
        await this.md.fetchPriceSubmissionInfoForPerpetual(symbol);
      if (!this.checkSubmissionsInSync(newPxSubmission.submission.timestamps)) {
        return false;
      }
      this.pxSubmission.set(symbol, newPxSubmission);
    } catch (e) {
      console.log("error fetching from price service:");
      console.log(e);
      return false;
    }

    const curPx = this.pxSubmission.get(symbol)!;
    const ordersSent: Set<string> = new Set();

    for (const digest of orders.keys()) {
      const orderBundle = orders.get(digest)!;
      if (this.isExecutable(orderBundle, curPx.pxS2S3)) {
        const msg = JSON.stringify({
          symbol: symbol,
          digest: digest,
          trader: orderBundle.trader,
          onChain: orderBundle.order !== undefined,
        });
        if (
          Date.now() - (this.messageSentAt.get(msg) ?? 0) >
          this.config.executeIntervalSecondsMin * 1_000
        ) {
          await this.redisPubClient.publish("ExecuteOrder", msg);
          this.messageSentAt.set(msg, Date.now());
        }
        ordersSent.add(msg);
      }
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
    if (
      order.order.reduceOnly &&
      traderPos !== undefined &&
      traderPos * order.order.quantity >= 0
    ) {
      return false;
    }

    if (order.order.type == ORDER_TYPE_MARKET) {
      return true;
    } else if (order.order.type == ORDER_TYPE_LIMIT) {
      if (order.order.side == BUY_SIDE) {
        return (
          order.order.limitPrice != undefined &&
          pxS2S3[0] * (1 + this.markPremium.get(order.symbol)!) <=
            order.order.limitPrice
        );
      } else {
        return (
          order.order.stopPrice != undefined &&
          pxS2S3[0] * (1 + this.markPremium.get(order.symbol)!) >=
            order.order.stopPrice
        );
      }
    } else if (
      order.order.type == ORDER_TYPE_STOP_LIMIT ||
      order.order.type == ORDER_TYPE_STOP_MARKET
    ) {
      if (order.order.side == BUY_SIDE) {
        return (
          order.order.stopPrice != undefined &&
          pxS2S3[0] * (1 + this.midPremium.get(order.symbol)!) <=
            order.order.stopPrice
        );
      } else {
        return (
          order.order.stopPrice != undefined &&
          pxS2S3[0] * (1 + this.midPremium.get(order.symbol)!) >=
            order.order.stopPrice
        );
      }
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
    if (gap > this.MAX_OUTOFSYNC_SECONDS) {
      return false;
    }
    return true;
  }
}
