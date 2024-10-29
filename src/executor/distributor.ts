import {
  MarketData,
  PerpetualDataHandler,
  ABK64x64ToFloat,
  COLLATERAL_CURRENCY_QUOTE,
  Multicall3__factory,
  MULTICALL_ADDRESS,
  Multicall3,
  Order,
  ORDER_TYPE_MARKET,
  ORDER_TYPE_STOP_LIMIT,
  ORDER_TYPE_STOP_MARKET,
  BUY_SIDE,
  ORDER_TYPE_LIMIT,
  ZERO_ORDER_ID,
  SELL_SIDE,
  IdxPriceInfo,
} from "@d8x/perpetuals-sdk";
import { Redis } from "ioredis";
import { constructRedis, executeWithTimeout, sleep } from "../utils";
import {
  BrokerOrderMsg,
  ExecuteOrderCommand,
  ExecutionFailedMsg,
  ExecutorConfig,
  OrderBundle,
  OrderType,
  PerpetualLimitOrderCancelledMsg,
  PerpetualLimitOrderCreatedMsg,
  Position,
  TradeMsg,
  UpdateMarginAccountMsg,
  UpdateMarkPriceMsg,
} from "../types";
import {
  IPerpetualManager,
  IPerpetualOrder,
  PerpStorage,
} from "@d8x/perpetuals-sdk/dist/esm/contracts/IPerpetualManager";
import Executor from "./executor";
import { JsonRpcProvider } from "ethers";

export default class Distributor {
  // objects
  private md: MarketData;
  private redisSubClient: Redis;
  public providers: JsonRpcProvider[];

  // state
  private blockNumber = 0;
  private priceCurveUpdatedAtBlock: Map<string, number> = new Map(); // symbol => block number
  private lastRefreshTime: Map<string, number> = new Map();
  private openPositions: Map<string, Map<string, Position>> = new Map(); // symbol => (trader => Position)
  public openOrders: Map<string, Map<string, OrderBundle>> = new Map(); // symbol => (digest => order bundle)
  private brokerOrders: Map<string, Map<string, number>> = new Map(); // symbol => (digest => received ts)
  private pxSubmission: Map<string, IdxPriceInfo> = new Map(); // symbol => px submission
  private markPremium: Map<string, number> = new Map();
  private midPremium: Map<string, number> = new Map();
  private unitAccumulatedFunding: Map<string, number> = new Map();
  private tradePremium: Map<string, [number, number]> = new Map();
  // order digest => sent for execution timestamp
  private messageSentAt: Map<string, number> = new Map();
  private pricesFetchedAt: Map<string, number> = new Map();
  public ready: boolean = false;

  // static info
  private config: ExecutorConfig;
  private isQuote: Map<string, boolean> = new Map();
  private symbols: string[] = [];
  private maintenanceRate: Map<string, number> = new Map();

  // constants

  // calculated block time in milliseconds, used for delaying broker orders
  // execution
  public blockTimeMS = 0;
  // Last block timestamp for blockTimeMS measurement
  public lastBlockTs = 0;
  // blockTimeMS * brokerOrderBlockSizeDelayFactor is the delay in milliseconds
  // for broker orders, ideally below 1
  public brokerOrderBlockSizeDelayFactor = 0.5;

  // publish times must be within 10 seconds of each other, or submission will fail on-chain
  private MAX_OUTOFSYNC_SECONDS: number = 10;

  // Last time when refreshAllOpenOrders was called
  private lastRefreshOfAllOpenOrders: Date = new Date();

  constructor(config: ExecutorConfig, private executor: Executor) {
    this.config = config;
    const sdkConfig = PerpetualDataHandler.readSDKConfig(config.sdkConfig);
    if (config.configSource !== undefined) {
      sdkConfig.configSource = config.configSource;
    }
    this.redisSubClient = constructRedis("commanderSubClient");
    this.providers = this.config.rpcWatch.map(
      (url) => new JsonRpcProvider(url)
    );
    this.md = new MarketData(sdkConfig);
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
    console.log(JSON.stringify(info, undefined, "  "));

    this.symbols = info.pools
      .filter(({ isRunning }) => isRunning)
      .map((pool) =>
        pool.perpetuals
          .filter(({ state }) => state === "NORMAL")
          .map(
            (perpetual) =>
              `${perpetual.baseCurrency}-${perpetual.quoteCurrency}-${pool.poolSymbol}`
          )
      )
      .flat();
    console.log({ symbols: this.symbols });

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
        await this.md.fetchPricesForPerpetual(symbol)
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
      this.tradePremium.set(symbol, [
        this.midPremium.get(symbol)! + 5e-4,
        this.midPremium.get(symbol)! - 5e-4,
      ]);
      // "preallocate" trader set
      this.openPositions.set(symbol, new Map());
      this.openOrders.set(symbol, new Map());
      this.brokerOrders.set(symbol, new Map());
      // dummy values
      this.lastRefreshTime.set(symbol, 0);
    }

    // Subscribe to blockchain events
    await this.redisSubClient.subscribe(
      "block",
      "UpdateMarkPriceEvent",
      "UpdateMarginAccountEvent",
      "TradeEvent",
      "ExecutionFailedEvent",
      "PerpetualLimitOrderCreatedEvent",
      "PerpetualLimitOrderCancelledEvent",
      "BrokerOrderCreatedEvent",
      "Restart",
      "switch-mode",
      "listener-error",
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

      setInterval(() => {
        for (const symbol of this.symbols) {
          if (this.openOrders.get(symbol)?.size ?? 0 > 0) {
            this.checkOrders(symbol);
          }
        }
      }, 500);

      this.redisSubClient.on("message", async (channel, msg) => {
        switch (channel) {
          case "block": {
            this.blockNumber = +msg;
            for (const symbol of this.symbols) {
              this.checkOrders(symbol);
            }
            if (
              Date.now() - Math.min(...this.lastRefreshTime.values()) >
              this.config.refreshOrdersIntervalSecondsMax * 1_000
            ) {
              this.refreshAllOpenOrders();
            }

            // Periodically recalculate the block time for broker order delay
            if (this.lastBlockTs == 0) {
              this.lastBlockTs = Date.now();
            } else {
              this.blockTimeMS = Date.now() - this.lastBlockTs;
              this.lastBlockTs = Date.now();
            }
            break;
          }

          case "UpdateMarginAccountEvent": {
            const { traderAddr, perpetualId }: UpdateMarginAccountMsg =
              JSON.parse(msg);
            const account = await (
              this.md.getReadOnlyProxyInstance() as unknown as IPerpetualManager
            ).getMarginAccount(perpetualId, traderAddr);
            this.updatePosition({
              address: traderAddr,
              perpetualId: perpetualId,
              positionBC: ABK64x64ToFloat(account.fPositionBC),
              cashCC: ABK64x64ToFloat(account.fCashCC),
              lockedInQC: ABK64x64ToFloat(account.fLockedInValueQC),
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

          case "PerpetualLimitOrderCreatedEvent": {
            const {
              symbol,
              digest,
              trader,
              order,
            }: PerpetualLimitOrderCreatedMsg = JSON.parse(msg);
            this.addOrder(
              symbol,
              trader,
              digest,
              order.type as OrderType,
              order
            );
            await this.updatePriceCurve(symbol);
            if (!this.openPositions.get(symbol)?.has(trader)) {
              // new trader, refresh
              await this.refreshAccount(symbol, trader);
            }
            this.checkOrders(symbol);
            break;
          }

          case "PerpetualLimitOrderCancelledEvent": {
            const { symbol, digest }: PerpetualLimitOrderCancelledMsg =
              JSON.parse(msg);
            this.removeOrder(symbol, digest, "removing cancelled order");
            break;
          }

          case "TradeEvent": {
            const { perpetualId, symbol, digest, trader }: TradeMsg =
              JSON.parse(msg);
            this.removeOrder(symbol, digest, "removing executed order", trader);
            const unitAccumulatedFundingCC = ABK64x64ToFloat(
              (
                await this.md
                  .getReadOnlyProxyInstance()
                  .getPerpetual(perpetualId)
              ).fUnitAccumulatedFunding
            );
            this.unitAccumulatedFunding.set(symbol, unitAccumulatedFundingCC);
            this.updatePriceCurve(symbol);
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
            const { symbol, traderAddr, digest, type }: BrokerOrderMsg =
              JSON.parse(msg);
            // introduce delay of less than 1 block for broker orders
            console.log({
              info: "delaying broker order",
              amountMS: this.blockTimeMS * this.brokerOrderBlockSizeDelayFactor,
              digest: digest,
              time: new Date(Date.now()).toISOString(),
            });
            await sleep(
              this.blockTimeMS * this.brokerOrderBlockSizeDelayFactor
            );

            this.addOrder(symbol, traderAddr, digest, type, undefined);
            this.brokerOrders.get(symbol)!.set(digest, Date.now());
            this.checkOrders(symbol);
            break;
          }

          case "listener-error":
          case "switch-mode":
            // Whenever something wrong happens on sentinel, refresh orders if
            // they were not refreshed recently in the last 30 (should be more
            // than refreshOrdersIntervalSecondsMin) seconds. Sentinel might
            // have missed events and executed orders might still be held in
            // memory in distributor.
            if (
              new Date(Date.now() - 30_000) > this.lastRefreshOfAllOpenOrders
            ) {
              console.log({
                message: "Refreshing all open orders due to sentinel error",
                time: new Date(Date.now()).toISOString(),
                lastRefreshOfAllOpenOrders:
                  this.lastRefreshOfAllOpenOrders.toISOString(),
                sentinelReason: channel,
              });
              this.refreshAllOpenOrders();
            }
            break;

          case "Restart": {
            process.exit(0);
          }
        }
      });
      await this.refreshAllOpenOrders();
    });
  }

  /**
   * Signed geometric mean of order sizes
   * @param symbol
   * @param side
   * @returns
   */
  private getOrderAverage(symbol: string, side: string) {
    const logSizes = [...this.openOrders.get(symbol)!]
      .filter(([, order]) => order.order?.side === side)
      .map(([, order]) => Math.log(order.order!.quantity)); // undefined is ok because these are fill or kill if tried
    if (logSizes.length > 0) {
      return (
        Math.exp(
          logSizes.reduce((acc, val) => acc + val, 0) / logSizes.length
        ) * (side === BUY_SIDE ? 1 : -1)
      );
    } else {
      return undefined;
    }
  }
  /**
   * Update [short prem, long prem] by computing price of average long (short) order sizes.
   * No update needed if no orders exist on that side.
   * Broker orders are only tried without knowing the order side/quantity when they are fill or kill, so can be ignored.
   * @param symbol
   */
  private async updatePriceCurve(symbol: string) {
    const blockLatency = 2;
    if (
      this.priceCurveUpdatedAtBlock.has(symbol) &&
      this.priceCurveUpdatedAtBlock.get(symbol)! >=
        this.blockNumber + blockLatency
    ) {
      // price curve already updated at most X blocks ago
      return;
    }
    this.priceCurveUpdatedAtBlock.set(symbol, this.blockNumber);
    const prem = this.tradePremium.get(symbol)!;
    for (const i of [0, 1]) {
      const side = [BUY_SIDE, SELL_SIDE][i];
      const pxS2S3 = this.pxSubmission.get(symbol)!;
      if (pxS2S3.s2MktClosed || pxS2S3.s3MktClosed) {
        // mkt is closed
        return;
      }
      const tradeSize = this.getOrderAverage(symbol, side);
      if (tradeSize) {
        // empirical
        const p = await this.md.getPerpetualPrice(symbol, tradeSize, pxS2S3);
        if (this.md.isPredictionMarket(symbol)) {
          prem[i] = p - pxS2S3.s2;
        } else {
          prem[i] = p / pxS2S3.s2 - 1;
        }
      } else {
        // default to mid premium +/- 5 bps buffer
        prem[i] =
          this.midPremium.get(symbol)! + (side === BUY_SIDE ? 1e5 : -1e5);
      }
    }
    this.tradePremium.set(symbol, prem);
  }

  private addOrder(
    symbol: string,
    trader: string,
    digest: string,
    type: OrderType,
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
        type: type,
        isPredictionMarket: this.md.isPredictionMarket(symbol),
      });
      console.log({
        info: "order added",
        symbol: symbol,
        trader: trader,
        digest: digest,
        onChain: order !== undefined,
        time: new Date(Date.now()).toISOString(),
      });
    }
  }

  private removeOrder(
    symbol: string,
    digest: string,
    reason?: string,
    trader?: string
  ) {
    this.executor.recordExecutedOrder(digest);
    if (!this.openOrders.get(symbol)?.has(digest)) {
      // nothing to remove
      return;
    }
    this.openOrders.get(symbol)?.delete(digest);
    console.log({
      info: "order removed",
      reason: reason,
      symbol: symbol,
      trader: trader,
      digest: digest,
      time: new Date(Date.now()).toISOString(),
    });
  }

  private updatePosition(position: Position) {
    const symbol = this.md.getSymbolFromPerpId(position.perpetualId)!;
    if (!this.openPositions.has(symbol)) {
      this.openPositions.set(symbol, new Map());
    }

    if (
      position.positionBC !== 0 ||
      [...(this.openOrders.get(symbol) ?? [])].findIndex(
        ([digest, orderBundle]) => orderBundle.trader === position.address
      ) >= 0
    ) {
      // trader has either an open position or open orders - track position
      this.openPositions.get(symbol)!.set(position.address, position);
    } else {
      // untrack inactive trader
      this.openPositions.get(symbol)!.delete(position.address);
    }
  }

  /**
   * Refresh open orders, in parallel over perpetuals
   */
  public async refreshAllOpenOrders() {
    this.lastRefreshOfAllOpenOrders = new Date();
    await Promise.allSettled(
      this.symbols.map((symbol) => this.refreshOpenOrders(symbol))
    );
  }

  public async refreshOpenOrders(symbol: string) {
    this.requireReady();
    if (
      Date.now() - (this.lastRefreshTime.get(symbol) ?? 0) <
      this.config.refreshOrdersIntervalSecondsMin * 1_000
    ) {
      console.log("[refreshOpenOrders] called too soon", {
        symbol: symbol,
        time: new Date(Date.now()).toISOString(),
        lastRefresh: new Date(this.lastRefreshTime.get(symbol) ?? 0),
      });
      return;
    }
    console.log(`refreshing open orders for symbol ${symbol}...`);
    const chunkSize1 = 2 ** 6; // for orders
    const rpcProviders = this.config.rpcWatch.map(
      (url) => new JsonRpcProvider(url)
    );
    let providerIdx = Math.floor(Math.random() * rpcProviders.length);
    this.lastRefreshTime.set(symbol, Date.now());

    let tsStart = Date.now();

    const numOpenOrders = Number(
      await executeWithTimeout(
        this.md
          .getOrderBookContract(symbol, rpcProviders[providerIdx])!
          .orderCount(),
        10_000
      )
    );
    console.log(`found ${numOpenOrders} open ${symbol} orders.`);

    // fetch orders
    const promises = [];
    for (let i = 0; i < numOpenOrders; i += chunkSize1) {
      const ob = this.md!.getOrderBookContract(
        symbol,
        rpcProviders[providerIdx]
      );
      promises.push(ob.pollRange(i, chunkSize1));
      providerIdx = (providerIdx + 1) % rpcProviders.length;
    }

    if (!this.openOrders.has(symbol)) {
      this.openOrders.set(symbol, new Map<string, OrderBundle>());
    }
    const orderBundles: Map<string, OrderBundle> = new Map();
    for (let i = 0; i < promises.length; i += rpcProviders.length) {
      try {
        const chunks = await executeWithTimeout(
          Promise.allSettled(promises.slice(i, i + rpcProviders.length)),
          10_000
        );
        for (const result of chunks) {
          if (result.status === "fulfilled") {
            const [orders, orderHashes, submittedTs] = result.value;
            for (let j = 0; j < orders.length; j++) {
              if (orderHashes[j] == ZERO_ORDER_ID) {
                continue;
              }
              const bundle = {
                symbol: symbol,
                trader: orders[j].traderAddr,
                digest: orderHashes[j],
                isPredictionMarket: this.md.isPredictionMarket(symbol),
                order: this.md!.smartContractOrderToOrder({
                  brokerAddr: orders[j].brokerAddr,
                  brokerFeeTbps: orders[j].brokerFeeTbps,
                  brokerSignature: orders[j].brokerSignature,
                  executionTimestamp: orders[j].executionTimestamp,
                  fAmount: orders[j].fAmount,
                  flags: orders[j].flags,
                  fLimitPrice: orders[j].fLimitPrice,
                  fTriggerPrice: orders[j].fTriggerPrice,
                  iDeadline: orders[j].iDeadline,
                  leverageTDR: orders[j].leverageTDR,
                  traderAddr: orders[j].traderAddr,
                  iPerpetualId: this.md!.getPerpIdFromSymbol(symbol),
                  executorAddr: this.config.rewardsAddress,
                  submittedTimestamp: submittedTs[j],
                } as IPerpetualOrder.OrderStruct),
                type: ORDER_TYPE_MARKET as OrderType,
              };
              bundle.type = bundle.order.type as OrderType;
              bundle.order.parentChildOrderIds = [
                orders[j].parentChildDigest1,
                orders[j].parentChildDigest2,
              ];
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
      marketOpen: orderArray.filter(
        ({ order }) => order?.type === ORDER_TYPE_MARKET && !order?.reduceOnly
      ).length,
      marketClose: orderArray.filter(
        ({ order }) => order?.type === ORDER_TYPE_MARKET && order?.reduceOnly
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
    // if (orderBundles.size > 0) {
    // found some orders, report
    console.log({
      info: "open orders",
      symbol: symbol,
      orderBook: this.md!.getOrderBookContract(symbol)!.target,
      time: new Date(Date.now()).toISOString(),
      ...numOrders,
      waited: `${Date.now() - tsStart} ms`,
    });
    // }
    // console.log(orderBundles);
    await this.updatePriceCurve(symbol);
    await this.refreshAccounts(symbol);
  }

  private async refreshAccount(symbol: string, traderAddr: string) {
    const perpId = this.md.getPerpIdFromSymbol(symbol)!;
    const proxy = this.md.getReadOnlyProxyInstance();
    const account = await proxy.getMarginAccount(perpId, traderAddr);
    const position: Position = {
      perpetualId: perpId,
      address: traderAddr,
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

  private async refreshAccounts(symbol: string) {
    console.log(`refreshing accounts for symbol ${symbol}...`);
    const chunkSize2 = 2 ** 4; // for margin accounts
    const perpId = this.md.getPerpIdFromSymbol(symbol)!;
    const proxy = this.md.getReadOnlyProxyInstance();
    const rpcProviders = this.config.rpcWatch.map(
      (url) => new JsonRpcProvider(url)
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
        target: proxy.target,
        callData: proxy.interface.encodeFunctionData("getMarginAccount", [
          perpId,
          addr,
        ]),
      }));
      promises2.push(
        multicall
          .connect(rpcProviders[providerIdx])
          .aggregate3.staticCall(calls)
      );
      addressChunks.push(addressChunk);
      providerIdx = (providerIdx + 1) % rpcProviders.length;
    }

    let tsStart = Date.now();
    for (let i = 0; i < promises2.length; i += rpcProviders.length) {
      try {
        const addressChunkBin = addressChunks.slice(i, i + rpcProviders.length);
        const accountChunk = await executeWithTimeout(
          Promise.allSettled(promises2.slice(i, i + rpcProviders.length)),
          10_000
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

  private async refreshPrices(symbol: string) {
    if (
      Date.now() - (this.pricesFetchedAt.get(symbol) ?? 0) >
      this.config.fetchPricesIntervalSecondsMin * 1_000
    ) {
      let tsStart = Date.now();
      this.pricesFetchedAt.set(symbol, tsStart);
      const newPxSubmission = await this.md.fetchPricesForPerpetual(symbol);
      this.pxSubmission.set(symbol, newPxSubmission);
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
      // console.log(`no open orders for symbol ${symbol}`);
      return;
    }

    try {
      await this.refreshPrices(symbol);
    } catch (e) {
      console.log("error fetching from price service");
      throw e;
    }

    const curPx = this.pxSubmission.get(symbol)!;
    if (curPx.s2MktClosed || curPx.s3MktClosed) {
      // console.log(`${symbol} market is closed`);
      return;
    }

    const removeOrders: string[] = [];
    for (const [digest, orderBundle] of orders) {
      const command: ExecuteOrderCommand = {
        symbol: orderBundle.symbol,
        digest: orderBundle.digest,
        trader: orderBundle.trader,
        reduceOnly: orderBundle.order?.reduceOnly,
      };
      // check if it's not too soon to send order for execution again
      if (
        Date.now() - (this.messageSentAt.get(command.digest) ?? 0) <
        this.config.executeIntervalSecondsMin * 500
      ) {
        continue;
      }

      const isExecOnChain = this.isExecutableIfOnChain(orderBundle, curPx.s2);
      if (isExecOnChain) {
        await this.sendCommand(command);
      } else {
        // console.log("Not executable:", { orderBundle, curPx });
      }
      if (
        orderBundle.order == undefined &&
        Date.now() - (this.brokerOrders.get(symbol)?.get(digest) ?? 0) > 60_000
      ) {
        removeOrders.push(orderBundle.digest);
        this.removeOrder(
          orderBundle.symbol,
          orderBundle.digest,
          "broker order expired"
        );
      }
    }
    // cleanup
    for (const digest of removeOrders) {
      this.openOrders.get(symbol)?.delete(digest);
      this.brokerOrders.get(symbol)?.delete(digest);
    }
    return;
  }

  private async sendCommand(msg: ExecuteOrderCommand) {
    // Prevent multiple executions of the same order within a short time
    if (
      Date.now() - (this.messageSentAt.get(msg.digest) ?? 0) >
      this.config.executeIntervalSecondsMin * 2000
    ) {
      if (!this.messageSentAt.has(msg.digest)) {
        console.log({
          info: "execute",
          order: msg,
          time: new Date(Date.now()).toISOString(),
        });
      }
      this.messageSentAt.set(msg.digest, Date.now());
      this.executor.ExecuteOrder(msg);
    }
  }

  /**
   * True if order can be executed if found on-chain
   * @param order Order bundle
   * @param pxS2S3 spot prices [S2, S3]
   * @returns
   */
  public isExecutableIfOnChain(order: OrderBundle, indexPrice: number) {
    // console.log(order);
    if (order.order == undefined) {
      // broker order: if it's market and on chain, it's executable, nothing to check
      return order.type == ORDER_TYPE_MARKET;
    }

    if (
      order.isPredictionMarket &&
      order.type !== ORDER_TYPE_MARKET &&
      !order.order?.reduceOnly
    ) {
      // prediction markets are spot only (non-market orders can only be closing)
      return false;
    }
    // exec ts
    if (order.order.executionTimestamp > Date.now() / 1_000) {
      // too soon
      return false;
    }

    // deadline
    if (!!order.order.deadline && order.order.deadline < Date.now() / 1_000) {
      // expired - get paid to remove it
      return true;
    }

    // dependencies must be checked before reduce-only order checks, since there
    // can be a case when a reduce-only order is dependent on another parent
    // order. If this check would be done after reduce only check, in case
    // traderPos === 0 order would still be sent off for execution which would
    // cause dpcy not fulfilled error. Note that order dependencies are also
    // checked in the executor and orders without loaded parentChildOrderIds
    // info are not executed.
    if (
      !order.order.parentChildOrderIds ||
      (order.order.parentChildOrderIds[0] == ZERO_ORDER_ID &&
        this.openOrders
          .get(order.symbol)
          ?.has(order.order.parentChildOrderIds[1]))
    ) {
      // dependency hasn't been cleared
      return false;
    }

    // reduce only
    const traderPos = this.openPositions
      .get(order.symbol)
      ?.get(order.trader)?.positionBC;
    const isLong = order.order.side === BUY_SIDE;
    if (order.order.reduceOnly) {
      if (traderPos == undefined) {
        // not enough information
        return false;
      } else if ((traderPos < 0 && !isLong) || (traderPos > 0 && isLong)) {
        return false;
      } else if (traderPos === 0 || order.order.type === ORDER_TYPE_MARKET) {
        return true;
      }
    }

    const markPrice = order.isPredictionMarket
      ? indexPrice + this.markPremium.get(order.symbol)!
      : indexPrice * (1 + this.markPremium.get(order.symbol)!);
    // const midPrice = pxS2S3[0] * (1 + this.midPremium.get(order.symbol)!);
    const limitPrice = order.order.limitPrice;
    const triggerPrice = order.order.stopPrice;

    const refSize = this.getOrderAverage(order.symbol, order.order.side);
    // scale premium by: 1 if this order is small or we have no reference, else ratio this order  size / reference size
    const scale =
      !refSize || Math.abs(refSize) > order.order.quantity
        ? 1
        : order.order.quantity / Math.abs(refSize);
    const sideIdx = [BUY_SIDE, SELL_SIDE].findIndex(
      (side) => side === order.order!.side
    );
    let tradePrice: number;
    tradePrice = order.isPredictionMarket
      ? indexPrice + this.tradePremium.get(order.symbol)![sideIdx] * scale
      : indexPrice *
        (1 + this.tradePremium.get(order.symbol)![sideIdx] * scale);

    let execute = false;

    // smart contract:
    // bool isTriggerSatisfied = _isLong
    // ? _fMarkPrice >= _fTriggerPrice
    // : _fMarkPrice <= _fTriggerPrice;

    switch (order.order.type) {
      case ORDER_TYPE_MARKET:
        execute = true;
        // console.log("mkt");
        break;

      case ORDER_TYPE_LIMIT:
        execute =
          limitPrice != undefined &&
          ((isLong && tradePrice < limitPrice) ||
            (!isLong && tradePrice > limitPrice));
        break;

      case ORDER_TYPE_STOP_MARKET:
        execute =
          triggerPrice != undefined &&
          ((isLong && markPrice > triggerPrice) ||
            (!isLong && markPrice < triggerPrice));
        break;

      case ORDER_TYPE_STOP_LIMIT:
        execute =
          triggerPrice != undefined &&
          limitPrice != undefined &&
          ((isLong && markPrice > triggerPrice) ||
            (!isLong && markPrice < triggerPrice)) &&
          ((isLong && tradePrice < limitPrice) ||
            (!isLong && tradePrice > limitPrice));
        break;

      default:
        break;
    }
    // if (execute) {
    //   console.log({
    //     side: order.order.side,
    //     indexPrice,
    //     tradePrice,
    //     limitPrice,
    //     markPrice,
    //     triggerPrice,
    //     size: order.order.quantity,
    //     refSize,
    //     scale,
    //     tradePrem: this.tradePremium.get(order.symbol)![sideIdx],
    //     timestamp: new Date(Date.now()).toISOString(),
    //   });
    // }
    return execute;
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

  public getOrder(symbol: string, digest: string) {
    return this.openOrders.get(symbol)?.get(digest);
  }

  public getOrderByDigest(digest: string): OrderBundle | undefined {
    for (const symbol of this.symbols) {
      const order = this.openOrders.get(symbol)?.get(digest);
      if (order) {
        return order;
      }
    }
    return undefined;
  }
}
