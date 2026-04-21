import {
  ABK64x64ToFloat,
  BUY_SIDE,
  IdxPriceInfo,
  MarketData,
  Multicall3,
  Multicall3__factory,
  MULTICALL_ADDRESS,
  Order,
  ORDER_TYPE_LIMIT,
  ORDER_TYPE_MARKET,
  ORDER_TYPE_STOP_LIMIT,
  ORDER_TYPE_STOP_MARKET,
  OrderStatus,
  PerpetualDataHandler,
  SELL_SIDE,
  ZERO_ORDER_ID,
} from "@d8-x/d8x-node-sdk";
import type {
  IPerpetualManager,
  IPerpetualOrder,
  PerpStorage,
} from "@d8-x/d8x-node-sdk/contracts/IPerpetualManager";
import { JsonRpcProvider, Overrides, ZeroAddress } from "ethers";
import { Redis } from "ioredis";
import { MultiUrlJsonRpcProvider } from "../multiUrlJsonRpcProvider.js";
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
} from "../types.js";
import { constructRedis, executeWithTimeout } from "../utils.js";
import Executor from "./executor.js";
import { logger } from "../logger.js";

export default class Distributor {
  // objects
  private md: MarketData;
  private redisSubClient: Redis;
  public providers: MultiUrlJsonRpcProvider[];

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
  private refreshRpcIdx = 0;
  public ready: boolean = false;

  // static info
  private config: ExecutorConfig;
  private symbols: string[] = [];
  private chainId: number;

  // publish times must be within 10 seconds of each other, or submission will fail on-chain
  private MAX_OUTOFSYNC_SECONDS: number = 10;

  // Last time when refreshAllOpenOrders was called
  private lastRefreshOfAllOpenOrders: Date = new Date();

  constructor(config: ExecutorConfig, private executor: Executor) {
    this.config = config;
    const sdkConfig = PerpetualDataHandler.readSDKConfig(config.sdkConfig);
    if (config.priceFeedConfigNetwork !== undefined) {
      sdkConfig.priceFeedConfigNetwork = config.priceFeedConfigNetwork;
    }
    if (config.configSource !== undefined) {
      sdkConfig.configSource = config.configSource;
    }
    this.chainId = sdkConfig.chainId;
    this.redisSubClient = constructRedis("commanderSubClient");
    this.md = new MarketData(sdkConfig);
    this.providers = [
      new MultiUrlJsonRpcProvider(this.config.rpcWatch, this.md.network, {
        timeoutSeconds: 25,
        logErrors: true,
        logRpcSwitches: true,
        // Distributor uses free rpcs, make sure to switch on each call.
        switchRpcOnEachRequest: true,
        staticNetwork: true,
      }),
    ];
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
      logger.info(
        `${new Date(
          Date.now()
        ).toISOString()}: all rpcs are down ${this.config.rpcWatch.join(", ")}`
      );
    }

    const info = await this.md.exchangeInfo();
    logger.info(JSON.stringify(info, undefined, "  "));

    const symbols = info.pools
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
    logger.info({ symbols });

    for (const symbol of symbols) {
      try {
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
        this.symbols.push(symbol);
      } catch (e) {
        // symbol is ignored if cannot fetch data about it
        logger.info(`Could not fetch data for symbol ${symbol}`);
      }
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
          logger.info(
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

      setInterval(async () => {
        for (const symbol of this.symbols) {
          if (this.openOrders.get(symbol)?.size ?? 0 > 0) {
            await this.checkOrders(symbol);
          }
        }
      }, 500);

      this.redisSubClient.on("message", async (channel, msg) => {
        switch (channel) {
          case "block": {
            this.blockNumber = +msg;
            for (const symbol of this.symbols) {
              await this.checkOrders(symbol);
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
            const { chainId, traderAddr, perpetualId }: UpdateMarginAccountMsg =
              JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
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
            const {
              chainId,
              symbol,
              markPremium,
              midPremium,
            }: UpdateMarkPriceMsg = JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
            this.markPremium.set(symbol, markPremium);
            this.midPremium.set(symbol, midPremium);
            break;
          }

          case "PerpetualLimitOrderCreatedEvent": {
            const {
              chainId,
              symbol,
              digest,
              trader,
              order,
            }: PerpetualLimitOrderCreatedMsg = JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
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
            await this.checkOrders(symbol);
            break;
          }

          case "PerpetualLimitOrderCancelledEvent": {
            const { chainId, symbol, digest }: PerpetualLimitOrderCancelledMsg =
              JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
            this.removeOrder(symbol, digest, "removing cancelled order");
            break;
          }

          case "TradeEvent": {
            const { chainId, perpetualId, symbol, digest, trader }: TradeMsg =
              JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
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
            const {
              chainId,
              symbol,
              digest,
              trader,
              reason,
            }: ExecutionFailedMsg = JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
            if (reason != "cancel delay required") {
              this.removeOrder(symbol, digest, reason, trader);
            }
            break;
          }

          case "BrokerOrderCreatedEvent": {
            const {
              chainId,
              symbol,
              traderAddr,
              digest,
              type,
            }: BrokerOrderMsg = JSON.parse(msg);
            if (chainId !== this.chainId) {
              break;
            }
            this.addOrder(symbol, traderAddr, digest, type, undefined);
            this.brokerOrders.get(symbol)!.set(digest, Date.now());
            setTimeout(() => {
              this.upgradeBrokerStubFromChain(symbol, traderAddr, digest);
            }, 3_000);
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
              logger.info({
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
            logger.info("Restarting upong signal received...");
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
          prem[i] = p / pxS2S3.s2 - 1; // + 10 * (side === BUY_SIDE ? 1e-4 : -1e-4);
        }
      } else {
        // default to mid premium +/- 1 pct buffer
        prem[i] = this.midPremium.get(symbol)!; // + 100 * (side === BUY_SIDE ? 1e-4 : -1e-4);
      }
    }
    this.tradePremium.set(symbol, prem);
  }

  private async upgradeBrokerStubFromChain(
    symbol: string,
    trader: string,
    digest: string
  ) {
    const bundle = this.openOrders.get(symbol)?.get(digest);
    if (!bundle || bundle.order !== undefined) {
      return;
    }
    try {
      const status = await this.md.getOrderStatus(symbol, digest);
      if (status !== OrderStatus.OPEN) {
        return;
      }
      const provider =
        this.providers[Math.floor(Math.random() * this.providers.length)];
      const ob = this.md.getOrderBookContract(symbol, provider);
      const [scOrder, deps] = await Promise.all([
        ob.orderOfDigest(digest),
        ob.orderDependency(digest),
      ]);
      if (scOrder.traderAddr === ZeroAddress) {
        return;
      }
      const order = this.md.smartContractOrderToOrder(
        scOrder as unknown as IPerpetualOrder.OrderStruct
      );
      order.parentChildOrderIds = [deps[0], deps[1]];
      if (this.openOrders.get(symbol)?.get(digest)?.order !== undefined) {
        return;
      }
      this.addOrder(symbol, trader, digest, order.type as OrderType, order);
      logger.info({
        info: "broker stub upgraded via chain fallback",
        symbol,
        digest,
        time: new Date(Date.now()).toISOString(),
      });
      await this.checkOrders(symbol);
    } catch (e) {
      logger.info({
        info: "upgradeBrokerStubFromChain failed",
        symbol,
        digest,
        error: (e as Error)?.message ?? e,
      });
    }
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
      logger.info({
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
    logger.info({
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
    // in serial to avoid rate limits
    for (const symbol of this.symbols) {
      await this.refreshOpenOrders(symbol);
    }
  }

  public async refreshOpenOrders(symbol: string) {
    this.requireReady();
    if (
      Date.now() - (this.lastRefreshTime.get(symbol) ?? 0) <
      this.config.refreshOrdersIntervalSecondsMin * 1_000
    ) {
      logger.info({
        symbol: symbol,
        orders: this.openOrders.get(symbol)?.size,
        time: new Date(Date.now()).toISOString(),
        nextRefresh: new Date(
          (this.lastRefreshTime.get(symbol) ?? 0) +
          this.config.refreshOrdersIntervalSecondsMin * 1_000
        ),
      });
      return;
    }
    logger.info(`refreshing open orders for symbol ${symbol}...`);
    this.lastRefreshTime.set(symbol, Date.now());
    const tsStart = Date.now();
    const isPred = this.md.isPredictionMarket(symbol);

    const orderBundles: Map<string, OrderBundle> = new Map();
    const rpcUrls = this.config.rpcWatch;
    const rpcURL = rpcUrls[this.refreshRpcIdx % rpcUrls.length];
    this.refreshRpcIdx = (this.refreshRpcIdx + 1) % rpcUrls.length;
    const overrides: Overrides & { rpcURL: string } = { rpcURL };
    try {
      const [orders, digests, traders] = await executeWithTimeout(
        this.md.getAllOpenOrders(symbol, overrides),
        10_000
      );
      for (let i = 0; i < orders.length; i++) {
        const digest = digests[i];
        if (!digest || digest == ZERO_ORDER_ID) continue;
        orderBundles.set(digest, {
          symbol,
          trader: traders[i],
          digest,
          isPredictionMarket: isPred,
          order: orders[i],
          type: orders[i].type as OrderType,
        });
      }
    } catch (e) {
      logger.info(
        `${symbol} ${new Date(Date.now()).toISOString()}: error refreshing open orders`,
        e
      );
    }

    logger.info(`found ${orderBundles.size} open ${symbol} orders.`);
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

    logger.info({
      info: "open orders",
      symbol: symbol,
      orderBook: this.md!.getOrderBookContract(symbol)!.target,
      time: new Date(Date.now()).toISOString(),
      ...numOrders,
      waited: `${Date.now() - tsStart} ms`,
    });

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
    logger.info(`refreshing accounts for symbol ${symbol}...`);
    const chunkSize2 = 2 ** 4; // for margin accounts
    const perpId = this.md.getPerpIdFromSymbol(symbol)!;
    const proxy = this.md.getReadOnlyProxyInstance();
    const rpcProviders = this.config.rpcWatch.map(
      (url) => new JsonRpcProvider(url, undefined, { staticNetwork: true })
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
        logger.info("Error fetching account chunk (RPC?)");
      }
    }
    if (this.openPositions.get(symbol)!.size > 0) {
      // found something, report
      logger.info({
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
      return;
    }

    try {
      await this.refreshPrices(symbol);
    } catch (e) {
      logger.info("error fetching from price service");
      throw e;
    }

    const curPx = this.pxSubmission.get(symbol)!;
    if (curPx.s2MktClosed || curPx.s3MktClosed) {
      logger.info(`${symbol} market is closed`);
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

      if (this.isExecutableIfOnChain(orderBundle, curPx.s2)) {
        await this.sendCommand(command);
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
        logger.info({
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
    // broker order need to follow the main orders checking path too
    if (order.order == undefined) {
      return false;
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

    const delay = this.config.orderDelaySec ?? 0;
    if (
      delay > 0 &&
      order.order.submittedTimestamp !== undefined &&
      order.order.submittedTimestamp + delay > Date.now() / 1_000
    ) {
      return false;
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
