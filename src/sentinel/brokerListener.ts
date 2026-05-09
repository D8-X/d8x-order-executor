import { MarketData, PerpetualDataHandler } from "@d8-x/d8x-node-sdk";
import { Redis } from "ioredis";
import SturdyWebSocketPkg from "sturdy-websocket";

const SturdyWebSocket =
  (SturdyWebSocketPkg as any).default ?? SturdyWebSocketPkg;

import Websocket from "ws";
import {
  BrokerOrderMsg,
  BrokerWSMessage,
  BrokerWSUpdateData,
  ExecutorConfig,
  PerpetualLimitOrderCreatedMsg,
} from "../types.js";
import { constructRedis, executeWithTimeout, flagToOrderType } from "../utils.js";
import { PerpetualCreatedEvent } from "@d8-x/d8x-node-sdk/contracts/IPerpetualManager";
import { JsonRpcProvider } from "ethers";
import { logger } from "../logger.js";

export default class BackendListener {
  private config: ExecutorConfig;
  private wsIndex: number;

  // objects
  private httpProvider: JsonRpcProvider;
  private redisPubClient: Redis;
  private md: MarketData;
  private ws: any;

  // state
  private perpIds: bigint[] = [];
  private chainId: number;
  private lastRpcIndex = { http: -1, ws: -1 };

  // Periodic refresh of MarketData's perpetualId -> symbol map. BrokerListener
  // has its own MarketData instance, so it needs its own refresh path —
  // symmetric to the one in BlockchainListener.
  private symbolCacheRefreshTimer?: NodeJS.Timeout;
  private readonly SYMBOL_CACHE_REFRESH_INTERVAL_MS = 15 * 60 * 1000;

  constructor(config: ExecutorConfig, wsIndex: number) {
    this.config = config;
    this.wsIndex = wsIndex;
    const sdkConfig = PerpetualDataHandler.readSDKConfig(this.config.sdkConfig);
    this.md = new MarketData(sdkConfig);
    this.chainId = sdkConfig.chainId;
    this.redisPubClient = constructRedis("BlockchainListener");
    this.httpProvider = new JsonRpcProvider(
      this.chooseHttpRpc(),
      this.md.network,
      { staticNetwork: true }
    );
    this.ws = new SturdyWebSocket(this.config.brokerWS[wsIndex], {
      wsConstructor: Websocket,
    });
  }

  private chooseHttpRpc() {
    const idx = (this.lastRpcIndex.http + 1) % this.config.rpcListenHttp.length;
    this.lastRpcIndex.http = idx;
    return this.config.rpcListenHttp[idx];
  }

  public unsubscribe() {
    logger.debug(
      `${new Date(Date.now()).toISOString()} unsubscribing not implemented`
    );
  }

  private async refreshSymbolMap(reason: string): Promise<void> {
    try {
      await this.md.refreshSymbols(true);
      logger.info({ info: "broker symbol cache refreshed", reason });
    } catch (e) {
      logger.warn({
        info: "broker symbol cache refresh failed",
        reason,
        err: (e as Error)?.message ?? String(e),
      });
    }
  }

  public async start() {
    // infer chain from provider
    const network = await executeWithTimeout(
      this.httpProvider._detectNetwork(),
      10_000
    );
    // connect to http provider
    logger.info(
      `${new Date(Date.now()).toISOString()}: Broker listener connected to ${network.name
      }, chain id ${network.chainId}, using HTTP provider`
    );
    this.chainId = Number(network.chainId);

    await this.md.createProxyInstance(this.httpProvider);
    logger.info(
      `${new Date(
        Date.now()
      ).toISOString()}: http connection established with proxy @ ${this.md.getProxyAddress()}`
    );

    // Subscribe to ALL deployed perpetuals (not just NORMAL). Subscriptions
    // for non-NORMAL perps sit idle on the broker side until the perp
    // transitions to NORMAL — no resubscribe needed when state changes.
    // Avoids the gap where perps that were INVALID/EMERGENCY/MARKET_CLOSED at
    // startup never got a broker WS subscription and only reached the
    // executor via the slower chain path.
    const info = await this.md.exchangeInfo();
    this.perpIds = info.pools
      .filter(({ isRunning }) => isRunning)
      .map((pool) => pool.perpetuals.map(({ id }) => BigInt(id)))
      .flat();

    // subscribe
    this.addListeners();

    // Refresh perpetualId -> symbol cache periodically so events for a
    // rotated slot get labeled with the current symbol. SDK has internal
    // mutex (refreshPromise) so concurrent refreshes coalesce.
    this.symbolCacheRefreshTimer = setInterval(
      () => void this.refreshSymbolMap("periodic"),
      this.SYMBOL_CACHE_REFRESH_INTERVAL_MS
    );

    // reconnect
    setInterval(() => {
      if (!this.ws.OPEN && !this.ws.CLOSING && !this.ws.CONNECTING) {
        this.ws = new SturdyWebSocket(this.config.brokerWS[this.wsIndex], {
          wsConstructor: Websocket,
        });
        this.addListeners();
      }
    }, this.config.brokerReconnectIntervalMaxSeconds * 1_000);
  }

  private addListeners() {
    this.ws.addEventListener("open", () => {
      logger.info(
        `${new Date(Date.now()).toISOString()} Connected to broker WS`
      );
    });

    this.ws.addEventListener("close", () => {
      logger.warn(
        `${new Date(Date.now()).toISOString()} Disconnected from broker WS`
      );
    });

    this.perpIds.forEach((id) => {
      logger.debug(
        `${new Date(
          Date.now()
        ).toISOString()} Subscribing to perpetual id ${id} via broker WS ${this.config.brokerWS[this.wsIndex]
        }`
      );
      this.ws.send(
        JSON.stringify({
          type: "subscribe",
          topic: `${id}:${this.chainId}`,
        })
      );
    });

    this.ws.addEventListener("message", (event: any) => {
      const msg = JSON.parse(event.data) as BrokerWSMessage;
      const perpId = msg.topic.split(":")[0];
      switch (msg.type) {
        case "subscribe":
          if (msg.data === "ack") {
            logger.debug(
              `${new Date(
                Date.now()
              ).toISOString()} Subscribed to perpetual id ${perpId} via broker WS ${this.config.brokerWS[this.wsIndex]
              }`
            );
          } else {
            logger.warn(
              `${new Date(
                Date.now()
              ).toISOString()} Error subscribing to perpetual id ${perpId} on broker WS ${this.config.brokerWS[this.wsIndex]
              }`
            );
          }
          break;
        case "update":
          const {
            iDeadline,
            traderAddr,
            flags,
            fAmount,
            fLimitPrice,
            fTriggerPrice,
            executionTimestamp,
            orderId,
          } = msg.data as BrokerWSUpdateData;
          const symbol = this.md.getSymbolFromPerpId(+perpId);
          if (!symbol) {
            // MD doesn't know this perpId yet (e.g. brand-new slot deployed
            // after our last refresh). Drop the event rather than publish
            // {symbol: undefined}; the periodic refresh will pick it up.
            logger.warn({
              info: "broker WS update for unknown perpId",
              perpId,
              topic: msg.topic,
            });
            break;
          }
          const eventMsg: BrokerOrderMsg = {
            chainId: this.chainId,
            symbol,
            perpetualId: +perpId,
            traderAddr,
            digest: `0x${orderId}`,
            type: flagToOrderType(BigInt(flags), BigInt(fLimitPrice)),
            fAmount,
            fLimitPrice,
            fTriggerPrice,
            iDeadline,
            flags,
            executionTimestamp,
          };
          logger.debug({
            event: "BrokerOrderCreated",
            time: new Date(Date.now()).toISOString(),
            ...eventMsg,
          });
          this.redisPubClient.publish(
            "BrokerOrderCreatedEvent",
            JSON.stringify(eventMsg)
          );
          break;
        default:
          break;
      }
    });
  }
}
