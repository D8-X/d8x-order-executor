import { MarketData, PerpetualDataHandler } from "@d8-x/d8x-node-sdk";
import { Redis } from "ioredis";
import SturdyWebSocket from "sturdy-websocket";
import Websocket from "ws";
import {
  BrokerOrderMsg,
  BrokerWSMessage,
  BrokerWSUpdateData,
  ExecutorConfig,
  PerpetualLimitOrderCreatedMsg,
} from "../types";
import { constructRedis, executeWithTimeout, flagToOrderType } from "../utils";
import { PerpetualCreatedEvent } from "@d8-x/d8x-node-sdk/contracts/IPerpetualManager";
import { JsonRpcProvider } from "ethers";

export default class BackendListener {
  private config: ExecutorConfig;
  private wsIndex: number;

  // objects
  private httpProvider: JsonRpcProvider;
  private redisPubClient: Redis;
  private md: MarketData;
  private ws: SturdyWebSocket;

  // state
  private perpIds: bigint[] = [];
  private chainId: number;
  private lastRpcIndex = { http: -1, ws: -1 };

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
    console.log(
      `${new Date(Date.now()).toISOString()} unsubscribing not implemented`
    );
  }

  public async start() {
    // infer chain from provider
    const network = await executeWithTimeout(
      this.httpProvider._detectNetwork(),
      10_000
    );
    // connect to http provider
    console.log(
      `${new Date(Date.now()).toISOString()}: Broker listener connected to ${network.name
      }, chain id ${network.chainId}, using HTTP provider`
    );
    this.chainId = Number(network.chainId);

    await this.md.createProxyInstance(this.httpProvider);
    console.log(
      `${new Date(
        Date.now()
      ).toISOString()}: http connection established with proxy @ ${this.md.getProxyAddress()}`
    );

    // get perpetuals and order books
    const info = await this.md.exchangeInfo();
    this.perpIds = info.pools
      .filter(({ isRunning }) => isRunning)
      .map((pool) =>
        pool.perpetuals
          .filter(({ state }) => state === "NORMAL")
          .map(({ id }) => BigInt(id))
      )
      .flat();

    // subscribe
    this.addListeners();

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
      console.log(
        `${new Date(Date.now()).toISOString()} Connected to broker WS`
      );
    });

    this.ws.addEventListener("close", () => {
      console.log(
        `${new Date(Date.now()).toISOString()} Disconnected from broker WS`
      );
    });

    this.perpIds.forEach((id) => {
      console.log(
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

    this.ws.addEventListener("message", (event) => {
      const msg = JSON.parse(event.data) as BrokerWSMessage;
      const perpId = msg.topic.split(":")[0];
      switch (msg.type) {
        case "subscribe":
          if (msg.data === "ack") {
            console.log(
              `${new Date(
                Date.now()
              ).toISOString()} Subscribed to perpetual id ${perpId} via broker WS ${this.config.brokerWS[this.wsIndex]
              }`
            );
          } else {
            console.log(
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
          const eventMsg: BrokerOrderMsg = {
            chainId: this.chainId,
            symbol: this.md!.getSymbolFromPerpId(+perpId)!,
            perpetualId: +perpId,
            traderAddr: traderAddr,
            digest: `0x${orderId}`,
            type: flagToOrderType(BigInt(flags), BigInt(fLimitPrice)),
          };
          console.log({
            event: "BrokerOrderCreated",
            time: new Date(Date.now()).toISOString(),
            ...eventMsg,
          });
          this.redisPubClient.publish(
            "BrokerOrderCreatedEvent",
            JSON.stringify(eventMsg)
          );
        default:
          break;
      }
    });
  }
}
