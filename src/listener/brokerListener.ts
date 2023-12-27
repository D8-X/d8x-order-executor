import { MarketData, PerpetualDataHandler } from "@d8x/perpetuals-sdk";
import { ethers } from "ethers";
import { Redis } from "ioredis";
import SturdyWebSocket from "sturdy-websocket";
import Websocket from "ws";
import {
  BrokerListenerConfig,
  BrokerWSMessage,
  BrokerWSUpdateData,
} from "../types";
import { chooseRPC, constructRedis, executeWithTimeout } from "../utils";

enum ListeningMode {
  Polling = "Polling",
  Events = "Events",
}

export default class BackendListener {
  private config: BrokerListenerConfig;
  private wsIndex: number;

  // objects
  private httpProvider: ethers.providers.StaticJsonRpcProvider;
  private redisPubClient: Redis;
  private md: MarketData;
  private ws: SturdyWebSocket;

  // state
  private perpIds: number[] = [];
  private chainId: number | undefined = undefined;

  constructor(config: BrokerListenerConfig, wsIndex: number) {
    this.config = config;
    this.wsIndex = wsIndex;
    this.md = new MarketData(
      PerpetualDataHandler.readSDKConfig(this.config.sdkConfig)
    );
    this.redisPubClient = constructRedis("BlockchainListener");
    this.httpProvider = new ethers.providers.StaticJsonRpcProvider(
      chooseRPC(this.config.httpRPC)
    );
    this.ws = new SturdyWebSocket(this.config.brokerWS[wsIndex], {
      wsConstructor: Websocket,
    });
  }

  public unsubscribe() {
    console.log(
      `${new Date(Date.now()).toISOString()} unsubscribing not implemented`
    );
  }

  public async start() {
    // infer chain from provider
    const network = await executeWithTimeout(this.httpProvider.ready, 10_000);
    // connect to http provider
    console.log(
      `${new Date(Date.now()).toISOString()}: Broker listener connected to ${
        network.name
      }, chain id ${network.chainId}, using HTTP provider`
    );
    this.chainId = network.chainId;

    await this.md.createProxyInstance(this.httpProvider);
    console.log(
      `${new Date(
        Date.now()
      ).toISOString()}: http connection established with proxy @ ${this.md.getProxyAddress()}`
    );

    // get perpetuals and order books
    this.perpIds = (
      await this.md.getReadOnlyProxyInstance().getPoolStaticInfo(1, 255)
    )[0].flat();

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
    }, this.config.brokerReconnectIntervalMS);
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
        ).toISOString()} Subscribing to perpetual id ${id} via broker WS on chain ID ${
          this.chainId
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
              ).toISOString()} Subscribed to perpetual id ${perpId} via broker WS`
            );
          } else {
            console.log(
              `${new Date(
                Date.now()
              ).toISOString()} Error subscribing to perpetual id ${perpId} via broker WS`
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
          this.redisPubClient.publish(
            "PerpetualLimitOrderCreated",
            JSON.stringify({
              perpetualId: perpId,
              trader: traderAddr,
              order: {
                iDeadline,
                flags,
                fAmount,
                fLimitPrice,
                fTriggerPrice,
                executionTimestamp,
                fromEvent: false,
              },
              digest: `0x${orderId}`,
            })
          );
          console.log(
            `${new Date(
              Date.now()
            ).toISOString()} Perpetual Order ${perpId}:${orderId} received via broker WS`
          );
        default:
          break;
      }
    });
  }
}
