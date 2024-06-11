import {
  ABK64x64ToFloat,
  IPerpetualManager__factory,
  LimitOrderBook__factory,
  MarketData,
  PerpetualDataHandler,
} from "@d8x/perpetuals-sdk";
import { BigNumber } from "@ethersproject/bignumber";
import {
  Network,
  StaticJsonRpcProvider,
  WebSocketProvider,
  Log,
} from "@ethersproject/providers";
import { Redis } from "ioredis";
import SturdyWebSocket from "sturdy-websocket";
import Websocket from "ws";
import {
  ExecutionFailedMsg,
  ExecutorConfig,
  LiquidateMsg,
  PerpetualLimitOrderCancelledMsg,
  PerpetualLimitOrderCreatedMsg,
  TradeMsg,
  UpdateMarginAccountMsg,
  UpdateMarkPriceMsg,
} from "../types";
import { constructRedis, executeWithTimeout, sleep } from "../utils";
import {
  IPerpetualOrder,
  LiquidateEvent,
  PerpetualLimitOrderCancelledEvent,
  TradeEvent,
  TransferAddressToEvent,
  UpdateMarginAccountEvent,
  UpdateMarkPriceEvent,
} from "@d8x/perpetuals-sdk/dist/esm/contracts/IPerpetualManager";
import {
  ExecutionFailedEventObject,
  PerpetualLimitOrderCreatedEventObject,
} from "@d8x/perpetuals-sdk/dist/esm/contracts/LimitOrderBook";

enum ListeningMode {
  Polling = "Polling",
  Events = "Events",
}

export default class BlockhainListener {
  private config: ExecutorConfig;
  private network: Network;

  // objects
  private httpProvider: StaticJsonRpcProvider;
  private listeningProvider:
    | StaticJsonRpcProvider
    | WebSocketProvider
    | undefined;
  private redisPubClient: Redis;
  private md: MarketData;

  // state
  private blockNumber: number | undefined;
  private mode: ListeningMode = ListeningMode.Events;
  private lastBlockReceivedAt: number;
  private lastRpcIndex = { http: -1, ws: -1 };

  constructor(config: ExecutorConfig) {
    this.config = config;
    this.md = new MarketData(
      PerpetualDataHandler.readSDKConfig(this.config.sdkConfig)
    );
    this.redisPubClient = constructRedis("sentinelPubClient");
    this.httpProvider = new StaticJsonRpcProvider(this.chooseHttpRpc());
    this.lastBlockReceivedAt = Date.now();
    this.network = { name: "", chainId: 0 };
  }

  private chooseHttpRpc() {
    const idx = (this.lastRpcIndex.http + 1) % this.config.rpcListenHttp.length;
    this.lastRpcIndex.http = idx;
    return this.config.rpcListenHttp[idx];
  }

  private chooseWsRpc() {
    const idx = (this.lastRpcIndex.ws + 1) % this.config.rpcListenWs.length;
    this.lastRpcIndex.ws = idx;
    return this.config.rpcListenWs[idx];
  }

  public unsubscribe() {
    console.log(
      `${new Date(Date.now()).toISOString()} BlockchainListener: unsubscribing`
    );
    if (this.listeningProvider) {
      this.listeningProvider.removeAllListeners();
    }
  }

  public checkHeartbeat() {
    const blockTime = Math.floor(
      (Date.now() - this.lastBlockReceivedAt) / 1_000
    );
    if (blockTime > this.config.waitForBlockSeconds) {
      console.log(
        `${new Date(
          Date.now()
        ).toISOString()}: websocket connection block time is ${blockTime} - disconnecting`
      );
      return false;
    }
    console.log(
      `${new Date(
        Date.now()
      ).toISOString()}: websocket connection block time is ${blockTime} seconds @ block ${
        this.blockNumber
      }`
    );
    return true;
  }

  private async switchListeningMode() {
    this.unsubscribe();
    this.blockNumber = undefined;
    this.listeningProvider?.removeAllListeners();
    if (this.mode == ListeningMode.Events) {
      console.log(
        `${new Date(Date.now()).toISOString()}: switching from WS to HTTP`
      );
      this.mode = ListeningMode.Polling;
      this.listeningProvider = new StaticJsonRpcProvider(
        this.chooseHttpRpc(),
        this.network
      );
    } else {
      console.log(
        `${new Date(Date.now()).toISOString()}: switching from HTTP to WS`
      );
      this.mode = ListeningMode.Events;
      this.listeningProvider = new WebSocketProvider(
        this.chooseWsRpc(),
        this.network
      );
    }
    await this.addListeners();
    this.redisPubClient.publish("switch-mode", this.mode);
  }

  private async connectOrSwitch() {
    // try to connect via ws, switch to http on failure
    this.blockNumber = undefined;
    setTimeout(async () => {
      if (!this.blockNumber) {
        console.log(
          `${new Date(
            Date.now()
          ).toISOString()}: websocket connection could not be established`
        );
        await this.switchListeningMode();
      }
    }, this.config.waitForBlockSeconds * 1_000);
    await sleep(this.config.waitForBlockSeconds * 1_000);
  }

  private resetHealthChecks() {
    // periodic health checks
    setInterval(async () => {
      if (this.mode == ListeningMode.Events) {
        // currently on WS - check that block time is still reasonable or if we need to switch
        if (!this.checkHeartbeat()) {
          this.switchListeningMode();
        }
      } else {
        // currently on HTTP - check if we can switch to WS by seeing if we get blocks
        let success = false;
        let wsProvider = new WebSocketProvider(
          new SturdyWebSocket(this.chooseWsRpc(), {
            wsConstructor: Websocket,
          }),
          this.network
        );
        wsProvider.once("block", () => {
          success = true;
        });
        // after N seconds, check if we received a block - if yes, switch
        setTimeout(async () => {
          if (success) {
            await this.switchListeningMode();
          }
        }, this.config.waitForBlockSeconds * 1_000);
      }
    }, this.config.healthCheckSeconds * 1_000);
  }

  public async start() {
    // http rpc
    this.network = await executeWithTimeout(
      this.httpProvider.ready,
      10_000,
      "could not establish http connection"
    );
    await this.md.createProxyInstance(this.httpProvider);
    console.log(
      `${new Date(Date.now()).toISOString()}: connected to ${
        this.network.name
      }, chain id ${this.network.chainId}, using HTTP provider`
    );
    // ws rpc
    if (this.config.rpcListenWs.length > 0) {
      this.listeningProvider = new WebSocketProvider(
        new SturdyWebSocket(this.chooseWsRpc(), {
          wsConstructor: Websocket,
        }),
        this.network
      );
    } else if (this.config.rpcListenHttp.length > 0) {
      this.listeningProvider = new StaticJsonRpcProvider(this.chooseHttpRpc());
    } else {
      throw new Error(
        "Please specify RPC URLs for listening to blockchain events"
      );
    }

    this.connectOrSwitch();
    this.addListeners();
    this.resetHealthChecks();
  }

  private async addListeners() {
    if (this.listeningProvider === undefined) {
      throw new Error("No provider ready to listen.");
    }
    // on error terminate
    this.listeningProvider.on("error", (e) => {
      console.log(
        `${new Date(
          Date.now()
        ).toISOString()} BlockchainListener received error msg in ${
          this.mode
        } mode:`,
        e
      );
      this.unsubscribe();
      this.switchListeningMode();
    });

    this.listeningProvider.on("block", (blockNumber) => {
      this.lastBlockReceivedAt = Date.now();
      this.redisPubClient.publish("block", blockNumber.toString());
      this.blockNumber = blockNumber;
    });

    const proxy = IPerpetualManager__factory.connect(
      this.md.getProxyAddress(),
      this.listeningProvider
    );

    proxy.on(
      "Liquidate",
      (
        perpetualId: number,
        liquidator: string,
        trader: string,
        amountLiquidatedBC: BigNumber,
        liquidationPrice: BigNumber,
        newPositionSizeBC: BigNumber,
        fFeeCC: BigNumber,
        fPnlCC: BigNumber,
        event: LiquidateEvent
      ) => {
        const symbol = this.md.getSymbolFromPerpId(perpetualId)!;
        const msg: LiquidateMsg = {
          perpetualId: perpetualId,
          symbol: symbol,
          traderAddr: trader,
          tradeAmount: ABK64x64ToFloat(amountLiquidatedBC),
          liquidator: liquidator,
          pnl: ABK64x64ToFloat(fPnlCC),
          fee: ABK64x64ToFloat(fFeeCC),
          newPositionSizeBC: ABK64x64ToFloat(newPositionSizeBC),
          block: event.blockNumber,
          hash: event.transactionHash,
          id: `${event.transactionHash}:${event.logIndex}`,
        };
        this.redisPubClient.publish("LiquidateEvent", JSON.stringify(msg));
      }
    );

    proxy.on(
      "UpdateMarginAccount",
      (
        perpetualId: number,
        trader: string,
        fFundingPaymentCC: BigNumber,
        event: UpdateMarginAccountEvent
      ) => {
        const symbol = this.md.getSymbolFromPerpId(perpetualId)!;
        const msg: UpdateMarginAccountMsg = {
          perpetualId: perpetualId,
          symbol: symbol,
          traderAddr: trader,
          fundingPaymentCC: ABK64x64ToFloat(fFundingPaymentCC),
          block: event.blockNumber,
          hash: event.transactionHash,
          id: `${event.transactionHash}:${event.logIndex}`,
        };
        this.redisPubClient.publish(
          "UpdateMarginAccountEvent",
          JSON.stringify(msg)
        );
      }
    );

    proxy.on(
      "UpdateMarkPrice",
      (
        perpetualId: number,
        fMidPricePremium: BigNumber,
        fMarkPricePremium: BigNumber,
        fSpotIndexPrice: BigNumber,
        event: UpdateMarkPriceEvent
      ) => {
        const symbol = this.md.getSymbolFromPerpId(perpetualId)!;
        const msg: UpdateMarkPriceMsg = {
          perpetualId: perpetualId,
          symbol: symbol,
          midPremium: ABK64x64ToFloat(fMidPricePremium),
          markPremium: ABK64x64ToFloat(fMarkPricePremium),
          spotIndexPrice: ABK64x64ToFloat(fSpotIndexPrice),
          block: event.blockNumber,
          hash: event.transactionHash,
          id: `${event.transactionHash}:${event.logIndex}`,
        };
        this.redisPubClient.publish(
          "UpdateMarkPriceEvent",
          JSON.stringify(msg)
        );
      }
    );

    proxy.on(
      "Trade",
      (
        perpetualId: number,
        trader: string,
        scOrder: IPerpetualOrder.OrderStructOutput,
        orderDigest: string,
        newPositionSizeBC: BigNumber,
        price: BigNumber,
        fFeeCC: BigNumber,
        fPnlCC: BigNumber,
        fB2C: BigNumber,
        event: TradeEvent
      ) => {
        const order = this.md!.smartContractOrderToOrder(scOrder);
        const msg: TradeMsg = {
          perpetualId: perpetualId,
          trader: trader,
          digest: orderDigest,
          ...order,
          brokerAddr: scOrder.brokerAddr,
          executor: scOrder.executorAddr,
          block: event.blockNumber,
          hash: event.transactionHash,
          id: `${event.transactionHash}:${event.logIndex}`,
        };
        console.log({
          event: "Trade",
          time: new Date(Date.now()).toISOString(),
          mode: this.mode,
          ...msg,
        });
        this.redisPubClient.publish("TradeEvent", JSON.stringify(msg));
      }
    );

    proxy.on(
      "PerpetualLimitOrderCancelled",
      (
        perpetualId: number,
        digest: string,
        event: PerpetualLimitOrderCancelledEvent
      ) => {
        const symbol = this.md!.getSymbolFromPerpId(perpetualId)!;
        const msg: PerpetualLimitOrderCancelledMsg = {
          symbol: symbol,
          perpetualId: perpetualId,
          digest: digest,
          block: event.blockNumber,
          hash: event.transactionHash,
          id: `${event.transactionHash}:${event.logIndex}`,
        };
        console.log({
          event: "PerpetualLimitOrderCancelled",
          time: new Date(Date.now()).toISOString(),
          mode: this.mode,
          ...msg,
        });
        this.redisPubClient.publish(
          "PerpetualLimitOrderCancelledEvent",
          JSON.stringify(msg)
        );
      }
    );

    proxy.on(
      "TransferAddressTo",
      (
        module: string,
        oldAddress: string,
        newAddress: string,
        event: TransferAddressToEvent
      ) => {
        this.redisPubClient.publish("Restart", module);
        process.exit(0);
      }
    );

    const iOrderBook = LimitOrderBook__factory.createInterface();

    this.listeningProvider.on(
      [
        iOrderBook.encodeFilterTopics("ExecutionFailed", [])[0],
        iOrderBook.encodeFilterTopics("PerpetualLimitOrderCreated", [])[0],
      ],
      async (event: Log) => {
        const parsedEvent = iOrderBook.parseLog(event);
        let msg: ExecutionFailedMsg | PerpetualLimitOrderCreatedMsg | undefined;
        // handle different events
        switch (parsedEvent.name) {
          case "ExecutionFailed":
            {
              const { perpetualId, trader, digest, reason } =
                parsedEvent.args as [number, string, string, string] &
                  ExecutionFailedEventObject;
              const symbol = this.md.getSymbolFromPerpId(perpetualId)!;
              msg = {
                perpetualId: perpetualId,
                symbol: symbol,
                trader: trader,
                digest: digest,
                reason: reason,
                block: event.blockNumber,
                hash: event.transactionHash,
                id: `${event.transactionHash}:${event.logIndex}`,
              };
            }
            break;
          case "PerpetualLimitOrderCreated":
            {
              const { perpetualId, trader, brokerAddr, order, digest } =
                parsedEvent.args as [
                  number,
                  string,
                  string,
                  IPerpetualOrder.OrderStructOutput,
                  string
                ] &
                  PerpetualLimitOrderCreatedEventObject;
              msg = {
                symbol: this.md!.getSymbolFromPerpId(perpetualId)!,
                perpetualId: perpetualId,
                trader: trader,
                brokerAddr: brokerAddr,
                order: this.md!.smartContractOrderToOrder(order),
                digest: digest,
                block: event.blockNumber,
                hash: event.transactionHash,
                id: `${event.transactionHash}:${event.logIndex}`,
              };
              // Include parent/child order dependency ids. Order dependency is
              // not included in PerpetualLimitOrderCreated event but is needed
              // for the dependency checks in distributor and executor.
              const orderDependency = await this.getOrderDependenciesHttp(
                event.address,
                digest
              );
              if (orderDependency) {
                const [id1, id2] = [orderDependency[0], orderDependency[1]];
                msg.order.parentChildOrderIds = [id1, id2];
              }
            }
            break;

          default:
            break;
        }
        // notify
        console.log({
          event: parsedEvent.name,
          time: new Date(Date.now()).toISOString(),
          mode: this.mode,
          ...msg,
        });
        this.redisPubClient.publish(
          `${parsedEvent.name}Event`,
          JSON.stringify(msg)
        );
      }
    );
  }

  // Retrieves order dependencies with a http provider instead of a websocket
  // one.
  public async getOrderDependenciesHttp(
    obAddress: string,
    orderDigest: string
  ): Promise<[string, string]> {
    const ob = LimitOrderBook__factory.connect(obAddress, this.httpProvider);
    return await ob.orderDependency(orderDigest);
  }
}
