import {
  ABK64x64ToFloat,
  IPerpetualManager__factory,
  LimitOrderBook__factory,
  MarketData,
  PerpetualDataHandler,
  ZERO_ORDER_ID,
} from "@d8x/perpetuals-sdk";
import { Redis } from "ioredis";
import SturdyWebSocket from "sturdy-websocket";
import Websocket from "ws";
import {
  ExecutionFailedMsg,
  ExecutorConfig,
  LiquidateMsg,
  PerpetualLimitOrderCancelledMsg,
  PerpetualLimitOrderCreatedMsg,
  RedisMsg,
  TradeMsg,
  UpdateMarginAccountMsg,
  UpdateMarkPriceMsg,
} from "../types";
import { constructRedis, executeWithTimeout, sleep } from "../utils";
import {
  JsonRpcProvider,
  Log,
  LogDescription,
  Result,
  WebSocketProvider,
} from "ethers";
import {
  IPerpetualManagerInterface,
  IPerpetualOrder,
  LiquidateEvent,
  PerpetualLimitOrderCancelledEvent,
  TradeEvent,
  UpdateMarginAccountEvent,
  UpdateMarkPriceEvent,
} from "@d8x/perpetuals-sdk/dist/esm/contracts/IPerpetualManager";
import {
  ExecutionFailedEvent,
  LimitOrderBookInterface,
  PerpetualLimitOrderCreatedEvent,
} from "@d8x/perpetuals-sdk/dist/esm/contracts/LimitOrderBook";

enum ListeningMode {
  Polling = "HTTP",
  Events = "Websocket",
}

export default class BlockhainListener {
  private config: ExecutorConfig;
  private orderBooks: Set<string> = new Set();

  // objects
  private httpProvider: JsonRpcProvider;
  private listeningProvider: JsonRpcProvider | WebSocketProvider | undefined;
  private redisPubClient: Redis;
  private md: MarketData;
  private proxyInterface: IPerpetualManagerInterface;
  private orderBookInterface: LimitOrderBookInterface;

  // state
  private blockNumber: number | undefined;
  private mode: ListeningMode = ListeningMode.Events;
  private lastBlockReceivedAt: number;
  private lastRpcIndex = { http: -1, ws: -1 };

  constructor(config: ExecutorConfig) {
    this.config = config;
    const sdkConfig = PerpetualDataHandler.readSDKConfig(this.config.sdkConfig);
    // Chain id supplied from env. For testing purposes (hardhat network)
    if (process.env.CHAIN_ID !== undefined) {
      sdkConfig.chainId = parseInt(process.env.CHAIN_ID);
    }
    this.md = new MarketData(sdkConfig);
    this.redisPubClient = constructRedis("sentinelPubClient");
    this.httpProvider = new JsonRpcProvider(
      this.chooseHttpRpc(),
      this.md.network,
      { staticNetwork: true }
    );
    this.lastBlockReceivedAt = Date.now();
    this.proxyInterface = IPerpetualManager__factory.createInterface();
    this.orderBookInterface = LimitOrderBook__factory.createInterface();
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
    this.blockNumber = undefined;

    // Destroy websockets provider and close underlying connection. Do not call
    // unsubscribe for websocket provider as it will cause async eth_unsubscribe
    // calls while underlying connection is being closed.
    //
    // Change this once ether.js is upgraded to v6
    if (this.listeningProvider instanceof WebSocketProvider) {
      this.listeningProvider.destroy();
    } else {
      this.listeningProvider?.removeAllListeners();
    }

    if (this.mode == ListeningMode.Events) {
      console.log(
        `${new Date(Date.now()).toISOString()}: switching from WS to HTTP`
      );
      this.mode = ListeningMode.Polling;
      this.listeningProvider = new JsonRpcProvider(
        this.chooseHttpRpc(),
        this.md.network,
        { staticNetwork: true }
      );
    } else {
      console.log(
        `${new Date(Date.now()).toISOString()}: switching from HTTP to WS`
      );
      this.mode = ListeningMode.Events;
      this.listeningProvider = new WebSocketProvider(
        this.chooseWsRpc(),
        this.md.network,
        { staticNetwork: true }
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
          this.md.network,
          { staticNetwork: true }
        );
        wsProvider.once("block", () => {
          success = true;
        });
        // after N seconds, check if we received a block - if yes, switch
        setTimeout(async () => {
          if (success) {
            await this.switchListeningMode();
          }
          // Destroy the one-off websocket provider and close underlying ws
          // connection.
          wsProvider.destroy();
        }, this.config.waitForBlockSeconds * 1_000);
      }
    }, this.config.healthCheckSeconds * 1_000);
  }

  public async start() {
    // http rpc
    const network = await executeWithTimeout(
      this.httpProvider._detectNetwork(),
      10_000,
      "could not establish http connection"
    );
    await this.md.createProxyInstance(this.httpProvider);

    const perps = await this.md
      .exchangeInfo()
      .then(({ pools }) =>
        pools
          .map(({ perpetuals, poolSymbol }) =>
            perpetuals
              .filter(({ state }) => state === "NORMAL")
              .map(
                ({ baseCurrency, quoteCurrency }) =>
                  `${baseCurrency}-${quoteCurrency}-${poolSymbol}`
              )
          )
          .flat()
      );

    for (const symbol of perps) {
      this.orderBooks.add(
        this.md.getOrderBookContract(symbol).target.toString()
      );
    }

    console.log(
      `${new Date(Date.now()).toISOString()}: connected to ${
        network.name
      }, chain id ${Number(network.chainId)}, using HTTP provider`
    );

    // ws rpc
    if (this.config.rpcListenWs.length > 0) {
      this.listeningProvider = new WebSocketProvider(
        new SturdyWebSocket(this.chooseWsRpc(), {
          wsConstructor: Websocket,
        }),
        network,
        { staticNetwork: true }
      );
    } else if (this.config.rpcListenHttp.length > 0) {
      this.listeningProvider = new JsonRpcProvider(
        this.chooseHttpRpc(),
        network,
        { staticNetwork: true }
      );
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
      // Submit last block received ts to executor/distributor to take action if
      // needed.
      this.redisPubClient.publish(
        "listener-error",
        this.lastBlockReceivedAt.toString()
      );

      this.unsubscribe();
      this.switchListeningMode();
    });

    this.listeningProvider.on("block", (blockNumber) => {
      this.lastBlockReceivedAt = Date.now();
      this.redisPubClient.publish("block", blockNumber.toString());
      this.blockNumber = blockNumber;
    });

    const filters = [
      [
        ...[
          "Liquidate",
          "Trade",
          "UpdateMarginAccount",
          "UpdateMarkPrice",
          "PerpetualLimitOrderCancelled",
          "TransferAddressTo",
        ].map(
          (eventName) =>
            this.proxyInterface.encodeFilterTopics(eventName, [])[0] as string
        ),
        ...["PerpetualLimitOrderCreated", "ExecutionFailed"].map(
          (eventName) =>
            this.orderBookInterface.encodeFilterTopics(
              eventName,
              []
            )[0] as string
        ),
      ],
    ];

    this.listeningProvider.on(filters, async (event: Log) => {
      if (event.address === this.md.getProxyAddress()) {
        // event was emitted by the proxy contract
        this.handleProxyEvent(event);
      } else if (this.orderBooks.has(event.address)) {
        // event was emitted by an order book
        this.handleOrderBookEvent(event);
      } // else: not one of ours
    });
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

  private sendMsg(parsedEvent: LogDescription, msg: RedisMsg) {
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

  private handleProxyEvent(event: Log) {
    const parsedEvent = this.proxyInterface.parseLog(event);
    if (!parsedEvent) {
      console.log("Unexpected event log:", event);
      return;
    }
    let msg:
      | LiquidateMsg
      | UpdateMarginAccountMsg
      | TradeMsg
      | UpdateMarkPriceMsg
      | PerpetualLimitOrderCancelledMsg;

    // handle different events
    switch (parsedEvent.name) {
      case "TransferAddressTo":
        this.redisPubClient.publish("Restart", parsedEvent.args[0]);
        this.unsubscribe();
        // force restart
        process.exit(0);

      case "Liquidate":
        {
          const {
            perpetualId,
            trader,
            amountLiquidatedBC,
            liquidator,
            fPnlCC,
            fFeeCC,
            newPositionSizeBC,
          } = parsedEvent.args as unknown as LiquidateEvent.OutputObject;
          const symbol = this.md.getSymbolFromPerpId(Number(perpetualId))!;
          msg = {
            perpetualId: Number(perpetualId),
            symbol: symbol,
            traderAddr: trader,
            tradeAmount: ABK64x64ToFloat(amountLiquidatedBC),
            liquidator: liquidator,
            pnl: ABK64x64ToFloat(fPnlCC),
            fee: ABK64x64ToFloat(fFeeCC),
            newPositionSizeBC: ABK64x64ToFloat(newPositionSizeBC),
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
          };
        }
        break;
      case "Trade":
        {
          const {
            perpetualId,
            order: scOrder,
            trader,
            orderDigest,
          } = parsedEvent.args as unknown as TradeEvent.OutputObject;
          const order = this.md!.smartContractOrderToOrder(scOrder);
          msg = {
            perpetualId: Number(perpetualId),
            trader: trader,
            digest: orderDigest,
            ...order,
            brokerAddr: scOrder.brokerAddr,
            executor: scOrder.executorAddr,
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
          };
        }
        break;
      case "UpdateMarginAccount":
        {
          const { perpetualId, trader, fFundingPaymentCC } =
            parsedEvent.args as unknown as UpdateMarginAccountEvent.OutputObject;
          const symbol = this.md.getSymbolFromPerpId(Number(perpetualId))!;
          msg = {
            perpetualId: Number(perpetualId),
            symbol: symbol,
            traderAddr: trader,
            fundingPaymentCC: ABK64x64ToFloat(fFundingPaymentCC),
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
          };
        }
        break;
      case "UpdateMarkPrice":
        {
          const {
            perpetualId,
            fMarkPricePremium,
            fMidPricePremium,
            fMarkIndexPrice,
          } = parsedEvent.args as unknown as UpdateMarkPriceEvent.OutputObject;
          const symbol = this.md.getSymbolFromPerpId(Number(perpetualId))!;
          msg = {
            perpetualId: Number(perpetualId),
            symbol: symbol,
            midPremium: ABK64x64ToFloat(fMidPricePremium),
            markPremium: ABK64x64ToFloat(fMarkPricePremium),
            spotIndexPrice: ABK64x64ToFloat(fMarkIndexPrice),
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
          };
        }
        break;
      case "PerpetualLimitOrderCancelled":
        {
          const { perpetualId, orderHash } =
            parsedEvent.args as unknown as PerpetualLimitOrderCancelledEvent.OutputObject;
          const symbol = this.md!.getSymbolFromPerpId(Number(perpetualId))!;
          msg = {
            symbol: symbol,
            perpetualId: Number(perpetualId),
            digest: orderHash,
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
          };
        }
        break;
      default:
        console.log("Unexpected event:", parsedEvent);
        return;
    }
    this.sendMsg(parsedEvent, msg);
  }

  private async handleOrderBookEvent(event: Log) {
    const parsedEvent = this.orderBookInterface.parseLog(event);
    if (!parsedEvent) {
      console.log("Unexpected order book event log:", event);
      return;
    }

    let msg: ExecutionFailedMsg | PerpetualLimitOrderCreatedMsg;
    // handle different events
    switch (parsedEvent.name) {
      case "ExecutionFailed":
        {
          const { perpetualId, trader, digest, reason } =
            parsedEvent.args as unknown as ExecutionFailedEvent.OutputObject;
          const symbol = this.md.getSymbolFromPerpId(Number(perpetualId))!;
          msg = {
            perpetualId: Number(perpetualId),
            symbol: symbol,
            trader: trader,
            digest: digest,
            reason: reason,
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
          };
        }
        break;
      case "PerpetualLimitOrderCreated":
        {
          const { perpetualId, trader, brokerAddr, order, digest } =
            parsedEvent.args as unknown as PerpetualLimitOrderCreatedEvent.OutputObject;
          msg = {
            symbol: this.md!.getSymbolFromPerpId(Number(perpetualId))!,
            perpetualId: Number(perpetualId),
            trader: trader,
            brokerAddr: brokerAddr,
            order: this.md!.smartContractOrderToOrder(
              // Convert ethers result proxy to js object with OrderStruct keys
              (order as any as Result).toObject() as IPerpetualOrder.OrderStruct
            ),
            digest: digest,
            block: event.blockNumber,
            hash: event.transactionHash,
            id: `${event.transactionHash}:${event.index}`,
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
        console.log("Unexpected event:", parsedEvent);
        return;
    }
    this.sendMsg(parsedEvent, msg);
  }
}
