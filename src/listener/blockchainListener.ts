import { MarketData, PerpetualDataHandler } from "@d8x/perpetuals-sdk";
import { ethers } from "ethers";
import { Redis } from "ioredis";
import SturdyWebSocket from "sturdy-websocket";
import Websocket from "ws";
import { ListenerConfig } from "../types";
import { chooseRPC, constructRedis, executeWithTimeout } from "../utils";

enum ListeningMode {
  Polling = "Polling",
  Events = "Events",
}

export default class BlockhainListener {
  private config: ListenerConfig;

  // objects
  private httpProvider: ethers.providers.StaticJsonRpcProvider;
  private listeningProvider: ethers.providers.StaticJsonRpcProvider | ethers.providers.WebSocketProvider | undefined;
  private redisPubClient: Redis;
  private md: MarketData;

  // state
  private blockNumber: number = 0;
  private mode: ListeningMode = ListeningMode.Events;
  private perpIds: number[] = [];
  private obAddr: string[] = [];
  private lastBlockReceivedAt: number;

  constructor(config: ListenerConfig) {
    this.config = config;
    this.md = new MarketData(PerpetualDataHandler.readSDKConfig(this.config.sdkConfig));
    this.redisPubClient = constructRedis("BlockchainListener");
    this.httpProvider = new ethers.providers.StaticJsonRpcProvider(chooseRPC(this.config.httpRPC));
    this.lastBlockReceivedAt = Date.now();
  }

  public unsubscribe() {
    console.log(`${new Date(Date.now()).toISOString()} BlockchainListener: unsubscribing`);
    if (this.listeningProvider) {
      this.listeningProvider.removeAllListeners();
    }
  }

  public checkHeartbeat() {
    const blockTime = Math.floor((Date.now() - this.lastBlockReceivedAt) / 1_000);
    if (this.mode == ListeningMode.Polling) {
      console.log(
        `${new Date(Date.now()).toISOString()}: http connection block time is ${blockTime} seconds @ block ${
          this.blockNumber
        }`
      );
      return true;
    }
    const isAlive = blockTime < this.config.waitForBlockseconds;
    if (!isAlive) {
      console.log(
        `${new Date(Date.now()).toISOString()}: websocket connection block time is ${blockTime} - disconnecting`
      );
      return false;
    }
    console.log(
      `${new Date(Date.now()).toISOString()}: websocket connection block time is ${blockTime} seconds @ block ${
        this.blockNumber
      }`
    );
    return true;
  }

  private async switchListeningMode() {
    // reset
    this.unsubscribe();
    this.blockNumber = 0;
    if (this.mode == ListeningMode.Events) {
      console.log(`${new Date(Date.now()).toISOString()}: switching from WS to HTTP`);
      this.mode = ListeningMode.Polling;
      this.listeningProvider = new ethers.providers.StaticJsonRpcProvider(chooseRPC(this.config.httpRPC));
      await this.addListeners(this.listeningProvider);
    } else {
      console.log(`${new Date(Date.now()).toISOString()}: switching from HTTP to WS`);
      this.mode = ListeningMode.Events;
      if (this.listeningProvider) {
        this.listeningProvider.removeAllListeners();
      }
      this.listeningProvider = new ethers.providers.WebSocketProvider(chooseRPC(this.config.wsRPC));
      await this.addListeners(this.listeningProvider);
    }
    this.redisPubClient.publish("switch-mode", this.mode);
  }

  public async start() {
    // infer chain from provider
    const network = await executeWithTimeout(this.httpProvider.ready, 10_000);
    // connect to http provider
    console.log(
      `${new Date(Date.now()).toISOString()}: connected to ${network.name}, chain id ${
        network.chainId
      }, using HTTP provider`
    );
    // connect to ws provider
    if (this.config.wsRPC.length > 0) {
      this.listeningProvider = new ethers.providers.WebSocketProvider(
        new SturdyWebSocket(chooseRPC(this.config.wsRPC), { wsConstructor: Websocket }),
        network
      );
    } else if (this.config.httpRPC.length > 0) {
      this.listeningProvider = new ethers.providers.StaticJsonRpcProvider(chooseRPC(this.config.httpRPC));
    } else {
      throw new Error("Please specify your RPC URLs");
    }

    await this.md.createProxyInstance(this.httpProvider);
    console.log(
      `${new Date(Date.now()).toISOString()}: http connection established with proxy @ ${this.md.getProxyAddress()}`
    );

    // get perpetuals and order books
    this.perpIds = (await this.md.getReadOnlyProxyInstance().getPoolStaticInfo(1, 255))[0].flat();
    this.obAddr = await Promise.all(
      this.perpIds.map((id) => this.md.getReadOnlyProxyInstance().getOrderBookAddress(id))
    );

    // connection is established if we get a block
    setTimeout(async () => {
      if (this.blockNumber == 0) {
        console.log(`${new Date(Date.now()).toISOString()}: websocket connection could not be established`);
        await this.switchListeningMode();
      }
    }, this.config.waitForBlockseconds * 1_000);

    // add listeners
    await this.addListeners(this.listeningProvider);

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
        let wsProvider = new ethers.providers.WebSocketProvider(
          new SturdyWebSocket(chooseRPC(this.config.wsRPC), { wsConstructor: Websocket }),
          network
        );
        wsProvider.once("block", () => {
          success = true;
        });
        // after N seconds, check if we received a block - if yes, switch
        await setTimeout(async () => {
          if (success) {
            this.switchListeningMode();
          }
        }, this.config.waitForBlockseconds * 1_000);
      }
    }, this.config.healthCheckSeconds * 1_000);
  }

  private async addListeners(provider: ethers.providers.StaticJsonRpcProvider | ethers.providers.WebSocketProvider) {
    await executeWithTimeout(provider.ready, 10_000);
    // on error terminate
    provider.on("error", (e) => {
      console.log(
        `${new Date(Date.now()).toISOString()} BlockchainListener received error msg in ${this.mode} mode:`,
        e
      );
      this.unsubscribe();
      this.switchListeningMode();
    });

    // broadcast new blocks
    provider.on("block", (blockNumber) => {
      this.redisPubClient.publish("block", blockNumber.toString());
      this.lastBlockReceivedAt = Date.now();
      if (blockNumber % 10 == 0) {
        console.log(
          `${new Date(Date.now()).toISOString()} BlockchainListener received new block ${blockNumber} @ ts ${
            Date.now() / 1_000
          }, mode: ${this.mode}`
        );
      }
      this.blockNumber = blockNumber;
    });

    // smart contract events
    const orderBooks = this.obAddr.map((addr) => new ethers.Contract(addr, this.md.getABI("lob")!, provider));
    const proxy = new ethers.Contract(this.md.getProxyAddress(), this.md.getABI("proxy")!, provider);

    // add listeners for each order book
    orderBooks.map((ob, idx) => {
      console.log(`Subscribing to ${this.perpIds[idx]} ${this.obAddr[idx]}`);

      // order posted
      ob.on("PerpetualLimitOrderCreated", (perpetualId, trader, _brokerAddr, scOrder, digest) => {
        this.redisPubClient.publish(
          "PerpetualLimitOrderCreated",
          JSON.stringify({
            perpetualId: perpetualId,
            trader: trader,
            order: { ...scOrder },
            digest: digest,
            fromEvent: true,
          })
        );
        console.log(
          `${new Date(Date.now()).toISOString()} Block: ${this.blockNumber}, ${
            this.mode
          } mode, PerpetualLimitOrderCreated:`,
          {
            perpetualId: perpetualId,
            trader: trader,
            digest: digest,
          }
        );
      });

      // order execution failed
      ob.on("ExecutionFailed", (perpetualId, _trader, digest, reason) => {
        this.redisPubClient.publish(
          "ExecutionFailed",
          JSON.stringify({ perpetualId: perpetualId, digest: digest, reason: reason })
        );
        console.log(
          `${new Date(Date.now()).toISOString()} Block: ${this.blockNumber}, ${this.mode} mode, ExecutionFailed:`,
          {
            perpetualId: perpetualId,
            digest: digest,
            reason: reason,
          }
        );
      });
    });

    // order cancelled
    proxy.on("PerpetualLimitOrderCancelled", (perpetualId, digest) => {
      this.redisPubClient.publish(
        "PerpetualLimitOrderCancelled",
        JSON.stringify({ perpetualId: perpetualId, digest: digest })
      );
      console.log(
        `${new Date(Date.now()).toISOString()} Block: ${this.blockNumber}, ${
          this.mode
        } mode, PerpetualLimitOrderCancelled:`,
        {
          perpetualId: perpetualId,
          digest: digest,
        }
      );
    });

    // order executed
    proxy.on(
      "Trade",
      (perpetualId, traderAddr, _positionId, _order, digest, _fNewPos, _fPrice, _fFee, _fPnLCC, _fB2C) => {
        this.redisPubClient.publish(
          "Trade",
          JSON.stringify({ perpetualId: perpetualId, digest: digest, traderAddr: traderAddr })
        );
        console.log(`${new Date(Date.now()).toISOString()} Block: ${this.blockNumber}, ${this.mode} mode, Trade:`, {
          perpetualId: perpetualId,
          traderAddr: traderAddr,
          digest: digest,
        });
      }
    );
  }
}
