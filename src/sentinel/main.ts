import { loadConfig } from "../utils";
import BlockhainListener from "./blockchainListener";
import BrokerListener from "./brokerListener";

require("dotenv").config();

async function start() {
  const sdkConfig = process.env.SDK_CONFIG;
  if (sdkConfig == undefined) {
    throw new Error(`Environment variable SDK_CONFIG not defined.`);
  }
  const cfg = loadConfig(sdkConfig);

  const eventStreamer = new BlockhainListener(cfg);
  eventStreamer.start();

  for (let i = 0; i < cfg.brokerWS.length; i++) {
    const wsStreamer = new BrokerListener(cfg, i);
    wsStreamer.start();
  }
}

start();
