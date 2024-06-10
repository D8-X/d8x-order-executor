import { sleep } from "@d8x/perpetuals-sdk";
import Executor from "./executor";
import { loadAccounts, loadConfig } from "../utils";
import Distributor from "./distributor";

require("dotenv").config();

async function start() {
  const sdkConfig = process.env.SDK_CONFIG;
  if (sdkConfig == undefined) {
    throw new Error(`Environment variable SDK_CONFIG not defined.`);
  }
  // seed phrase for executor
  const seedPhrase = process.env.SEED_PHRASE;
  if (seedPhrase == undefined) {
    throw new Error(`Environment variable SEED_PHRASE not defined.`);
  }

  const cfg = loadConfig(sdkConfig);

  // bot treasury
  const {
    pk: [treasuryPK],
  } = loadAccounts(seedPhrase, 0, 0);

  // bot wallets
  const { addr, pk } = loadAccounts(seedPhrase, 1, cfg.bots);
  console.log(
    `\nStarting ${addr.length} bots with addresses ${addr.join("\n")}`
  );

  const executor = new Executor(treasuryPK, pk, cfg);

  try {
    await executor.fundWallets(addr);
  } catch (e) {
    console.log(e);
    await sleep(60_000);
    process.exit(1);
  }
  await executor.initialize();
  executor.run();

  const obj = new Distributor(cfg, executor);
  await obj.initialize();
  obj.run();
}

start();
