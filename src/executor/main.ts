import Executor from "./executor.js";
import { loadAccounts, loadConfig, sleep } from "../utils.js";
import Distributor from "./distributor.js";
import 'dotenv/config';

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
  await executor.initialize();

  try {
    await executor.fundWallets(addr);
  } catch (e) {
    console.log(e);
    await sleep(60_000);
    process.exit(1);
  }

  const obj = new Distributor(cfg, executor);
  await obj.initialize();

  // Set the managing distributor for our executor before running it
  executor.setDistributor(obj);

  obj.run();
  executor.run();
}

start();
