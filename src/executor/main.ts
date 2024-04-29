import Executor from "./executor";
import { loadAccounts, loadConfig, sleep } from "../utils";

require("dotenv").config();

async function run() {
  // sdk config
  const sdkConfig = process.env.SDK_CONFIG;
  if (sdkConfig == undefined) {
    throw new Error(`Environment variable SDK_CONFIG not defined.`);
  }
  // seed phrase
  const seedPhrase = process.env.SEED_PHRASE;
  if (seedPhrase == undefined) {
    throw new Error(`Environment variable SEED_PHRASE not defined.`);
  }
  // config
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

  const bot = new Executor(treasuryPK, pk, cfg);

  try {
    await bot.fundWallets(addr);
  } catch (e) {
    console.log(e);
    await sleep(60_000);
    process.exit(1);
  }
  await bot.initialize();

  bot.run();
}

run();
