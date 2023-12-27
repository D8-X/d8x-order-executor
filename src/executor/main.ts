import { BigNumber, ethers } from "ethers";
import Executor from "./executor";
import { chooseRPC, loadAccounts, loadExecutorConfig, sleep } from "../utils";

require("dotenv").config();

/**
 * args (command line):
 */
async function run() {
  let args = process.argv.slice(2);
  // args
  const idxFrom = Number(args[0]);
  // validate
  if (idxFrom <= 0) {
    throw new Error(
      `Invalid wallet starting index for bot: ${idxFrom}. It should be a positive integer.`
    );
  }
  // env
  const chainId = process.env.CHAIN_ID
    ? BigNumber.from(process.env.CHAIN_ID as string)
    : undefined;
  const mnemonicSeed = process.env.SEED_PHRASE;
  const earningsAddr = process.env.EARNINGS_WALLET;
  const accountsPerPerp = Number(process.env.ACCOUNTS_PER_BOT ?? "1");
  const modulo = Number(process.env.PEER_COUNT as string);
  const residual = Number(process.env.PEER_INDEX as string);
  // validate
  if (!chainId) {
    throw new Error(
      "Please enter a chain ID in .env file, e.g. export CHAIN_ID=42"
    );
  }
  if (mnemonicSeed === undefined) {
    throw new Error(
      "Please enter a mnemonic seed in .env file, e.g. export SEED_PHRASE='your seed phrase'"
    );
  }
  if (earningsAddr === undefined) {
    throw new Error(
      "Please enter an address to collect earnings in .env file, e.g. export EARNINGS_WALLET='0xYourAddress'"
    );
  }
  if (accountsPerPerp === undefined || accountsPerPerp <= 0) {
    throw new Error(
      "Please enter a valid number of wallets per bot in .env file, e.g. export ACCOUNTS_PER_BOT=5"
    );
  }
  if (
    modulo == undefined ||
    modulo < 1 ||
    residual == undefined ||
    residual < 0 ||
    residual >= modulo
  ) {
    throw new Error(`Invalid peer index/count pair ${residual}/${modulo}`);
  }
  // bot treasury
  const {
    addr: [treasuryAddr],
    pk: [treasuryPK],
  } = loadAccounts(mnemonicSeed, 0, 0);
  // bot wallets
  const { addr, pk } = loadAccounts(
    mnemonicSeed,
    idxFrom,
    idxFrom + accountsPerPerp - 1
  );
  // load config
  const execConfig = loadExecutorConfig(chainId);
  // check that price services are up
  for (const pxServices of execConfig.priceFeedEndpoints) {
    let someOk = false;
    for (const endpoint of pxServices.endpoints) {
      const response = await fetch(endpoint.replace("/api", "/live"));
      someOk = someOk || response.ok;
    }
    if (!someOk) {
      console.log(
        `${pxServices.type} price service is down. Reconnecting in 1 minute`
      );
      await sleep(60_000);
      process.exit(1);
    }
  }
  console.log("Price services are live");
  // start bot
  console.log(`\nStarting executor with ${addr.length} wallets:`);
  for (let refAddr of addr) {
    console.log(refAddr);
  }

  let lastRPC = "";
  let executor: Executor;
  let runPromise: Promise<void>;
  try {
    executor = new Executor(pk, execConfig, modulo, residual, earningsAddr);
    let currRPC = chooseRPC(execConfig.executeRPC, lastRPC);
    console.log(`using RPC:\n${currRPC}`);
    lastRPC = currRPC;
    const provider = new ethers.providers.StaticJsonRpcProvider(currRPC);
    const treasury = new ethers.Wallet(treasuryPK, provider);
    // min balance should cover 1e7 gas
    const minBalance = ethers.utils.parseUnits(
      `${execConfig.maxGasPriceGWei * 1e7}`,
      "gwei"
    ); //ethers.BigNumber.from(Math.round(refConfig.maxGasPriceGWei * 1e16)); // 1e9: to wei, 1e7: 10 million
    for (let execAddr of addr) {
      const executorBalance = await provider.getBalance(execAddr);
      console.log(
        `Executor (${execAddr}) balance: ${ethers.utils.formatUnits(
          executorBalance
        )} ETH (or native token)`
      );
      const treasuryBalance = await provider.getBalance(treasuryAddr);
      console.log(
        `Treasury (${treasuryAddr}) balance: ${ethers.utils.formatUnits(
          treasuryBalance
        )} ETH (or native token)`
      );
      console.log(
        `Minimum balance: ${ethers.utils.formatUnits(
          minBalance
        )} ETH (or native token)`
      );
      if (executorBalance.lt(minBalance)) {
        // transfer twice the min so it doesn't transfer every time
        const transferAmount = minBalance.mul(2).sub(executorBalance);
        if (transferAmount.lt(treasuryBalance)) {
          console.log(
            `Funding relayer with ${ethers.utils.formatUnits(
              transferAmount
            )} tokens...`
          );
          const tx = await treasury.sendTransaction({
            to: execAddr,
            value: transferAmount,
          });
          console.log(`Transferring tokens - tx hash: ${tx.hash}`);
          await tx.wait();
          console.log(
            `Successfully transferred ${ethers.utils.formatUnits(
              transferAmount
            )} ETH (or native token) from treasury to executor ${execAddr}`
          );
        } else {
          // TODO: notify a human
          throw new Error(
            `CRITICAL: insufficient balance in treasury ${ethers.utils.formatUnits(
              treasuryBalance
            )} ETH (or native token)`
          );
        }
      }
    }
    await executor.initialize();
    runPromise = executor.run();
  } catch (error: any) {
    // we are here if we couldn't create a executor (typically an RPC issue)
    if (error?.reason) {
      console.log(`error creating executor: ${error?.reason}`);
    }
    console.error(error);
    // exit to restart
    process.exit(1);
  }
  runPromise;
}

run();
