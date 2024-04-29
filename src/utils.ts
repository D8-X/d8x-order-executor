import { Redis } from "ioredis";
import { RedisConfig, ExecutorConfig, OrderType } from "./types";
import { BigNumber, ethers } from "ethers";
import {
  MASK_LIMIT_ORDER,
  MASK_STOP_ORDER,
  MAX_64x64,
  ORDER_TYPE_LIMIT,
  ORDER_TYPE_MARKET,
  ORDER_TYPE_STOP_LIMIT,
  ORDER_TYPE_STOP_MARKET,
  containsFlag,
} from "@d8x/perpetuals-sdk";

require("dotenv").config();

export function loadConfig(sdkConfig: string): ExecutorConfig {
  const configList = require("./config/live.config.json") as ExecutorConfig[];
  const config = configList.find((config) => config.sdkConfig == sdkConfig);
  if (!config) {
    throw new Error(`SDK Config ${sdkConfig} not found in config file.`);
  }
  return config;
}

export function loadAccounts(
  mnemonicSeed: string,
  idxFrom: number,
  idxTo: number
) {
  let addr: string[] = [];
  let pk: string[] = [];
  for (let myIdx = idxFrom; myIdx <= idxTo; myIdx++) {
    let [myAddr, myPK] = getPrivateKeyFromSeed(mnemonicSeed, myIdx);
    addr.push(myAddr);
    pk.push(myPK);
  }
  if (pk.length < 1) {
    throw new Error("private key not defined");
  }
  return { addr: addr, pk: pk };
}

export function getPrivateKeyFromSeed(mnemonic: string, idx: number) {
  if (mnemonic == undefined) {
    throw Error("mnemonic seed phrase needed: export mnemonic='...");
  }
  const baseDerivationPath = "m/44'/60'/0'/0";
  const path = `${baseDerivationPath}/${idx}`;
  const mnemonicWallet = ethers.Wallet.fromMnemonic(mnemonic, path);
  return [mnemonicWallet.address, mnemonicWallet.privateKey];
}

export function getRedisConfig(): RedisConfig {
  let config = {
    host: process.env.REDIS_HOST!,
    port: +process.env.REDIS_PORT!,
    password: process.env.REDIS_PASSWORD,
  };
  return config;
}

export function constructRedis(name: string): Redis {
  let client;
  let redisConfig = getRedisConfig();
  client = new Redis(redisConfig);
  client.on("error", (err) => console.log(`${name} Redis Client Error:` + err));
  return client;
}

/**
 *
 * @param promise async function to be esxecuted
 * @param timeoutMs timeout in MS
 * @param errMsgOnTimeout optional error message
 * @returns function return value or ends in error
 */
export function executeWithTimeout<T>(
  promise: Promise<T>,
  timeout: number,
  errMsgOnTimeout: string | undefined = undefined
): Promise<T> {
  let timeoutId: NodeJS.Timeout;

  const timeoutPromise = new Promise<T>((_, reject) => {
    timeoutId = setTimeout(() => {
      const msg = errMsgOnTimeout ?? "Function execution timed out.";
      reject(new Error(msg));
    }, timeout);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => {
    clearTimeout(timeoutId);
  });
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function flagToOrderType(
  orderFlags: BigNumber,
  orderLimitPrice: BigNumber
): OrderType {
  let flag = BigNumber.from(orderFlags);
  let isLimit = containsFlag(flag, MASK_LIMIT_ORDER);
  let hasLimit =
    !BigNumber.from(orderLimitPrice).eq(0) ||
    !BigNumber.from(orderLimitPrice).eq(MAX_64x64);
  let isStop = containsFlag(flag, MASK_STOP_ORDER);

  if (isStop && hasLimit) {
    return ORDER_TYPE_STOP_LIMIT;
  } else if (isStop && !hasLimit) {
    return ORDER_TYPE_STOP_MARKET;
  } else if (isLimit && !isStop) {
    return ORDER_TYPE_LIMIT;
  } else {
    return ORDER_TYPE_MARKET;
  }
}
