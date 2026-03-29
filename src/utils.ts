import { Redis } from "ioredis";
import { RedisConfig, ExecutorConfig, OrderType } from "./types.js";
import {
  MASK_LIMIT_ORDER,
  MASK_STOP_ORDER,
  MAX_64x64,
  ORDER_TYPE_LIMIT,
  ORDER_TYPE_MARKET,
  ORDER_TYPE_STOP_LIMIT,
  ORDER_TYPE_STOP_MARKET,
  containsFlag,
} from "@d8-x/d8x-node-sdk";
import { HDNodeWallet } from "ethers";
import fs from "node:fs";
import 'dotenv/config';

const shuffle = (array: string[]) => {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
};


export function loadConfig(sdkConfig: string): ExecutorConfig {
  const cfgPath = process.env.EXECUTOR_CONFIG;
  if (!cfgPath) throw new Error("EXECUTOR_CONFIG env var not set");

  const configList = JSON.parse(fs.readFileSync(cfgPath, "utf8")) as ExecutorConfig[];
  const config = configList.find((c) => c.sdkConfig === sdkConfig);

  if (!config) {
    throw new Error(`SDK Config ${sdkConfig} not found in config file.`);
  }

  config.rpcExec = shuffle(config.rpcExec);
  config.rpcListenHttp = shuffle(config.rpcListenHttp);
  config.rpcWatch = shuffle(config.rpcWatch);
  config.rpcListenWs = shuffle(config.rpcListenWs);

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
  const mnemonicWallet = HDNodeWallet.fromPhrase(mnemonic, undefined, path);
  return [mnemonicWallet.address, mnemonicWallet.privateKey];
}

export function getRedisConfig(): RedisConfig {
  let config = {
    host: process.env.REDIS_HOST!,
    port: +process.env.REDIS_PORT!,
    password: process.env.REDIS_PASSWORD,
    db: +process.env.REDIS_ID!,
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
  orderFlags: bigint,
  orderLimitPrice: bigint
): OrderType {
  let flag = BigInt(orderFlags);
  let isLimit = containsFlag(flag, MASK_LIMIT_ORDER);
  let hasLimit =
    BigInt(orderLimitPrice) != 0n || BigInt(orderLimitPrice) != MAX_64x64;
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

export async function createRedisTimer(r: Redis, name: string) {
  const d = new Date();
  await r.rpush(name, d.getTime());

  console.log(`[REDIS TIMER: ${name}] Started at ${d.toISOString()}`);
}

export async function subRedisTimer(r: Redis, name: string, info: string) {
  const prev = await r.rpop(name);
  const d = new Date();
  if (prev !== null) {
    const prevTimestamp = parseInt(prev);
    const diff = (d.getTime() - prevTimestamp) / 1000;
    console.log(
      `[REDIS TIMER: ${name}] ${info} at ${d.toISOString()} sub from last: ${diff}s`
    );

    await r.rpush(name, prev);
    await r.rpush(name, d.getTime());
  }
}
