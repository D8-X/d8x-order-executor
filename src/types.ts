import { Order } from "@d8x/perpetuals-sdk";

export interface OrderBundle {
  id: string;
  order: Order;
  isLocked: boolean;
  ts: number;
  onChain: boolean;
  traderAddr: string;
}

export interface ExecutorConfig {
  chainId: number;
  executeRPC: string[];
  recountRPC: string[];
  executeIntervalSeconds: number;
  refreshOrdersSeconds: number;
  maxGasPriceGWei: number;
  maxExecutionBatchSize: number;
  executeDelaySeconds: number;
  recountIntervalSeconds: 10;
  priceFeedEndpoints: Array<{ type: string; endpoints: string[] }>;
}

export interface BrokerListenerConfig {
  brokerWS: string[];
  brokerReconnectIntervalMS: number;
  sdkConfig: string;
  httpRPC: string[];
}

export interface BlockchainListenerConfig {
  chainId: number;
  sdkConfig: string;
  httpRPC: string[];
  wsRPC: string[];
  waitForBlockseconds: number;
  healthCheckSeconds: number;
}

export interface ListenerConfig
  extends BlockchainListenerConfig,
    BrokerListenerConfig {}

export interface BrokerWSErrorData {
  error: string;
}

export interface BrokerWSUpdateData {
  orderId: string;
  traderAddr: string;
  iDeadline: number;
  flags: number;
  fAmount: string;
  fLimitPrice: string;
  fTriggerPrice: string;
  executionTimestamp: number;
}

export interface BrokerWSMessage {
  type: string;
  topic: string;
  data: "ack" | BrokerWSErrorData | BrokerWSUpdateData;
}
export interface RedisConfig {
  host: string;
  port: number;
  password: string;
}

export interface watchDogAlarm {
  isCoolOff: boolean;
  timestampSec: number;
}

export interface GasPriceV2 {
  maxPriorityFee: number;
  maxfee: number;
}

export interface GasInfo {
  safeLow: number | GasPriceV2;
  standard: number | GasPriceV2;
  fast: number | GasPriceV2;
  fastest?: number;
  estimatedBaseFee?: number;
  blockTime: number;
  blockNumber: number;
}
