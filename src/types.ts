import {
  ORDER_TYPE_LIMIT,
  ORDER_TYPE_MARKET,
  ORDER_TYPE_STOP_LIMIT,
  ORDER_TYPE_STOP_MARKET,
  Order,
  floatToABK64x64,
} from "@d8x/perpetuals-sdk";
import { IPerpetualOrder } from "@d8x/perpetuals-sdk/dist/esm/contracts/IPerpetualManager";

export const ZERO_POSITION = floatToABK64x64(0);

export interface Position {
  address: string;
  perpetualId: number;
  positionBC: number;
  cashCC: number;
  lockedInQC: number;
  unpaidFundingCC: number;
}

export interface OrderBundle {
  symbol: string;
  trader: string;
  digest: string;
  order?: Order;
  type: OrderType;
  isPredictionMarket: boolean;
}

export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
}

export interface ExecutorConfig {
  sdkConfig: string;
  bots: number;
  rewardsAddress: string;
  brokerWS: string[];
  rpcExec: string[];
  rpcWatch: string[];
  rpcListenHttp: string[];
  rpcListenWs: string[];
  waitForBlockSeconds: number;
  healthCheckSeconds: number;
  refreshOrdersIntervalSecondsMax: number;
  refreshOrdersIntervalSecondsMin: number;
  executeIntervalSecondsMax: number;
  executeIntervalSecondsMin: number;
  refreshOrdersSecondsMax: number;
  fetchPricesIntervalSecondsMin: number;
  brokerReconnectIntervalMaxSeconds: number;
  maxGasPriceGWei: number;
  gasLimit: number;
  priceFeedEndpoints: [{ type: "pyth" | "odin"; endpoints: string[] }];
  // How much gas to send to each bot when funding them in ETH. If not defined,
  // gasLimit*5 approach is used.
  fundGasAmountETH?: number;
  // What is the minimum balance in ETH that a bot must have to be funded
  minimumBalanceETH?: number;
  // config source to override sdk default
  configSource?: string;
  // oracle network to override default
  priceFeedConfigNetwork?: string;
}

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

export interface RedisMsg {
  block: number;
  hash: string;
  id: string;
}

export type OrderType =
  | typeof ORDER_TYPE_MARKET
  | typeof ORDER_TYPE_LIMIT
  | typeof ORDER_TYPE_STOP_LIMIT
  | typeof ORDER_TYPE_STOP_MARKET;

export interface BrokerOrderMsg {
  symbol: string;
  perpetualId: number;
  traderAddr: string;
  digest: string;
  type: OrderType;
}

export interface LiquidateMsg extends RedisMsg {
  perpetualId: number;
  symbol: string;
  traderAddr: string;
  tradeAmount: number;
  pnl: number;
  fee: number;
  newPositionSizeBC: number;
  liquidator: string;
}

export interface UpdateMarginAccountMsg extends RedisMsg {
  perpetualId: number;
  symbol: string;
  traderAddr: string;
  // positionBC: number;
  // cashCC: number;
  // lockedInQC: number;
  fundingPaymentCC: number;
}

export interface UpdateMarkPriceMsg extends RedisMsg {
  perpetualId: number;
  symbol: string;
  midPremium: number;
  markPremium: number;
  spotIndexPrice: number;
}

export interface UpdateUnitAccumulatedFundingMsg extends RedisMsg {
  perpetualId: number;
  symbol: string;
  unitAccumulatedFundingCC: number;
}

export interface PerpetualLimitOrderCreatedMsg extends RedisMsg {
  symbol: string;
  perpetualId: number;
  trader: string;
  brokerAddr?: string;
  order: Order;
  digest: string;
}

export interface PerpetualLimitOrderCancelledMsg extends RedisMsg {
  symbol: string;
  perpetualId: number;
  digest: string;
}

export interface ExecutionFailedMsg extends RedisMsg {
  symbol: string;
  perpetualId: number;
  trader: string;
  digest: string;
  reason: string;
}

export interface TradeMsg extends RedisMsg, Order {
  perpetualId: number;
  trader: string;
  executor: string;
  digest: string;
}

export interface LiquidateTraderMsg {
  symbol: string;
  traderAddr: string;
  // px: PriceFeedSubmission;
}

export interface ExecuteOrderMsg {
  symbol: string;
  digest: string;
  trader: string;
  onChain: boolean;
}

export enum BotStatus {
  Ready = "Ready",
  Busy = "Busy",
  PartialError = "PartialError",
  Error = "Error",
}

export interface ExecuteOrderCommand {
  symbol: string;
  digest: string;
  trader: string;
  reduceOnly: boolean | undefined;
}
