import * as promClient from "prom-client";
import express from "express";
import { logger } from "../logger.js";

export enum OrderExecutionError {
  insufficient_funds = "insufficient_funds",
  order_not_found = "order_not_found",
  too_low_gas_price = "too_low_gas_price",
  too_low_intrinsic_gas = "too_low_intrinsic_gas",
}

export type ExecutionOutcome = "confirmed" | "failed" | "rejected";

export class ExecutorMetrics {
  private readonly chain: string;

  constructor(
    chain: string = process.env.SDK_CONFIG ?? "unknown",
    private port: number = 9001,
    private endpoint: string = "metrics",
    private metricsList = {
      orderExecutionErrors: new promClient.Counter({
        name: "execute_order_errors",
        help: "Cumulative amount of errors during order execution",
        labelNames: ["type"],
      }),
      orderExecutionConfirmations: new promClient.Counter({
        name: "execute_order_confirmations",
        help: "Number of confirmed executed orders",
      }),
      orderExecutionFailedConfirmations: new promClient.Counter({
        name: "execute_order_failed_confirmations",
        help: "Number of failed executed orders confirmations",
      }),
      openOrders: new promClient.Gauge({
        name: "executor_open_orders",
        help: "Number of currently open orders per symbol and order type, observed at the last refresh.",
        labelNames: ["chain", "symbol", "type"] as const,
      }),
      openOrderOldestAge: new promClient.Gauge({
        name: "executor_open_order_oldest_age_seconds",
        help: "Age in seconds of the oldest currently open order per symbol and order type.",
        labelNames: ["chain", "symbol", "type"] as const,
      }),
      lastExecutionTimestamp: new promClient.Gauge({
        name: "executor_last_execution_timestamp_seconds",
        help: "Unix timestamp of the last successful order execution per worker wallet.",
        labelNames: ["chain", "bot_idx", "bot_addr"] as const,
      }),
      executionsTotal: new promClient.Counter({
        name: "executor_executions_total",
        help: "Cumulative count of order execution outcomes per worker wallet.",
        labelNames: ["chain", "bot_idx", "bot_addr", "outcome"] as const,
      }),
    }
  ) {
    this.chain = chain;
  }

  public async start() {
    this.metricsEndpoint(this.port, this.endpoint);
  }

  private async metricsEndpoint(port: number, endpoint: string = "metrics") {
    const app = express();
    app.get(`/${endpoint}`, async (req: any, res: any) => {
      res.set("Content-Type", promClient.register.contentType);
      res.end(await promClient.register.metrics());
    });
    logger.info(
      `Starting metrics endpoint available at http://localhost:${port}/${endpoint}`
    );
    app.listen(port);
  }

  private incrOrderExecutionError(type: OrderExecutionError) {
    this.metricsList.orderExecutionErrors.inc({ type });
  }

  public incrementInsufficientFunds() {
    this.incrOrderExecutionError(OrderExecutionError.insufficient_funds);
  }

  public incrementOrderNotFound() {
    this.incrOrderExecutionError(OrderExecutionError.order_not_found);
  }

  public incrementGasPriceTooLow() {
    this.incrOrderExecutionError(OrderExecutionError.too_low_gas_price);
  }

  public incrementIntrinsicGasTooLow() {
    this.incrOrderExecutionError(OrderExecutionError.too_low_intrinsic_gas);
  }

  public incrementOrderExecutionConfirmations() {
    this.metricsList.orderExecutionConfirmations.inc();
  }

  public incrementOrderExecutionFailedConfirmations() {
    this.metricsList.orderExecutionFailedConfirmations.inc();
  }

  public setOpenOrders(symbol: string, type: string, count: number, oldestAgeSeconds: number) {
    const t = type.toLowerCase();
    this.metricsList.openOrders.labels(this.chain, symbol, t).set(count);
    this.metricsList.openOrderOldestAge.labels(this.chain, symbol, t).set(oldestAgeSeconds);
  }

  public observeLastExecution(botIdx: number, botAddr: string, when: Date = new Date()) {
    this.metricsList.lastExecutionTimestamp
      .labels(this.chain, String(botIdx), botAddr.toLowerCase())
      .set(Math.floor(when.getTime() / 1000));
  }

  public incExecutionOutcome(botIdx: number, botAddr: string, outcome: ExecutionOutcome, n: number = 1) {
    if (n <= 0) return;
    this.metricsList.executionsTotal
      .labels(this.chain, String(botIdx), botAddr.toLowerCase(), outcome)
      .inc(n);
  }
}
