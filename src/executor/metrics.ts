import * as promClient from "prom-client";
import express from "express";

export enum OrderExecutionError {
  insufficient_funds = "insufficient_funds",
  order_not_found = "order_not_found",
  too_low_gas_price = "too_low_gas_price",
  too_low_intrinsic_gas = "too_low_intrinsic_gas",
}

export class ExecutorMetrics {
  constructor(
    // Port on which metrics endpoint server will be exposed
    private port: number = 9001,
    // Endpoint on which the metrics will be exposed
    private endpoint: string = "metrics",

    // All the exported metrics below
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
    }
  ) {}

  /**
   * Start the metrics endpoint
   */
  public async start() {
    this.metricsEndpoint(this.port, this.endpoint);
  }

  /**
   * Exposes metrics endpoint at given port
   * @param port
   */
  private async metricsEndpoint(port: number, endpoint: string = "metrics") {
    const app = express();
    app.get(`/${endpoint}`, async (req: any, res: any) => {
      res.set("Content-Type", promClient.register.contentType);
      res.end(await promClient.register.metrics());
    });
    console.log(
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
}
