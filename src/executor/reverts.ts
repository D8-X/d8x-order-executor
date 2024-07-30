import { IncomingWebhook } from "@slack/webhook";
import { Provider, toUtf8String, TransactionResponse } from "ethers";

// sendTxRevertedMessage sends a message to the Slack channel when a transaction
// is reverted with reason other than execution frontruns
export const sendTxRevertedMessage = async (
  revertReason: Promise<string>,
  txHash: string,
  orderDigest: string,
  perpetualSymbol: string
) => {
  const revertMsg = await revertReason;

  // Skip reverts which should be ignored
  const whiteList = ["order not found"];
  for (const msg of whiteList) {
    if (revertMsg.toLowerCase().includes(msg.toLowerCase())) {
      return;
    }
  }
  console.log("transaction reverted with non-whitelisted reason", {
    revertReasonMessage: revertMsg,
    txHash,
    orderDigest,
  });

  // Send message to Slack
  const endpoint = process.env.SLACK_WEBHOOK_URL!;
  if (endpoint === undefined || endpoint === "") {
    console.log("SLACK_WEBHOOK_URL not set, skipping Slack notification");
    return;
  }

  const webhook = new IncomingWebhook(endpoint);
  await webhook.send({
    blocks: [
      {
        type: "header",
        text: {
          type: "plain_text",
          text: "🚨 executing order failed",
          emoji: true,
        },
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `Revert reason: *${revertMsg}*`,
        },
      },
      {
        type: "divider",
      },
      {
        type: "section",
        fields: [
          {
            type: "mrkdwn",
            text: `*Network*\n${process.env.SDK_CONFIG}`,
          },
          {
            type: "mrkdwn",
            text: `*Server IP*\n${process.env.SERVER_IP}`,
          },
          {
            type: "mrkdwn",
            text: `*Order digest*\n${orderDigest}`,
          },
          {
            type: "mrkdwn",
            text: `*Symbol*\n${perpetualSymbol}`,
          },
        ],
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*Tx hash*\n${txHash}`,
        },
      },
    ],
  });
};

// getTxRevertReason retrieves revert reason string
export const getTxRevertReason = async (
  tx: TransactionResponse,
  p: Provider
) => {
  const txResp = await p.call({
    to: tx.to,
    from: tx.from,
    data: tx.data,
    value: tx.value,
  });

  // Substring 128 also works. revert => Error(string)
  return toUtf8String("0x" + txResp.substring(138)).trim();
};
