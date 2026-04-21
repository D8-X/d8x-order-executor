import pino from "pino";

const level = process.env.LOG_LEVEL ?? "info";
const pretty = process.env.LOG_PRETTY === "1";

const base = pino(
  pretty
    ? {
        level,
        transport: {
          target: "pino-pretty",
          options: { colorize: true, translateTime: "SYS:standard" },
        },
      }
    : { level }
);

function normalize(args: unknown[]): { obj?: object; msg: string } {
  if (args.length === 0) return { msg: "" };
  const first = args[0];
  if (
    first !== null &&
    typeof first === "object" &&
    !(first instanceof Error)
  ) {
    const rest = args.slice(1);
    const msg = rest
      .map((a) => (typeof a === "string" ? a : safeStringify(a)))
      .join(" ");
    return { obj: first as object, msg };
  }
  const msg = args
    .map((a) => (typeof a === "string" ? a : safeStringify(a)))
    .join(" ");
  return { msg };
}

function safeStringify(v: unknown): string {
  if (v instanceof Error) return v.stack ?? v.message;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

export const logger = {
  info: (...args: unknown[]) => {
    const { obj, msg } = normalize(args);
    if (obj) base.info(obj, msg);
    else base.info(msg);
  },
  warn: (...args: unknown[]) => {
    const { obj, msg } = normalize(args);
    if (obj) base.warn(obj, msg);
    else base.warn(msg);
  },
  error: (...args: unknown[]) => {
    const { obj, msg } = normalize(args);
    if (obj) base.error(obj, msg);
    else base.error(msg);
  },
  debug: (...args: unknown[]) => {
    const { obj, msg } = normalize(args);
    if (obj) base.debug(obj, msg);
    else base.debug(msg);
  },
};
