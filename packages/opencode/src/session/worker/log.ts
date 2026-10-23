import { Effect } from "effect"
import type { EffectBridge } from "@/effect/bridge"

export type WorkerLogLevel = "info" | "warn" | "error"

export type WorkerLogger = (message: string, fields: Record<string, unknown>, level?: WorkerLogLevel) => void

const WORKER_LOG_LABEL = "session workers"

export const WORKER_LOG_PREFIX = `${WORKER_LOG_LABEL}: `

export const workerLogMessage = (event: string): string => `${WORKER_LOG_PREFIX}${event}`

export const noopWorkerLogger: WorkerLogger = () => {}

/**
 * Parse a raw severity (an explicit argument or a `level` field) into a trusted
 * `WorkerLogLevel`. Unrecognized or non-string values fall back to "info" and
 * never throw: an observability seam must not break its emitting call site.
 */
const parseWorkerLogLevel = (raw: unknown): WorkerLogLevel => {
  if (raw === "warn" || raw === "error") return raw
  if (typeof raw !== "string") return "info"
  const normalized = raw.toLowerCase()
  if (normalized === "warn" || normalized === "error") return normalized
  return "info"
}

const logAtLevel = (level: WorkerLogLevel, message: string, fields: Record<string, unknown>) => {
  if (level === "error") return Effect.logError(message, fields)
  if (level === "warn") return Effect.logWarning(message, fields)
  return Effect.logInfo(message, fields)
}

/** Drop the `level` key so severity is not duplicated in the emitted fields. */
const withoutLevelField = (fields: Record<string, unknown>): Record<string, unknown> => {
  if (!("level" in fields)) return fields
  const { level: _severity, ...rest } = fields
  return rest
}

/** Field keys that may carry the session id, in precedence order. */
const SESSION_ID_KEYS = ["sessionID", "session.id"] as const

/** The branded session-id prefix, stripped from the human-facing message prefix. */
const SESSION_ID_PREFIX = "ses_"

/**
 * Render a session id for the message prefix as its bare ULID: strip exactly one
 * leading `ses_` when present, otherwise show the id untouched. The stored
 * `sessionID` field keeps the full branded value.
 */
const displaySessionID = (sessionID: string): string =>
  sessionID.startsWith(SESSION_ID_PREFIX) ? sessionID.slice(SESSION_ID_PREFIX.length) : sessionID

/** Read the first non-empty string session id from the structured fields. */
const sessionIdOf = (fields: Record<string, unknown>): string | undefined => {
  for (const key of SESSION_ID_KEYS) {
    const value = fields[key]
    if (typeof value === "string" && value.length > 0) return value
  }
  return undefined
}

/**
 * Hoist the session id into the message prefix: `session workers[<bare id>]: <event>`.
 * Without a session id the message is returned untouched. If a caller passes a
 * bare event that does not already carry `WORKER_LOG_PREFIX`, the new prefix is
 * prepended defensively so the log anchor is always present.
 */
const withSessionPrefix = (message: string, sessionID: string | undefined): string => {
  if (sessionID === undefined) return message
  const prefix = `${WORKER_LOG_LABEL}[${displaySessionID(sessionID)}]: `
  if (!message.startsWith(WORKER_LOG_PREFIX)) return `${prefix}${message}`
  return `${prefix}${message.slice(WORKER_LOG_PREFIX.length)}`
}

export const workerLoggerFromBridge =
  (bridge: EffectBridge.Shape): WorkerLogger =>
  (message, fields, level) => {
    const resolved = parseWorkerLogLevel(level ?? fields.level)
    const prefixed = withSessionPrefix(message, sessionIdOf(fields))
    void bridge.fork(logAtLevel(resolved, prefixed, withoutLevelField(fields)))
  }

export * as WorkerLog from "./log"
