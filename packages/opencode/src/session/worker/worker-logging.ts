// Worker log forwarding: the single user-visible logging path for the
// session-worker subsystem.
//
// Every emission site calls `Effect.logInfo` / `Effect.logWarning` /
// `Effect.logError`. The forwarding logger below turns those structured events
// into the operator-facing `session workers[<session>]: <event>` line:
//
//   * the session id is read from the log annotation attached at the
//     per-session/per-worker fiber boundary (`Effect.annotateLogs`), with the
//     structured `sessionID` field kept as the emission-site fallback,
//   * the branded `ses_` prefix is stripped from the displayed id but kept in
//     the forwarded field,
//   * the upstream sink (the main `opencode.log` file, or a test recorder)
//     receives the unchanged log level and every field.
//
// The worker isolate owns no logger, so `workerLoggingLayer` attaches the same
// file sink the main process uses, scoped to the worker's run id.

import { Effect, Layer, Logger, References, type LogLevel } from "effect"
import { NodeFileSystem } from "@effect/platform-node"
import { Logging } from "@opencode-ai/core/observability/logging"

const WORKER_LOG_LABEL = "session workers"

export const WORKER_LOG_PREFIX = `${WORKER_LOG_LABEL}: `

export const workerLogMessage = (event: string): string => `${WORKER_LOG_PREFIX}${event}`

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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

/** Read the first non-empty string session id from a structured record. */
const sessionIdOf = (fields: Record<string, unknown>): string | undefined => {
  for (const key of SESSION_ID_KEYS) {
    const value = fields[key]
    if (typeof value === "string" && value.length > 0) return value
  }
  return undefined
}

/**
 * Split an Effect log message into its human-readable head and the structured
 * fields. `Effect.logInfo(message, fields)` encodes the event as
 * `[message, fields]`; a bare message stays a scalar.
 */
const splitMessage = (message: unknown): { readonly head: unknown; readonly fields: Record<string, unknown> | undefined } => {
  if (!Array.isArray(message)) return { head: message, fields: undefined }
  const [head, fields] = message
  return { head, fields: isRecord(fields) ? fields : undefined }
}

/**
 * Hoist the session id into the message prefix: `session workers[<bare id>]: <event>`.
 * A bare event gains the base `session workers: ` prefix; an event that already
 * carries it only gains the id. Without a session id the base prefix is kept.
 */
const withSessionPrefix = (message: unknown, sessionID: string | undefined): unknown => {
  if (typeof message !== "string") return message
  const body = message.startsWith(WORKER_LOG_PREFIX) ? message.slice(WORKER_LOG_PREFIX.length) : message
  if (sessionID === undefined) return `${WORKER_LOG_PREFIX}${body}`
  return `${WORKER_LOG_LABEL}[${displaySessionID(sessionID)}]: ${body}`
}

/** Drop the `level` key so severity is not duplicated in the emitted fields. */
const withoutLevelField = (fields: Record<string, unknown>): Record<string, unknown> => {
  if (!("level" in fields)) return fields
  const { level: _severity, ...rest } = fields
  return rest
}

/** Re-render one log event's message with the worker prefix and session id. */
const renderWorkerMessage = (options: Logger.Options<unknown>): unknown => {
  const { head, fields } = splitMessage(options.message)
  const annotations = options.fiber.getRef(References.CurrentLogAnnotations)
  const sessionID = (fields === undefined ? undefined : sessionIdOf(fields)) ?? sessionIdOf(annotations)
  const prefixed = withSessionPrefix(head, sessionID)
  if (!Array.isArray(options.message)) return prefixed
  return [prefixed, ...options.message.slice(1).map((value) => (isRecord(value) ? withoutLevelField(value) : value))]
}

/**
 * Wrap a sink so every event is rendered in the worker's user-visible format
 * before forwarding. The sink keeps its own level handling and field rendering;
 * only the message prefix is rewritten here.
 */
export function forwardingLogger(sink: Logger.Logger<unknown, void>): Logger.Logger<unknown, void> {
  return Logger.make((options) => sink.log({ ...options, message: renderWorkerMessage(options) }))
}

/**
 * A layer installing the forwarding wrapper around already-built loggers (the
 * ambient hub loggers or a test recorder). This is the single forwarding path:
 * the session id is read from the annotation attached at the fiber boundary.
 */
export function forwardingLayer(
  loggers: Iterable<Logger.Logger<unknown, unknown>>,
  minimumLogLevel?: LogLevel.LogLevel,
): Layer.Layer<never> {
  const forwards = [...loggers].map((logger) => forwardingLogger(logger as Logger.Logger<unknown, void>))
  const layer = Logger.layer(forwards, { mergeWithExisting: false })
  return minimumLogLevel === undefined
    ? layer
    : layer.pipe(Layer.merge(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)))
}

export interface WorkerLoggingOptions {
  /** Sink file; defaults to the main `opencode.log`. */
  readonly file?: string
  /** Run id written into every line; defaults to the ambient main-process run id. */
  readonly id?: string
}

/**
 * The worker-local logging layer: the main file sink plus the
 * `OPENCODE_LOG_LEVEL` threshold, wrapped in the worker forwarding format.
 * Fail-soft — a sink that cannot build degrades to a logger-less layer rather
 * than breaking the run it observes.
 */
export function workerLoggingLayer(options: WorkerLoggingOptions = {}): Layer.Layer<never> {
  const forwarding = Effect.map(Logging.fileLogger(options.file, options.id), (sink) => forwardingLogger(sink))
  return Logger.layer([forwarding], { mergeWithExisting: false }).pipe(
    Layer.provide(NodeFileSystem.layer),
    Layer.merge(Layer.succeed(References.MinimumLogLevel, Logging.minimumLogLevel())),
    // Effect v4 has no `Layer.orElse`; `Layer.catchCause` is the fail-soft
    // combinator — any build failure (typed error or defect) recovers to the
    // logger-less `Layer.empty`, so a broken sink never breaks a run.
    Layer.catchCause(() => Layer.empty),
  )
}

export * as WorkerLogging from "./worker-logging"
