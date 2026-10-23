// Worker log forwarding: route the session-worker isolate's Effect logs to the
// main `opencode.log` sink.
//
// The isolate owns no logger (each executor frame runs on the default runtime),
// so the worker's `Effect.log*` calls — the reduced `SessionPrompt` loop, the
// model-facing plugin hooks, and the run-failure path — vanish into the default
// no-op. This layer attaches the same file sink the main process uses
// (`Logging.fileLogger`), scoped to the worker's run id, so worker lines land
// beside the hub's in one `opencode.log`.

import { Layer, Logger, References } from "effect"
import { NodeFileSystem } from "@effect/platform-node"
import { Logging } from "@opencode-ai/core/observability/logging"

export interface WorkerLoggingOptions {
  /** Sink file; defaults to the main `opencode.log`. */
  readonly file?: string
  /** Run id written into every line; defaults to the ambient main-process run id. */
  readonly id?: string
}

/**
 * The worker-local logging layer: the main file sink plus the
 * `OPENCODE_LOG_LEVEL` threshold. Fail-soft — a sink that cannot build degrades
 * to a logger-less layer rather than breaking the run it observes.
 */
export function workerLoggingLayer(options: WorkerLoggingOptions = {}): Layer.Layer<never> {
  return Logger.layer([Logging.fileLogger(options.file, options.id)], { mergeWithExisting: false }).pipe(
    Layer.provide(NodeFileSystem.layer),
    Layer.merge(Layer.succeed(References.MinimumLogLevel, Logging.minimumLogLevel())),
    // Effect v4 has no `Layer.orElse`; `Layer.catchCause` is the fail-soft
    // combinator — any build failure (typed error or defect) recovers to the
    // logger-less `Layer.empty`, so a broken sink never breaks a run.
    Layer.catchCause(() => Layer.empty),
  )
}

export * as WorkerLogging from "./worker-logging"
