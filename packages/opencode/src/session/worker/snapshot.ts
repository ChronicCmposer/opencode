// Production producer for the immutable per-worker `WorkerSnapshot`
// (plan decisions 67/68).
//
// A session worker runs against a frozen copy of the hub's configuration: it
// never reads config files, `process.env` or the auth store on its own. The hub
// builds the snapshot once per generation and hands it to every worker through
// `workerData`, so all workers of a generation agree on one epoch.
//
// The epoch is monotonic: wall clocks can jump backwards, but a snapshot may
// never regress, otherwise a worker could adopt an "older" generation and keep
// stale-tolerant cache entries alive.

import { Effect } from "effect"
import { ModelsDev } from "@opencode-ai/core/models-dev"

import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceState } from "@/effect/instance-state"

import type { WorkerSnapshot } from "./layer"
import { resolveWorkerCount } from "./pool"

/**
 * Monotonic epoch step. Given the previously emitted epoch and the current
 * clock reading, the next epoch is strictly greater than both, so it never
 * regresses even if the clock does.
 */
export const nextEpoch = (previous: number, now: number): number => Math.max(previous + 1, now)

export interface BuildWorkerSnapshotOptions {
  /** Explicit pool size; defaults to {@link resolveWorkerCount}. */
  readonly workerCount?: number
  /** Injected clock; defaults to `Date.now`. */
  readonly clock?: () => number
}

let lastEpoch = 0

/**
 * Build the immutable snapshot the hub ships to its workers.
 *
 * Reads the hub-authoritative services (`Config`, `Auth`, `ModelsDev`,
 * `RuntimeFlags`) plus the active instance directory, and stamps it with a
 * monotonic epoch. The result is structured-clone friendly: plain data only.
 */
export const buildWorkerSnapshot = Effect.fn("WorkerSnapshot.build")(function* (
  options: BuildWorkerSnapshotOptions = {},
) {
  const config = yield* Config.Service
  const auth = yield* Auth.Service
  const modelsDev = yield* ModelsDev.Service
  const flags = yield* RuntimeFlags.Service
  const directory = yield* InstanceState.directory

  const [configInfo, authInfo, models] = yield* Effect.all([config.get(), auth.all(), modelsDev.get()])

  const now = (options.clock ?? (() => Date.now()))()
  const epoch = nextEpoch(lastEpoch, now)
  lastEpoch = epoch

  return {
    epoch,
    directory,
    config: configInfo,
    auth: authInfo,
    modelsDev: models,
    flags,
    workerCount: options.workerCount ?? resolveWorkerCount(),
  } satisfies WorkerSnapshot
})

export * as WorkerSnapshotModule from "./snapshot"
