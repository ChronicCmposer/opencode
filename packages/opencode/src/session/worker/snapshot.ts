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

import { Effect, Layer } from "effect"
import { ModelsDev } from "@opencode-ai/core/models-dev"

import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceState } from "@/effect/instance-state"
import { Snapshot } from "@/snapshot"

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
  /**
   * Explicit snapshot epoch. When supplied the build is pure/idempotent for that
   * input (same options => same epoch), which lets a caller own the sequence.
   * When absent the epoch is derived monotonically from the previous build and
   * the clock, so a generation never regresses.
   */
  readonly epoch?: number
}

/** The last derived epoch, so the default sequence stays strictly monotonic. */
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
  const ctx = yield* InstanceState.context

  const [configInfo, authInfo, models] = yield* Effect.all([config.get(), auth.all(), modelsDev.get()])
  const directories = yield* config.directories()

  const now = (options.clock ?? (() => Date.now()))()
  const epoch = options.epoch ?? nextEpoch(lastEpoch, now)
  // Keep the derived sequence monotonic even after an explicit epoch is used.
  lastEpoch = Math.max(lastEpoch, epoch)

  return {
    epoch,
    directory: ctx.directory,
    // The hub's config roots, so a worker scans the same directories the hub
    // would; without them a worker's config discovery would diverge.
    directories,
    // Ship the hub project's identity so the worker's ambient `InstanceRef` is
    // real, not the old `project: undefined as never` placeholder. `project` is
    // typed non-optional but the hub's fallback context leaves it undefined at
    // runtime, so the optional chain and conditional spreads keep the snapshot
    // honest (and JSON-serializable) in that degraded case.
    ...(ctx.project?.id === undefined ? {} : { projectId: ctx.project.id }),
    ...(ctx.project?.vcs === undefined ? {} : { projectVcs: ctx.project.vcs }),
    worktree: ctx.worktree,
    config: configInfo,
    auth: authInfo,
    modelsDev: models,
    flags,
    workerCount: options.workerCount ?? resolveWorkerCount(),
  } satisfies WorkerSnapshot
})

/**
 * A worker-appropriate `Snapshot` boundary.
 *
 * The worker isolate owns no OS child-process authority — `AppProcess` is a
 * hub-owned node replaced by a mock in the worker graph — so the real `Snapshot`
 * service dies with `UnimplementedError` as soon as a git-backed project tracks a
 * turn. This boundary degrades snapshot tracking to a fail-soft no-op: the session
 * completes, and snapshot/revert is simply unavailable inside the isolate (the hub
 * remains the snapshot authority).
 */
export const workerSnapshotBoundaryLayer = (): Layer.Layer<Snapshot.Service> =>
  Layer.succeed(Snapshot.Service, {
    init: () => Effect.void,
    cleanup: () => Effect.void,
    track: () => Effect.succeed(undefined),
    patch: () => Effect.succeed({ hash: "", files: [] }),
    restore: () => Effect.void,
    revert: () => Effect.void,
    diff: () => Effect.succeed(""),
    diffFull: () => Effect.succeed([]),
  })

export * as WorkerSnapshotModule from "./snapshot"
