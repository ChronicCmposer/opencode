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

import { Effect, Layer, Option } from "effect"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { RipgrepBinary } from "@opencode-ai/core/ripgrep/binary"
import { Project } from "@opencode-ai/schema/project"

import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceState } from "@/effect/instance-state"
import type { InstanceContext } from "@/project/instance-context"
import { Snapshot } from "@/snapshot"

import type { WorkerSnapshot } from "./layer"
import { resolveWorkerCount } from "./pool"

/**
 * Build an {@link InstanceContext} from the minimum identity an isolate or a hub
 * can supply. Every worker/hub instance-context builder funnels through here, so
 * the `project` shape (id, worktree, zero time span, no sandboxes) has exactly
 * one definition instead of three drifting copies.
 */
export interface InstanceContextInput {
  /** The instance working directory (required). */
  readonly directory: string
  /** The worktree; defaults to `directory` when absent (never persisted). */
  readonly worktree?: string
  /** The project id; callers pass their own fallback (e.g. the global project). */
  readonly projectID: string
  /** The project's VCS, when known. */
  readonly projectVcs?: "git"
}

export function makeInstanceContext(input: InstanceContextInput): InstanceContext {
  const worktree = input.worktree ?? input.directory
  return {
    directory: input.directory,
    worktree,
    project: {
      id: Project.ID.make(input.projectID),
      worktree,
      ...(input.projectVcs === undefined ? {} : { vcs: input.projectVcs }),
      // `Project.Info` requires a time span and sandboxes; the readers only touch
      // `id`/`vcs`, so a zero span and no sandboxes is a faithful minimum.
      time: { created: 0, updated: 0 },
      sandboxes: [],
    },
  }
}

/**
 * Monotonic epoch step. Given the previously emitted epoch and the current
 * clock reading, the next epoch is strictly greater than both, so it never
 * regresses even if the clock does.
 */
export const nextEpoch = (previous: number, now: number): number => Math.max(previous + 1, now)

/**
 * An explicit holder for a snapshot's epoch sequence. The holder owns the last
 * emitted epoch, so two callers that use different holders never interfere:
 * passing one makes the sequence deterministic and testable, rather than relying
 * on hidden process-wide state.
 */
export interface EpochSequence {
  /** The next epoch for clock reading `now`, strictly greater than the last. */
  next(now: number): number
}

/** Create an independent epoch sequence, starting after `initial` (default 0). */
export const createEpochSequence = (initial = 0): EpochSequence => {
  let current = initial
  return {
    next(now) {
      current = nextEpoch(current, now)
      return current
    },
  }
}

export interface BuildWorkerSnapshotOptions {
  /** Explicit pool size; defaults to {@link resolveWorkerCount}. */
  readonly workerCount?: number
  /** Injected clock; defaults to `Date.now`. */
  readonly clock?: () => number
  /**
   * Explicit snapshot epoch. When supplied the build is pure/idempotent for that
   * input (same options => same epoch), which lets a caller own the sequence.
   * When absent the epoch comes from `epochSequence`, or from the clock alone so
   * no hidden process-wide state leaks between builds.
   */
  readonly epoch?: number
  /**
   * The epoch holder to advance. A hub that builds one snapshot per generation
   * owns a sequence here, so its epochs stay strictly monotonic; a caller that
   * omits both `epoch` and `epochSequence` gets a clock-derived, memoryless
   * epoch.
   */
  readonly epochSequence?: EpochSequence
}

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
  const epoch = options.epoch ?? (options.epochSequence ?? { next: (reading) => nextEpoch(0, reading) }).next(now)

  // Freeze the hub's resolved ripgrep path so the worker runs the same binary
  // instead of locating/downloading its own. Resolution is best-effort: a hub
  // without a resolvable binary still ships a snapshot, and the worker falls
  // back to a PATH-resolved `rg`. `serviceOption` keeps the build independent of
  // the hub graph, so a snapshot can be built from hub-authoritative services
  // alone.
  const binary = yield* Effect.serviceOption(RipgrepBinary.Service)
  const ripgrepPath = Option.isSome(binary) ? yield* binary.value.filepath.pipe(Effect.option) : Option.none<string>()

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
    ...(Option.isNone(ripgrepPath) ? {} : { ripgrepPath: ripgrepPath.value }),
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
