// Parameterized in-memory `WorkerPool` fake (extracted from the duplicated
// `FakeWorker`/`FakePool` in the dispatch/subagent/crash-cascade tests).
//
// No real threads: the pool hands out {@link FakeSessionWorker}s whose `execute`
// outcome is driven by a mode. `cooperative` (the default) stores the run so the
// test resolves it by hand through `finish` / `crash` / `fail`; the other three
// modes settle automatically. This keeps the scheduler tests deterministic while
// covering the full crash/failure taxonomy in one place.

import type { SessionID } from "@/session/schema"
import {
  SessionCancelledError,
  WorkerCrashedError,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"

/** How a fake worker settles an `execute` call. */
export type FakeWorkerMode = "cooperative" | "finish" | "crash" | "fail"

export interface FakeSessionWorkerOptions {
  /** Defaults to `"cooperative"`. */
  readonly mode?: FakeWorkerMode
  /** Error a `fail`-mode worker rejects with; defaults to `new Error("fake session failure")`. */
  readonly failure?: () => unknown
}

interface PendingRun {
  readonly resolve: () => void
  readonly reject: (error: unknown) => void
}

/** A worker whose runs the test completes/crashes/fails explicitly or by mode. */
export class FakeSessionWorker implements SessionWorker {
  alive = true
  readonly runs: SessionRun[] = []
  readonly mode: FakeWorkerMode
  private readonly pending = new Map<SessionID, PendingRun>()
  private readonly failure: () => unknown

  constructor(
    readonly id: number,
    options: FakeSessionWorkerOptions = {},
  ) {
    this.mode = options.mode ?? "cooperative"
    this.failure = options.failure ?? (() => new Error("fake session failure"))
  }

  /** Runs currently awaiting an explicit settle (only in `cooperative` mode). */
  get pendingCount(): number {
    return this.pending.size
  }

  execute(run: SessionRun): Promise<void> {
    this.runs.push(run)
    switch (this.mode) {
      case "finish":
        return Promise.resolve()
      case "crash":
        this.alive = false
        return Promise.reject(this.crashError())
      case "fail":
        return Promise.reject(this.failure())
      case "cooperative":
        return new Promise<void>((resolve, reject) => {
          this.pending.set(run.sessionID, { resolve, reject })
        })
    }
  }

  /** Resolve a pending run (no-op if the session is not pending). */
  finish(sessionID: SessionID): void {
    const pending = this.take(sessionID)
    pending?.resolve()
  }

  /** Crash the worker: mark it dead and reject its pending run. */
  crash(sessionID: SessionID): void {
    this.alive = false
    const pending = this.take(sessionID)
    pending?.reject(this.crashError())
  }

  /** Fail the session but keep the worker alive (a session-level failure). */
  fail(sessionID: SessionID, error: unknown): void {
    const pending = this.take(sessionID)
    pending?.reject(error)
  }

  async terminate(): Promise<void> {
    this.alive = false
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new SessionCancelledError({ message: "terminated", sessionID: "" }))
  }

  private take(sessionID: SessionID): PendingRun | undefined {
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    return pending
  }

  private crashError(): WorkerCrashedError {
    return new WorkerCrashedError({ message: `worker ${this.id} crashed`, workerID: this.id })
  }
}

export interface FakeWorkerPoolOptions {
  /** Mode applied to every worker; defaults to `"cooperative"`. */
  readonly mode?: FakeWorkerMode
  /** Per-worker mode override, given the worker's id, kind and whether it was initial. */
  readonly modeFor?: (context: { readonly id: number; readonly kind: WorkerKind; readonly initial: boolean }) => FakeWorkerMode
  /** Failure factory forwarded to `fail`-mode workers. */
  readonly failure?: () => unknown
  /** Id assigned to the first spawned worker; defaults to `workers.length + 1`. */
  readonly firstSpawnId?: number
}

/** An in-memory `WorkerPool`: K initial workers plus a spawn log. */
export class FakePool implements WorkerPool {
  readonly workers: FakeSessionWorker[]
  readonly spawned: FakeSessionWorker[] = []
  private readonly options: FakeWorkerPoolOptions
  private nextSpawnId: number

  constructor(k: number, options: FakeWorkerPoolOptions = {}) {
    if (!Number.isInteger(k) || k < 1) {
      throw new Error(`invalid worker count: ${k} (expected a positive integer)`)
    }
    this.options = options
    this.workers = Array.from({ length: k }, (_, index) =>
      new FakeSessionWorker(index + 1, {
        mode: options.modeFor?.({ id: index + 1, kind: "foreground", initial: true }) ?? options.mode,
        failure: options.failure,
      }),
    )
    this.nextSpawnId = options.firstSpawnId ?? this.workers.length + 1
  }

  spawn(kind: WorkerKind): SessionWorker {
    const id = this.nextSpawnId++
    const worker = new FakeSessionWorker(id, {
      mode: this.options.modeFor?.({ id, kind, initial: false }) ?? this.options.mode,
      failure: this.options.failure,
    })
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {
    await Promise.all(this.workers.map((worker) => worker.terminate()))
  }
}

/** Convenience factory for {@link FakePool}. */
export function createFakeWorkerPool(k: number, options: FakeWorkerPoolOptions = {}): FakePool {
  return new FakePool(k, options)
}

/** Cast a fake pool's typed worker list to the `WorkerPool` view. */
export function fakeWorkers(pool: FakePool): ReadonlyArray<FakeSessionWorker> {
  return pool.workers
}
