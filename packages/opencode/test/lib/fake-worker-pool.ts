// Shared in-memory worker/pool fakes for the session-worker tests.
//
// Two seams are faked here, matching the two structural interfaces the
// scheduler and hub consume:
//
//   - `WorkerPool`       (dispatch.ts) — `FakeSessionWorker` / `FakePool`, plus
//                        the real-isolate `FaultPool` for the fault seam.
//   - `ExecutorWorkerPool` (execute.ts) — `FakeExecutorWorker` / `FakeExecutorPool`.
//
// No real threads are used by the in-memory fakes: a worker's `execute` outcome
// is driven by a mode. `cooperative` (the default) stores the run so the test
// resolves it by hand through `finish` / `crash` / `fail`; the other modes settle
// automatically. This keeps the scheduler tests deterministic while covering the
// full crash/failure taxonomy in one place.

import type { SessionID } from "@/session/schema"
import {
  SessionCancelledError,
  WorkerCrashedError,
  type SessionRun,
  type SessionWorker,
  type WorkerKind,
  type WorkerPool,
} from "@/session/worker/dispatch"
import type { CancelFrame, CancelPort } from "@/session/worker/cancel"
import {
  type ExecutorRun,
  type ExecutorWorker,
  type ExecutorWorkerKind,
  type ExecutorWorkerPool,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import type { HarnessWorker, WorkerHarness } from "./worker-harness"
import { faultWorkerEntry } from "./faults"

// ---------------------------------------------------------------------------
// dispatch.ts `WorkerPool` fakes
// ---------------------------------------------------------------------------

/** How a fake worker settles an `execute` call. */
export type FakeWorkerMode = "cooperative" | "finish" | "crash" | "fail"

/** How a worker's cancel port settles the in-flight run. */
export type FakeCancelOutcome = "resolve" | "reject" | "ignore"

export interface FakeSessionWorkerOptions {
  /** Defaults to `"cooperative"`. */
  readonly mode?: FakeWorkerMode
  /** Error a `fail`-mode worker rejects with; defaults to `new Error("fake session failure")`. */
  readonly failure?: () => unknown
  /**
   * How the cancel port settles the run when a `cancel` frame arrives:
   * `"resolve"` unwinds cooperatively, `"reject"` fails with the
   * executor-machine error, `"ignore"` leaves the run running (grace expiry).
   * Defaults to `"resolve"`.
   */
  readonly onCancel?: FakeCancelOutcome
  /** Whether the cancel port reports a matching `cancel-ack`. Defaults to `true`. */
  readonly acknowledge?: boolean
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
  readonly cancelPort: CancelPort
  private readonly pending = new Map<SessionID, PendingRun>()
  private readonly failure: () => unknown
  private readonly onCancel: FakeCancelOutcome
  private readonly acknowledge: boolean
  private readonly ackHandlers = new Set<(frame: CancelFrame) => void>()

  constructor(
    readonly id: number,
    options: FakeSessionWorkerOptions = {},
  ) {
    this.mode = options.mode ?? "cooperative"
    this.failure = options.failure ?? (() => new Error("fake session failure"))
    this.onCancel = options.onCancel ?? "resolve"
    this.acknowledge = options.acknowledge ?? true
    this.cancelPort = {
      notify: (frame) => {
        if (frame.type !== "cancel") return
        if (this.acknowledge) {
          for (const handler of [...this.ackHandlers]) {
            handler({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
          }
        }
        this.settleCancel(frame.sessionID as SessionID)
      },
      onMessage: (handler) => {
        this.ackHandlers.add(handler)
        return () => void this.ackHandlers.delete(handler)
      },
    }
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

  /** Kill the worker with an arbitrary isolate error (a dead isolate, not a typed crash). */
  crashWith(sessionID: SessionID, error: unknown): void {
    this.alive = false
    const pending = this.take(sessionID)
    pending?.reject(error)
  }

  async terminate(): Promise<void> {
    this.alive = false
    const pending = [...this.pending.entries()]
    this.pending.clear()
    for (const [sessionID, run] of pending) {
      run.reject(new SessionCancelledError({ message: "terminated", sessionID }))
    }
  }

  private take(sessionID: SessionID): PendingRun | undefined {
    const pending = this.pending.get(sessionID)
    this.pending.delete(sessionID)
    return pending
  }

  private settleCancel(sessionID: SessionID): void {
    if (this.onCancel === "ignore") return
    const pending = this.take(sessionID)
    if (pending === undefined) return
    if (this.onCancel === "resolve") pending.resolve()
    else pending.reject(new SessionCancelledError({ message: "executor machine interrupted", sessionID }))
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
  /** Cancel-port settle behaviour applied to every worker. */
  readonly onCancel?: FakeCancelOutcome
  /** Whether every worker acknowledges cancels; defaults to `true`. */
  readonly acknowledge?: boolean
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
        onCancel: options.onCancel,
        acknowledge: options.acknowledge,
      }),
    )
    this.nextSpawnId = options.firstSpawnId ?? this.workers.length + 1
  }

  spawn(kind: WorkerKind): SessionWorker {
    const id = this.nextSpawnId++
    const worker = new FakeSessionWorker(id, {
      mode: this.options.modeFor?.({ id, kind, initial: false }) ?? this.options.mode,
      failure: this.options.failure,
      onCancel: this.options.onCancel,
      acknowledge: this.options.acknowledge,
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

// ---------------------------------------------------------------------------
// Fault-injection pool — a real isolate crashed through the OPENCODE_TEST_FAULTS
// seam. It is not an in-memory fake: it proves the crash taxonomy end-to-end.
// ---------------------------------------------------------------------------

type FaultWorkerOut = { type: "ready" } | { type: "crashing"; point: string } | { type: "survived"; point: string }
type FaultWorkerIn = { type: "crash"; point: string }

const FAULT_POINT = "dispatch:run"

/** A `SessionWorker` backed by the real `test/fixture/fault-worker.ts` isolate. */
class FaultWorker implements SessionWorker {
  constructor(
    private readonly handle: HarnessWorker<FaultWorkerOut, FaultWorkerIn>,
    readonly id: number,
  ) {}

  get alive(): boolean {
    return this.handle.alive
  }

  async execute(_run: SessionRun): Promise<void> {
    await this.handle.waitFor("ready")
    this.handle.post({ type: "crash", point: FAULT_POINT })
    await this.handle.waitFor("crashing")
    throw new WorkerCrashedError({ message: `worker ${this.id} crashed via the fault seam`, workerID: this.id })
  }

  async terminate(): Promise<void> {
    await this.handle.terminate()
  }
}

/** A `WorkerPool` whose workers are real isolates crashed at the fault point. */
export class FaultPool implements WorkerPool {
  readonly workers: FaultWorker[]
  readonly spawned: FaultWorker[] = []

  constructor(
    private readonly harness: WorkerHarness,
    k: number,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => this.make(index + 1))
  }

  private make(id: number): FaultWorker {
    return new FaultWorker(this.harness.spawn<FaultWorkerOut, FaultWorkerIn>(faultWorkerEntry), id)
  }

  spawn(): SessionWorker {
    const worker = this.make(100 + this.spawned.length)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}
}

// ---------------------------------------------------------------------------
// execute.ts `ExecutorWorkerPool` fakes
// ---------------------------------------------------------------------------

/** How a fake executor worker settles an `execute` call. */
export type FakeExecutorMode = "auto" | "blocking"

export interface FakeExecutorWorkerOptions {
  /** `"auto"` records the result immediately; `"blocking"` waits for a cancel. Defaults to `"auto"`. */
  readonly mode?: FakeExecutorMode
  /** Result value recorded for each run; defaults to `undefined`. */
  readonly value?: unknown
  /** Called with the session id as a cancel is observed, before the ack. */
  readonly onCancel?: (sessionID: SessionID) => void
}

/**
 * An in-memory `ExecutorWorker`. `auto` workers complete (and record a result)
 * the moment they are executed; `blocking` workers stay busy until cancelled,
 * which is how the hub's cooperative cancel path is exercised. Either way the
 * worker exposes a `cancelPort` that acknowledges and unwinds its run.
 */
export class FakeExecutorWorker implements ExecutorWorker {
  readonly id: number
  readonly cancelPort: CancelPort
  private readonly mode: FakeExecutorMode
  private readonly value: unknown
  private readonly onCancel?: (sessionID: SessionID) => void
  private readonly results = new Map<SessionID, SessionExecutionResult>()
  private readonly pending = new Map<SessionID, () => void>()
  private readonly ackHandlers = new Set<(frame: CancelFrame) => void>()

  constructor(id: number, options: FakeExecutorWorkerOptions = {}) {
    this.id = id
    this.mode = options.mode ?? "auto"
    this.value = options.value
    this.onCancel = options.onCancel
    this.cancelPort = {
      notify: (frame) => {
        if (frame.type !== "cancel") return
        this.onCancel?.(frame.sessionID as SessionID)
        for (const handler of [...this.ackHandlers]) {
          handler({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
        }
        this.settle(frame.sessionID as SessionID)
      },
      onMessage: (handler) => {
        this.ackHandlers.add(handler)
        return () => void this.ackHandlers.delete(handler)
      },
    }
  }

  get alive(): boolean {
    return true
  }

  execute(run: ExecutorRun): Promise<void> {
    if (this.mode === "auto") {
      this.record(run.sessionID)
      return Promise.resolve()
    }
    return new Promise<void>((resolve) => {
      this.pending.set(run.sessionID, () => {
        this.record(run.sessionID)
        resolve()
      })
    })
  }

  async cancel(sessionID: SessionID): Promise<void> {
    this.settle(sessionID)
  }

  async assertNotBusy(sessionID: SessionID): Promise<boolean> {
    return this.pending.has(sessionID)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }

  async terminate(): Promise<void> {}

  private record(sessionID: SessionID): void {
    this.results.set(sessionID, { sessionID, value: this.value })
  }

  private settle(sessionID: SessionID): void {
    const pending = this.pending.get(sessionID)
    if (pending === undefined) return
    this.pending.delete(sessionID)
    pending()
  }
}

export interface FakeExecutorPoolOptions {
  /** Mode applied to every worker; defaults to `"auto"`. */
  readonly mode?: FakeExecutorMode
  /** Result value recorded by every worker. */
  readonly value?: unknown
  /** Cancel callback applied to every worker. */
  readonly onCancel?: (sessionID: SessionID) => void
  /** Id assigned to the first spawned worker; defaults to `workers.length + 1`. */
  readonly firstSpawnId?: number
}

/** An in-memory `ExecutorWorkerPool`: K workers plus a spawn log. */
export class FakeExecutorPool implements ExecutorWorkerPool {
  readonly workers: FakeExecutorWorker[]
  readonly spawned: FakeExecutorWorker[] = []
  private readonly workerOptions: FakeExecutorWorkerOptions
  private nextSpawnId: number

  constructor(k: number, options: FakeExecutorPoolOptions = {}) {
    if (!Number.isInteger(k) || k < 1) {
      throw new Error(`invalid worker count: ${k} (expected a positive integer)`)
    }
    const { firstSpawnId, ...workerOptions } = options
    this.workerOptions = workerOptions
    this.workers = Array.from({ length: k }, (_, index) => new FakeExecutorWorker(index + 1, workerOptions))
    this.nextSpawnId = firstSpawnId ?? this.workers.length + 1
  }

  spawn(_kind: ExecutorWorkerKind): FakeExecutorWorker {
    const worker = new FakeExecutorWorker(this.nextSpawnId++, this.workerOptions)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {}

  byID(id: number): ExecutorWorker | undefined {
    return [...this.workers, ...this.spawned].find((worker) => worker.id === id)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    for (const worker of [...this.workers, ...this.spawned]) {
      const result = worker.resultFor(sessionID)
      if (result !== undefined) return result
    }
    return undefined
  }
}

/** Convenience factory for {@link FakeExecutorPool}. */
export function createFakeExecutorPool(k: number, options: FakeExecutorPoolOptions = {}): FakeExecutorPool {
  return new FakeExecutorPool(k, options)
}
