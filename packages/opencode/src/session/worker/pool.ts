// Phase 4.2 — session worker pool core (plan task 4.2).
//
// Hub-side pool that spawns K production bootstrap workers. K is a single
// worker below 4 cores, otherwise `availableParallelism() − 2`, so the hub, the
// single writer and the broker keep their cores (plan decision line 22). The
// count is overridable with `OPENCODE_SESSION_WORKER_COUNT`.
//
// Dispatch/scheduling is intentionally absent (task 4.3); this module owns only
// sizing and the worker lifecycle.

import os from "node:os"
import type { BootstrapInbound, BootstrapOutbound } from "./bootstrap"
import { mergeEnv } from "@/session/broker/env"
import { parsePositiveInt } from "@/util/parse"

export const WORKER_COUNT_ENV = "OPENCODE_SESSION_WORKER_COUNT"

const CORES_REQUIRED_FOR_MULTIPLE_WORKERS = 4
const RESERVED_CORES = 2
const DEFAULT_READY_TIMEOUT_MS = 10_000

export type Env = Record<string, string | undefined>

export interface WorkerCountOptions {
  /** Injected parallelism; defaults to `os.availableParallelism`. */
  readonly availableParallelism?: () => number
  /** Environment source for the override; defaults to `process.env`. */
  readonly env?: Env
}

/**
 * Resolve the pool size: `OPENCODE_SESSION_WORKER_COUNT` when set (positive
 * integer), otherwise `deriveWorkerCount(availableParallelism())`.
 */
export function resolveWorkerCount(options: WorkerCountOptions = {}): number {
  const override = parseWorkerCountOverride((options.env ?? process.env)[WORKER_COUNT_ENV])
  if (override !== undefined) return override
  return deriveWorkerCount(readParallelism(options.availableParallelism))
}

/** A single worker below 4 cores, otherwise `cores − 2`. */
export function deriveWorkerCount(cores: number): number {
  parsePositiveInt(cores, "availableParallelism")
  if (cores < CORES_REQUIRED_FOR_MULTIPLE_WORKERS) return 1
  return Math.max(1, cores - RESERVED_CORES)
}

function parseWorkerCountOverride(raw: string | undefined): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined
  return parsePositiveInt(raw, WORKER_COUNT_ENV)
}

function readParallelism(availableParallelism?: () => number): number {
  const detected = (availableParallelism ?? os.availableParallelism.bind(os))()
  return parsePositiveInt(detected, "availableParallelism")
}

export interface PoolWorker extends AsyncDisposable {
  readonly id: number
  /** Worker thread id; `0` until the worker reports ready. */
  readonly threadId: number
  readonly alive: boolean
  /** Resolve once the worker has booted and reported ready. */
  ready(): Promise<void>
  /** Run the bootstrap's trivial Effect and resolve with its value. */
  run(value: number): Promise<number>
  /** Terminate the worker; resolves with its exit code. */
  terminate(code?: number): Promise<number>
}

export interface WorkerPool extends AsyncDisposable {
  readonly size: number
  readonly workers: ReadonlyArray<PoolWorker>
  /** Resolve once every pooled worker is ready. */
  ready(): Promise<void>
  terminateAll(): Promise<void>
  dispose(): Promise<void>
}

export interface WorkerPoolOptions {
  readonly availableParallelism?: () => number
  readonly env?: Env
  /** Worker entry point; defaults to the production bootstrap. */
  readonly entry?: string | URL
  /** Explicit pool size; overrides sizing when provided. */
  readonly size?: number
  /** Readiness/run timeout in ms; defaults to 10000. */
  readonly timeoutMs?: number
}

declare global {
  /**
   * Compile-time path to the bundled pool bootstrap isolate, injected by
   * `script/build.ts` (mirrors `OPENCODE_SESSION_WORKER_PATH`). Absent in dev/test.
   */
  const OPENCODE_POOL_WORKER_PATH: string | undefined
}

/**
 * Resolve the pool bootstrap entry: the runtime env override, else the build-time
 * define, else the source-relative isolate. The packaged binary cannot resolve
 * the source path (`new URL("./bootstrap.ts", import.meta.url)` becomes
 * `/$bunfs/root/bootstrap.ts`), so this mirrors `resolveSessionWorkerEntry` to
 * load the bundled worker. Pure so the resolution is testable without
 * re-importing.
 */
export function resolvePoolWorkerEntry(
  options: { readonly env?: Record<string, string | undefined>; readonly definedPath?: string } = {},
): string | URL {
  const defined =
    options.definedPath ?? (typeof OPENCODE_POOL_WORKER_PATH !== "undefined" ? OPENCODE_POOL_WORKER_PATH : undefined)
  const fromEnv = (options.env ?? process.env).OPENCODE_POOL_WORKER_PATH
  const resolved = fromEnv ?? defined
  if (resolved === undefined || resolved === "") return new URL("./bootstrap.ts", import.meta.url)
  return resolved
}

const DEFAULT_ENTRY = resolvePoolWorkerEntry()

interface PendingRun {
  readonly resolve: (value: number) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

/**
 * Build the error a broken isolate rejects its callers with. The runtime's real
 * `message` is always preserved; a runtime-provided `cause` (an `ErrorEvent`'s
 * `error`, carrying the original stack) is attached so the failure stays
 * actionable instead of collapsing into a bare "worker N error".
 */
export function describeWorkerFailure(id: number, message: string, cause?: unknown): Error {
  const text = `worker ${id} error: ${message}`
  return cause === undefined ? new Error(text) : new Error(text, { cause })
}

function createPoolWorker(id: number, entry: string | URL, env: Record<string, string>, timeoutMs: number): PoolWorker {
  const worker = new Worker(entry, { env })
  const pending = new Map<number, PendingRun>()
  let threadId = 0
  let alive = true
  let nextRunId = 1
  let exitCode: number | undefined

  const rememberExitCode = (result: unknown): void => {
    if (typeof result === "number") exitCode = result
  }

  /** Terminate the isolate, capturing whatever exit code the runtime returns. */
  const terminateWorker = async (): Promise<void> => {
    rememberExitCode(await worker.terminate())
  }

  let settleReady!: () => void
  let failReady!: (error: Error) => void
  const readyPromise = new Promise<void>((resolve, reject) => {
    settleReady = resolve
    failReady = reject
  })
  const readyTimer = setTimeout(
    () => failReady(new Error(`worker ${id} did not become ready within ${timeoutMs}ms`)),
    timeoutMs,
  )
  // Lifecycle promise: readiness failures surface through `ready()` and
  // `worker.onerror`; suppress the unhandled-rejection noise when a caller never
  // observes `ready()`.
  readyPromise.catch(() => undefined)

  const failAll = (error: Error): void => {
    for (const run of pending.values()) {
      clearTimeout(run.timer)
      run.reject(error)
    }
    pending.clear()
  }

  const shutdown = (error: Error): void => {
    if (!alive) return
    alive = false
    clearTimeout(readyTimer)
    failReady(error)
    failAll(error)
    // A worker that errored must not linger: stop the isolate and remember its
    // exit code so a later `terminate()` reports the real one.
    void terminateWorker().catch(() => undefined)
  }

  worker.onmessage = (event: MessageEvent) => {
    const message = event.data as BootstrapOutbound
    switch (message.type) {
      case "ready":
        clearTimeout(readyTimer)
        threadId = message.threadId
        settleReady()
        return
      case "value": {
        const run = pending.get(message.id)
        if (!run) return
        pending.delete(message.id)
        clearTimeout(run.timer)
        run.resolve(message.value)
        return
      }
      case "error": {
        if (message.id === undefined) return
        const run = pending.get(message.id)
        if (!run) return
        pending.delete(message.id)
        clearTimeout(run.timer)
        run.reject(new Error(message.message))
        return
      }
    }
  }

  worker.onerror = (event: ErrorEvent) => shutdown(describeWorkerFailure(id, event.message, event.error))

  const run = async (value: number): Promise<number> => {
    await readyPromise
    if (!alive) throw new Error(`cannot run on terminated worker ${id}`)
    const requestId = nextRunId++
    return new Promise<number>((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(requestId)
        reject(new Error(`worker ${id} run ${requestId} timed out after ${timeoutMs}ms`))
      }, timeoutMs)
      pending.set(requestId, { resolve, reject, timer })
      worker.postMessage({ type: "run", id: requestId, value } satisfies BootstrapInbound)
    })
  }

  const terminate = async (code = 0): Promise<number> => {
    if (!alive) return exitCode ?? code
    alive = false
    clearTimeout(readyTimer)
    failReady(new Error(`worker ${id} terminated`))
    failAll(new Error(`worker ${id} terminated`))
    await terminateWorker()
    // Fall back to the requested code when the runtime reports no exit code, so
    // the code is stable (and idempotent) for every later terminate call.
    if (exitCode === undefined) exitCode = code
    return exitCode
  }

  return {
    id,
    get threadId() {
      return threadId
    },
    get alive() {
      return alive
    },
    ready: () => readyPromise,
    run,
    terminate,
    [Symbol.asyncDispose]: () => terminate().then(() => undefined),
  }
}

export interface SpawnWorkerOptions {
  readonly id?: number
  readonly entry?: string | URL
  readonly env?: Env
  readonly timeoutMs?: number
}

/**
 * Spawn a single production bootstrap worker. Used by the dispatcher's
 * production adapter for on-demand headroom and crash respawn, where the fixed
 * `createWorkerPool` set is not enough.
 */
export function spawnPoolWorker(options: SpawnWorkerOptions = {}): PoolWorker {
  return createPoolWorker(
    options.id ?? 1,
    options.entry ?? DEFAULT_ENTRY,
    mergeEnv(options.env),
    options.timeoutMs ?? DEFAULT_READY_TIMEOUT_MS,
  )
}

export function createWorkerPool(options: WorkerPoolOptions = {}): WorkerPool {
  const size = parsePositiveInt(options.size ?? resolveWorkerCount(options), "worker pool size")
  const entry = options.entry ?? DEFAULT_ENTRY
  const env = mergeEnv(options.env)
  const timeoutMs = options.timeoutMs ?? DEFAULT_READY_TIMEOUT_MS
  const workers = Array.from({ length: size }, (_, index) => createPoolWorker(index + 1, entry, env, timeoutMs))

  const terminateAll = async (): Promise<void> => {
    await Promise.all(workers.map((worker) => worker.terminate()))
  }

  return {
    size,
    get workers() {
      return workers.slice()
    },
    ready: () => Promise.all(workers.map((worker) => worker.ready())).then(() => undefined),
    terminateAll,
    dispose: terminateAll,
    [Symbol.asyncDispose]: terminateAll,
  }
}

export * as WorkerPoolModule from "./pool"
