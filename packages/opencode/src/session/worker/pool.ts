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
  assertPositiveInteger(cores, "availableParallelism")
  if (cores < CORES_REQUIRED_FOR_MULTIPLE_WORKERS) return 1
  return Math.max(1, cores - RESERVED_CORES)
}

function parseWorkerCountOverride(raw: string | undefined): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`invalid ${WORKER_COUNT_ENV}: ${raw} (expected a positive integer)`)
  }
  return parsed
}

function readParallelism(availableParallelism?: () => number): number {
  const detected = (availableParallelism ?? os.availableParallelism.bind(os))()
  assertPositiveInteger(detected, "availableParallelism")
  return detected
}

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`invalid ${label}: ${value} (expected a positive integer)`)
  }
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

const DEFAULT_ENTRY = new URL("./bootstrap.ts", import.meta.url)

function mergeEnv(overlay?: Env): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) env[key] = value
  }
  for (const [key, value] of Object.entries(overlay ?? {})) {
    if (value === undefined) delete env[key]
    else env[key] = value
  }
  return env
}

interface PendingRun {
  readonly resolve: (value: number) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

function createPoolWorker(id: number, entry: string | URL, env: Record<string, string>, timeoutMs: number): PoolWorker {
  const worker = new Worker(entry, { env })
  const pending = new Map<number, PendingRun>()
  let threadId = 0
  let alive = true
  let nextRunId = 1

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

  worker.onerror = (event: ErrorEvent) => shutdown(new Error(`worker ${id} error: ${event.message}`))

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
    if (!alive) return code
    alive = false
    clearTimeout(readyTimer)
    failReady(new Error(`worker ${id} terminated`))
    failAll(new Error(`worker ${id} terminated`))
    const result = await worker.terminate()
    return typeof result === "number" ? result : code
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
  const size = options.size ?? resolveWorkerCount(options)
  if (!Number.isInteger(size) || size < 1) {
    throw new Error(`invalid worker pool size: ${size} (expected a positive integer)`)
  }
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
