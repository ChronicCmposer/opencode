// Worker test harness (plan task 3.4).
//
// Spawns, observes and terminates Bun `Worker` threads, and injects a core
// count so pool sizing in later phases (4.2) is deterministic in tests.
//
// Design: `spawn` is synchronous (Bun's `new Worker` is synchronous); every
// message is buffered so a caller can `received()` one even if the worker
// already sent it. `waitFor` fails loudly with a descriptive timeout. Workers
// are dependency-free fixtures, so this harness runs without model APIs.

import os from "node:os"

export interface CoreCount {
  readonly cores: number
  availableParallelism(): number
}

export interface WorkerMessage<TData = unknown> {
  readonly type: string
  readonly data?: TData
  readonly message?: string
  readonly pid?: number
  readonly threadId?: number
}

export interface WorkerHarnessOptions {
  /** Injected core count; defaults to `os.availableParallelism()`. */
  readonly cores?: number
  /** Default `waitFor` timeout in ms. Defaults to 5000. */
  readonly timeoutMs?: number
  /** Environment overlay applied to every spawned worker. */
  readonly env?: Record<string, string | undefined>
}

export interface SpawnOptions {
  /** Per-worker environment overlay (wins over the harness default). */
  readonly env?: Record<string, string | undefined>
  /** Structured-cloned value delivered to the worker entry point. */
  readonly workerData?: unknown
}

export type MessageMatch<TOut> = string | ((message: TOut) => boolean)

/**
 * The variant of `TOut` whose `type` is exactly `TType`.
 *
 * When `TOut` is a discriminated union this selects the matching member, so a
 * `waitFor("ready")` result is already narrowed. When `TOut` is not a union (or
 * nothing matches), `TOut` is returned unchanged.
 */
export type MessageOfType<TOut, TType extends string> = Extract<TOut, { type: TType }> extends never
  ? TOut
  : Extract<TOut, { type: TType }>

export interface HarnessWorker<TOut = WorkerMessage, TIn = unknown> {
  readonly id: number
  readonly alive: boolean
  post(message: TIn): void
  readonly messages: ReadonlyArray<TOut>
  /** Resolve with the first buffered/future message matching `predicate`. */
  received(predicate?: (message: TOut) => boolean): Promise<TOut>
  /** Resolve on a message predicate, failing loudly on timeout. */
  waitFor(match: (message: TOut) => boolean, options?: { timeoutMs?: number }): Promise<TOut>
  /** Resolve on a message `type`, narrowing the result to the matching variant. */
  waitFor<TType extends string>(
    match: TType,
    options?: { timeoutMs?: number },
  ): Promise<MessageOfType<TOut, TType>>
  /** Resolves with the exit code once the worker terminates. */
  readonly exited: Promise<number>
  terminate(code?: number): Promise<number>
}

export interface WorkerHarness extends AsyncDisposable {
  readonly cores: number
  availableParallelism(): number
  spawn<TOut = WorkerMessage, TIn = unknown>(
    entry: string | URL,
    options?: SpawnOptions,
  ): HarnessWorker<TOut, TIn>
  readonly workers: ReadonlyArray<HarnessWorker>
  terminateAll(): Promise<void>
  dispose(): Promise<void>
}

const DEFAULT_TIMEOUT_MS = 5000

/** Parse an injected core count, or fall back to the real parallelism. */
export function coreCount(cores?: number): CoreCount {
  if (cores === undefined) {
    const detected = os.availableParallelism()
    return { cores: detected, availableParallelism: () => detected }
  }
  if (!Number.isInteger(cores) || cores < 1) {
    throw new Error(`invalid core count: ${cores} (expected a positive integer)`)
  }
  return { cores, availableParallelism: () => cores }
}

function workerEnv(overlay?: Record<string, string | undefined>): Record<string, string> {
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

interface PendingWaiter<TOut> {
  readonly predicate: (message: TOut) => boolean
  readonly resolve: (message: TOut) => void
  readonly reject: (error: Error) => void
}

function createHandle<TOut, TIn>(
  id: number,
  worker: Worker,
  defaultTimeoutMs: number,
): HarnessWorker<TOut, TIn> {
  const messages: TOut[] = []
  const waiters = new Set<PendingWaiter<TOut>>()
  let alive = true
  let exitCode: number | undefined

  let resolveExit!: (code: number) => void
  const exited = new Promise<number>((resolve) => {
    resolveExit = resolve
  })

  const settleExit = (code: number) => {
    if (!alive) return
    alive = false
    exitCode = code
    for (const waiter of [...waiters]) waiter.reject(new Error("worker terminated"))
    resolveExit(code)
  }

  worker.onmessage = (event: MessageEvent) => {
    const message = event.data as TOut
    messages.push(message)
    for (const waiter of [...waiters]) {
      if (waiter.predicate(message)) waiter.resolve(message)
    }
  }

  worker.onerror = (event: ErrorEvent) => {
    settleExit(1)
    for (const waiter of [...waiters]) waiter.reject(new Error(`worker error: ${event.message}`))
  }

  function waitFor<TType extends string>(
    match: TType,
    options?: { timeoutMs?: number },
  ): Promise<MessageOfType<TOut, TType>>
  function waitFor(match: (message: TOut) => boolean, options?: { timeoutMs?: number }): Promise<TOut>
  function waitFor(match: MessageMatch<TOut>, options?: { timeoutMs?: number }): Promise<TOut> {
    const predicate =
      typeof match === "function" ? match : (message: TOut) => (message as { type?: string } | undefined)?.type === match
    const buffered = messages.find(predicate)
    if (buffered !== undefined) return Promise.resolve(buffered)

    const timeoutMs = options?.timeoutMs ?? defaultTimeoutMs
    return new Promise<TOut>((resolve, reject) => {
      const timer = setTimeout(() => {
        waiters.delete(waiter)
        reject(new Error(`timed out waiting for worker message after ${timeoutMs}ms`))
      }, timeoutMs)
      const waiter: PendingWaiter<TOut> = {
        predicate,
        resolve: (message) => {
          clearTimeout(timer)
          waiters.delete(waiter)
          resolve(message)
        },
        reject: (error) => {
          clearTimeout(timer)
          waiters.delete(waiter)
          reject(error)
        },
      }
      waiters.add(waiter)
    })
  }

  const terminate = async (code = 0): Promise<number> => {
    if (!alive) return exitCode ?? code
    const resolved = await worker.terminate()
    settleExit(typeof resolved === "number" ? resolved : code)
    return exitCode ?? code
  }

  return {
    id,
    get alive() {
      return alive
    },
    post: (message) => {
      if (!alive) throw new Error(`cannot post to terminated worker ${id}`)
      worker.postMessage(message)
    },
    get messages() {
      return messages.slice()
    },
    received: (predicate) => waitFor(predicate ?? (() => true)),
    waitFor,
    get exited() {
      return exited
    },
    terminate,
  }
}

export function createWorkerHarness(options: WorkerHarnessOptions = {}): WorkerHarness {
  const count = coreCount(options.cores)
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const workers: HarnessWorker[] = []
  let nextId = 1

  const spawn = <TOut, TIn>(entry: string | URL, spawnOptions: SpawnOptions = {}): HarnessWorker<TOut, TIn> => {
    const env = workerEnv({ ...options.env, ...spawnOptions.env })
    const worker = new Worker(entry, {
      env,
      ...(spawnOptions.workerData === undefined ? {} : { workerData: spawnOptions.workerData }),
    })
    const handle = createHandle<TOut, TIn>(nextId++, worker, timeoutMs)
    workers.push(handle as HarnessWorker)
    return handle
  }

  const terminateAll = async () => {
    await Promise.all(workers.filter((worker) => worker.alive).map((worker) => worker.terminate()))
  }

  return {
    cores: count.cores,
    availableParallelism: count.availableParallelism,
    spawn,
    get workers() {
      return workers.slice()
    },
    terminateAll,
    dispose: async () => {
      await Promise.all(workers.map((worker) => worker.terminate()))
    },
    [Symbol.asyncDispose]: terminateAll,
  }
}
