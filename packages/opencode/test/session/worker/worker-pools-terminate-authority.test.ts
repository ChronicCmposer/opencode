// One isolate-terminate authority: the `ScopedCache` resource release. Crash,
// cancel, reap, model switch, and shutdown all invalidate a `(sessionID,
// generation)` entry, so the orderly DB/WAL close runs exactly once and a
// successor never constructs before the retired isolate has exited.

import { describe, expect, test } from "bun:test"
import { Effect, Exit, Scope } from "effect"
import { makeProduction, makeSpawnGate } from "@/session/worker/worker-pools"
import type { ProductionWorkerPoolOptions } from "@/session/worker/worker-pools"
import type { ExecutorWorkerPool } from "@/session/worker/execute"
import { createDispatcher } from "@/session/worker/dispatch"
import { createRecordingScheduler } from "../../lib/scheduler"
import { flush, pollUntil, sid } from "../../lib/async"

const ready = (threadId: number) => ({ type: "ready", threadId })

class FakeWorker {
  static instances: FakeWorker[] = []
  static terminateGate: Promise<void> | undefined
  /** When set, a run frame stays unanswered so a test can cancel/crash it. */
  static holdRuns = false
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: ((event: ErrorEvent) => void) | null = null
  readonly posted: unknown[] = []
  terminations = 0
  private readonly listeners = new Set<(event: { data: unknown }) => void>()

  constructor(readonly url: string | URL) {
    FakeWorker.instances.push(this)
    queueMicrotask(() => this.emit(ready(FakeWorker.instances.length)))
  }

  private emit(data: unknown): void {
    this.onmessage?.({ data } as MessageEvent)
    for (const listener of this.listeners) listener({ data })
  }

  postMessage(message: unknown): void {
    this.posted.push(message)
    const frame = message as { readonly type?: string; readonly id?: number; readonly sessionID?: string }
    if (frame.type === "shutdown") this.emit({ type: "shutdown-ack", id: 0 })
    if (frame.type === "hasActiveWork") this.emit({ type: "activeWork", id: frame.id, active: false })
    if ((frame.type === "run" || frame.type === "startShell") && !FakeWorker.holdRuns) {
      this.emit({ type: "done", id: frame.id, result: { sessionID: frame.sessionID } })
    }
  }

  /** Model an isolate-level crash: the transport sees the error and dies. */
  crash(): void {
    this.onerror?.({ message: "fake isolate crash", error: new Error("fake isolate crash") } as ErrorEvent)
  }

  addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    if (type === "message") this.listeners.add(listener)
  }

  removeEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    if (type === "message") this.listeners.delete(listener)
  }

  async terminate(): Promise<number> {
    this.terminations += 1
    await FakeWorker.terminateGate
    return 0
  }

  static reset(): void {
    FakeWorker.instances = []
    FakeWorker.terminateGate = undefined
    FakeWorker.holdRuns = false
  }
}

async function withFakeWorker(run: () => Promise<void>): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  FakeWorker.reset()
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    await run()
  } finally {
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

async function createProductionPool(options: ProductionWorkerPoolOptions) {
  const scope = Effect.runSync(Scope.make())
  const pools = await Effect.runPromise(Scope.provide(scope)(makeProduction(options)))
  return {
    pool: pools.executorPool as ExecutorWorkerPool,
    dispose: () => Effect.runPromise(Scope.close(scope, Exit.void)).then(() => undefined),
  }
}

const options = (overrides: Partial<ProductionWorkerPoolOptions> = {}): ProductionWorkerPoolOptions => ({
  init: () => ({}) as never,
  generationOf: () => "model-a",
  entry: "fake://session-worker",
  workKind: "session",
  timeoutMs: 2_000,
  ...overrides,
})

describe("one isolate-terminate authority", () => {
  test("pool shutdown runs the orderly close and terminates each isolate once", async () => {
    await withFakeWorker(async () => {
      const { pool, dispose } = await createProductionPool(options())
      try {
        const worker = pool.spawn()
        await worker.execute({ sessionID: sid("ses_authority_shutdown"), depth: 0 })
        expect(FakeWorker.instances).toHaveLength(1)

        await pool.terminateAll()
        await pool.terminateAll()

        const isolate = FakeWorker.instances[0]!
        expect(isolate.posted.some((frame) => (frame as { type?: string }).type === "shutdown")).toBe(true)
        expect(isolate.terminations).toBe(1)
      } finally {
        await dispose()
      }
    })
  })

  test("a model switch releases the old generation before the successor constructs", async () => {
    await withFakeWorker(async () => {
      let generation = "model-a"
      const { pool, dispose } = await createProductionPool(
        options({ generationOf: () => generation }),
      )
      try {
        const worker = pool.spawn()
        await worker.execute({ sessionID: sid("ses_authority_switch"), depth: 0 })
        expect(FakeWorker.instances).toHaveLength(1)

        let releaseTerminate!: () => void
        FakeWorker.terminateGate = new Promise<void>((resolve) => {
          releaseTerminate = resolve
        })
        generation = "model-b"
        const switched = worker.execute({ sessionID: sid("ses_authority_switch"), depth: 0 })

        // The old isolate is still closing: no successor may exist yet.
        await flush()
        await pollUntil(() => FakeWorker.instances[0]!.posted.some((frame) => (frame as { type?: string }).type === "shutdown"), {
          message: "the retired isolate never received the orderly shutdown frame",
        })
        expect(FakeWorker.instances).toHaveLength(1)

        releaseTerminate()
        await switched
        expect(FakeWorker.instances).toHaveLength(2)
        expect(FakeWorker.instances[0]!.terminations).toBe(1)

        await pool.terminateAll()
        expect(FakeWorker.instances[1]!.terminations).toBe(1)
      } finally {
        await dispose()
      }
    })
  })

  test("an interrupted permit waiter leaves the queue without taking a slot", async () => {
    const gate = makeSpawnGate({ timeoutMs: 1_000, capacity: 1 })
    const holder = await gate.acquire()
    const controller = new AbortController()
    const queued = gate.acquire(controller.signal)

    await pollUntil(() => gate.waiting === 1, { message: "the waiter never queued" })
    controller.abort()
    await expect(queued).rejects.toThrow(/aborted/)
    expect(gate.waiting).toBe(0)
    expect(gate.active).toBe(1)

    holder.release()
    await pollUntil(() => gate.active === 0, { message: "the held slot never released" })
    const next = await gate.acquire()
    expect(gate.active).toBe(1)
    next.release()
  })

  test("a crash retires the isolate through one orderly close", async () => {
    await withFakeWorker(async () => {
      FakeWorker.holdRuns = true
      const { pool, dispose } = await createProductionPool(options())
      const dispatcher = createDispatcher({ pool })
      const sessionID = sid("ses_authority_crash")
      try {
        const settled = dispatcher.dispatch({ sessionID }).catch((error) => error)
        await pollUntil(
          () => FakeWorker.instances[0]?.posted.some((frame) => (frame as { type?: string }).type === "run") === true,
          { message: "the crash run never reached the isolate" },
        )
        const isolate = FakeWorker.instances[0]!
        isolate.crash()
        expect(await settled).toBeInstanceOf(Error)

        await pollUntil(() => isolate.terminations === 1, {
          message: "the crashed isolate was not terminated exactly once",
        })
        await dispatcher.terminateAll()
        expect(isolate.terminations).toBe(1)
        expect(shutdownFrames(isolate)).toBe(1)
      } finally {
        await dispatcher.terminateAll()
        await dispose()
      }
    })
  })

  test("a cancel retires the isolate through one orderly close", async () => {
    await withFakeWorker(async () => {
      FakeWorker.holdRuns = true
      const { pool, dispose } = await createProductionPool(options())
      const dispatcher = createDispatcher({ pool })
      const sessionID = sid("ses_authority_cancel")
      try {
        const settled = dispatcher.dispatch({ sessionID }).catch((error) => error)
        await pollUntil(
          () => FakeWorker.instances[0]?.posted.some((frame) => (frame as { type?: string }).type === "run") === true,
          { message: "the cancelled run never reached the isolate" },
        )
        const isolate = FakeWorker.instances[0]!
        await dispatcher.cancel(sessionID)
        expect(await settled).toBeInstanceOf(Error)

        await pollUntil(() => isolate.terminations === 1, {
          message: "the cancelled isolate was not terminated exactly once",
        })
        await dispatcher.terminateAll()
        expect(isolate.terminations).toBe(1)
        expect(shutdownFrames(isolate)).toBe(1)
      } finally {
        await dispatcher.terminateAll()
        await dispose()
      }
    })
  })

  test("an idle reap retires the isolate through one orderly close", async () => {
    await withFakeWorker(async () => {
      const { pool, dispose } = await createProductionPool(options())
      const scheduler = createRecordingScheduler()
      const dispatcher = createDispatcher({ pool, schedule: scheduler.schedule, reapMs: 1_000 })
      const sessionID = sid("ses_authority_reap")
      try {
        await dispatcher.dispatch({ sessionID })
        const isolate = FakeWorker.instances[0]!
        await pollUntil(() => scheduler.pending().some((timer) => timer.delayMs === 1_000), {
          message: "the idle reap window was never armed",
        })

        scheduler.fire()
        await pollUntil(() => isolate.terminations === 1, {
          message: "the reaped isolate was not terminated exactly once",
        })
        expect(dispatcher.registry.has(sessionID)).toBe(false)

        await dispatcher.terminateAll()
        expect(isolate.terminations).toBe(1)
        expect(shutdownFrames(isolate)).toBe(1)
      } finally {
        await dispatcher.terminateAll()
        await dispose()
      }
    })
  })

  test("a terminate during the generation probe never constructs an isolate", async () => {
    await withFakeWorker(async () => {
      let releaseProbe!: () => void
      const probe = new Promise<string>((resolve) => {
        releaseProbe = () => resolve("model-a")
      })
      const { pool, dispose } = await createProductionPool(options({ generationOf: () => probe }))
      try {
        const worker = pool.spawn()
        const run = worker.execute({ sessionID: sid("ses_authority_probe"), depth: 0 })
        const outcome = run.then(
          () => undefined,
          (error: unknown) => error,
        )
        await flush()

        const terminating = worker.terminate()
        releaseProbe()
        await terminating

        expect(await outcome).toBeInstanceOf(Error)
        expect(FakeWorker.instances).toHaveLength(0)
      } finally {
        await dispose()
      }
    })
  })

  test("pool shutdown is bounded when the init resolver never settles", async () => {
    await withFakeWorker(async () => {
      const { pool, dispose } = await createProductionPool(
        options({ generationOf: undefined, init: () => new Promise<never>(() => undefined) }),
      )
      try {
        const worker = pool.spawn()
        // The slot parks in `resolveInit`; no isolate is ever constructed.
        void worker.execute({ sessionID: sid("ses_authority_wedged"), depth: 0 }).catch(() => undefined)
        await flush()

        const started = Date.now()
        await pool.terminateAll()
        expect(Date.now() - started).toBeLessThan(5_000)
        expect(FakeWorker.instances).toHaveLength(0)
      } finally {
        await dispose()
      }
    })
  })
})

const shutdownFrames = (worker: FakeWorker): number =>
  worker.posted.filter((frame) => (frame as { type?: string }).type === "shutdown").length
