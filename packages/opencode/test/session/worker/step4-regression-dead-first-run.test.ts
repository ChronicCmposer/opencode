// Regression for the Step-4 M2 interaction: the dead-first-run invalidation
// clears the slot's bound isolate, so the slot must still report `alive: false`
// until a fresh isolate materializes. Otherwise dispatch cannot classify the
// transport exit as a crash and never runs the resume decision.
//
// The cache key must stay invalidated: the release has to run, and the next
// turn must boot a fresh isolate instead of replaying the dead cached entry.

import { describe, expect, test } from "bun:test"
import { Effect, Exit, Scope } from "effect"
import { resolveBuildHash } from "@/session/worker/build-hash"
import { makeProduction, type ProductionWorkerPoolOptions } from "@/session/worker/worker-pools"
import type { ExecutorWorkerPool } from "@/session/worker/execute"
import { createDispatcher, WorkerCrashedError } from "@/session/worker/dispatch"
import { pollUntil, sid } from "../../lib/async"

const ready = (threadId: number) => ({ type: "ready", threadId, buildHash: resolveBuildHash({ env: {} }).hash })

/** A controllable isolate: it boots, holds runs, and can die like a real one. */
class FakeIsolate {
  static instances: FakeIsolate[] = []
  static holdRuns = false
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: ((event: ErrorEvent) => void) | null = null
  readonly posted: unknown[] = []
  terminations = 0
  private readonly listeners = new Set<(event: { data: unknown }) => void>()

  constructor(readonly url: string | URL) {
    FakeIsolate.instances.push(this)
    queueMicrotask(() => this.emit(ready(FakeIsolate.instances.length)))
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
    if ((frame.type === "run" || frame.type === "startShell") && !FakeIsolate.holdRuns) {
      this.emit({ type: "done", id: frame.id, result: { sessionID: frame.sessionID } })
    }
  }

  /** Model a hard transport exit: the isolate dies and every waiter fails. */
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
    return 0
  }

  static reset(): void {
    FakeIsolate.instances = []
    FakeIsolate.holdRuns = false
  }
}

async function withFakeIsolate(run: () => Promise<void>): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  FakeIsolate.reset()
  ;(globalThis as { Worker?: unknown }).Worker = FakeIsolate
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

const options = (): ProductionWorkerPoolOptions => ({
  init: () => ({}) as never,
  generationOf: () => "model-a",
  entry: "fake://session-worker",
  workKind: "session",
  timeoutMs: 2_000,
})

const runFrame = (window: FakeIsolate): boolean =>
  window.posted.some((frame) => (frame as { type?: string }).type === "run")

describe("dead first run", () => {
  test("the slot reports dead and a fresh isolate boots after the cache key is invalidated", async () => {
    await withFakeIsolate(async () => {
      FakeIsolate.holdRuns = true
      const { pool, dispose } = await createProductionPool(options())
      const sessionID = sid("ses_step4_pool_dead")
      try {
        const slot = pool.spawn()
        const first = slot.execute({ sessionID, depth: 0 })
        await pollUntil(() => FakeIsolate.instances[0] !== undefined && runFrame(FakeIsolate.instances[0]!), {
          message: "the first run never reached the isolate",
        })
        const dead = FakeIsolate.instances[0]!
        dead.crash()

        // The raw transport failure still reaches the caller, but the slot is
        // now observably dead even though its bound key was invalidated.
        await expect(first).rejects.toThrow(/fake isolate crash/)
        expect(slot.alive).toBe(false)
        await pollUntil(() => dead.terminations === 1, {
          message: "the dead isolate was not released through the cache invalidation",
        })

        // The invalidated key must not replay the dead entry: the next run boots
        // a fresh isolate, which makes the slot alive again.
        FakeIsolate.holdRuns = false
        await slot.execute({ sessionID, depth: 0 })
        expect(FakeIsolate.instances).toHaveLength(2)
        expect(slot.alive).toBe(true)
      } finally {
        await dispose()
      }
    })
  })

  test("dispatch surfaces WorkerCrashedError for a dead first run", async () => {
    await withFakeIsolate(async () => {
      FakeIsolate.holdRuns = true
      const { pool, dispose } = await createProductionPool(options())
      const dispatcher = createDispatcher({ pool })
      const sessionID = sid("ses_step4_dispatch_dead")
      try {
        const settled = dispatcher.dispatch({ sessionID }).catch((error: unknown) => error)
        await pollUntil(() => FakeIsolate.instances[0] !== undefined && runFrame(FakeIsolate.instances[0]!), {
          message: "the crashed run never reached the isolate",
        })
        const slot = pool.workers[0]!
        FakeIsolate.instances[0]!.crash()

        const error = await settled
        expect(error).toBeInstanceOf(WorkerCrashedError)
        expect(slot.alive).toBe(false)
      } finally {
        await dispatcher.terminateAll()
        await dispose()
      }
    })
  })
})
