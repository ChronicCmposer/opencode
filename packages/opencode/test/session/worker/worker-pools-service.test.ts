import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { createExecutorWorkerPool, type ExecutorWorker, type ExecutorWorkerPool } from "@/session/worker/execute"
import {
  createBunExecutorTransport,
  createBunExecutorWorkerPool,
  createIdleIsolateCap,
  createProductionWorkerPool,
  createSessionBoundExecutorWorker,
  createWorkerCap,
  fromPool,
  layer,
  liveSessionWorkerCount,
  makeProduction,
  node,
  Service,
  spawnBunExecutorWorker,
} from "@/session/worker/worker-pools"
import { pollUntil, sid } from "../../lib/async"
import { FakeExecutorWorker } from "../../lib/fake-worker-pool"
import { it } from "../../lib/effect"
import { sessionWorkerInit } from "../../lib/session-worker-init"

/** The dependency-light executor fixture (speaks the executor protocol). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

describe("WorkerPools service surface", () => {
  it.live("layer builds the service without spawning an isolate", () =>
    Effect.gen(function* () {
      const before = liveSessionWorkerCount()
      const pools = yield* Service

      expect(pools.executorPool.workers).toHaveLength(0)
      expect(liveSessionWorkerCount()).toBe(before)

      const stats = yield* pools.stats
      expect(stats.workers).toBe(0)
      expect(stats.idleWorkers).toBe(0)
      expect(stats.idleCapacity).toBeGreaterThan(0)
      expect(stats.idleEvictions).toBe(0)
    }).pipe(Effect.provide(layer({ init: sessionWorkerInit() }))),
  )

  test("node exposes the unbound WorkerPools service tag", () => {
    const poolsNode = node({ init: sessionWorkerInit() })

    expect(poolsNode.kind).toBe("layer")
    expect(poolsNode.service).toBe(Service)
    expect(poolsNode.dependencies).toEqual([])
  })

  it.live("fromPool adapts a structural pool through the Effect methods", () =>
    Effect.gen(function* () {
      const pool = createExecutorWorkerPool({ spawn: (id) => new FakeExecutorWorker(id, { value: "ok" }) })
      const pools = fromPool(pool)
      const sessionID = sid("ses_worker_pools_service_fake")

      const worker = yield* pools.spawn()
      expect(yield* pools.workers).toHaveLength(1)
      expect((yield* pools.byID(worker.id))?.id).toBe(worker.id)
      expect(yield* pools.resultFor(sessionID)).toBeUndefined()

      yield* Effect.promise(() => worker.execute({ sessionID, depth: 0 }))
      expect(yield* pools.resultFor(sessionID)).toEqual({ sessionID, value: "ok" })

      yield* pools.releaseSession(sessionID, worker.id)
      expect(yield* pools.byID(worker.id)).toBeUndefined()
      expect((yield* pools.stats).workers).toBe(0)

      yield* pools.terminateAll()
    }),
  )

  it.live("terminateAll joins concurrent calls and is idempotent", () =>
    Effect.gen(function* () {
      let calls = 0
      let finished = 0
      const gate = new Promise<void>((resolve) => setTimeout(resolve, 10))
      const pool: ExecutorWorkerPool = {
        workers: [],
        spawn: () => {
          throw new Error("spawn is unused by the terminateAll test")
        },
        terminateAll: async () => {
          calls += 1
          await gate
          finished += 1
        },
        byID: () => undefined,
        resultFor: () => undefined,
      }
      const pools = fromPool(pool)

      yield* Effect.all([pools.terminateAll(), pools.terminateAll()], { concurrency: "unbounded" })
      expect(calls).toBe(1)
      expect(finished).toBe(1)

      yield* pools.terminateAll()
      expect(calls).toBe(1)
      expect(finished).toBe(1)
    }),
  )

  it.live(
    "makeProduction dedupes concurrent init resolution",
    () =>
      Effect.gen(function* () {
        let initCalls = 0
        const pools = yield* makeProduction({
          init: () => {
            initCalls += 1
            return sessionWorkerInit()
          },
          entry: EXECUTOR_FIXTURE,
          workKind: "instant",
          timeoutMs: 15_000,
          maxIdleWorkers: 2,
        })
        const sessionID = sid("ses_worker_pools_init_dedupe")
        const first = yield* pools.spawn()
        const second = yield* pools.spawn()

        yield* Effect.promise(() =>
          Promise.all([first.execute({ sessionID, depth: 0 }), second.execute({ sessionID, depth: 0 })]),
        )

        expect(initCalls).toBe(1)
        yield* pools.terminateAll()
      }),
    30_000,
  )

  it.live(
    "makeProduction shares a failed init and retries it on the next turn",
    () =>
      Effect.gen(function* () {
        let builds = 0
        let shouldFail = true
        const pools = yield* makeProduction({
          init: () => {
            builds += 1
            if (shouldFail) {
              shouldFail = false
              throw new Error("production init failed")
            }
            return sessionWorkerInit()
          },
          entry: EXECUTOR_FIXTURE,
          workKind: "instant",
          timeoutMs: 15_000,
          maxIdleWorkers: 2,
        })
        const sessionID = sid("ses_worker_pools_init_retry")
        const first = yield* pools.spawn()
        const second = yield* pools.spawn()

        const failed = yield* Effect.promise(() =>
          Promise.allSettled([
            first.execute({ sessionID, depth: 0 }),
            second.execute({ sessionID, depth: 0 }),
          ]),
        )
        expect(failed.map((outcome) => outcome.status)).toEqual(["rejected", "rejected"])
        expect(builds).toBe(1)

        // The failed flight is gone, so the next turn rebuilds and succeeds.
        yield* Effect.promise(() => first.execute({ sessionID, depth: 0 }))
        expect(builds).toBe(2)
        expect((yield* pools.resultFor(sessionID))?.sessionID).toBe(sessionID)
        yield* pools.terminateAll()
      }),
    30_000,
  )

  test("idle-cap concurrent settlements run one single-flight enforce", async () => {
    const evicted: number[] = []
    const terminated: number[] = []
    let releaseEviction!: () => void
    const evictionGate = new Promise<void>((resolve) => {
      releaseEviction = resolve
    })
    const cap = createIdleIsolateCap({
      capacity: 1,
      onEvict: async (entry) => {
        evicted.push(entry.worker.id)
        await evictionGate
      },
    })
    const workers = [1, 2, 3].map((id) => {
      const inner: ExecutorWorker = {
        id,
        alive: true,
        execute: async () => {},
        cancel: async () => {},
        assertNotBusy: async () => false,
        resultFor: () => undefined,
        terminate: async () => {
          terminated.push(id)
        },
      }
      return cap.track(inner)
    })

    // Every worker settles at once, so three enforce requests race one flight.
    await Promise.all(
      workers.map((worker, index) => worker.execute({ sessionID: sid(`ses_idle_cap_${index}`), depth: 0 })),
    )

    // The first enforce is still awaiting its retirement: no second eviction may
    // overlap it, even though two more settlements just asked for enforcement.
    await pollUntil(() => evicted.length === 1, { message: "the first idle eviction never started" })
    expect(evicted).toEqual([1])
    expect(cap.evictions).toBe(1)

    releaseEviction()
    await pollUntil(() => cap.idle === 1, { message: "the idle cap never settled at capacity" })
    expect(cap.evictions).toBe(2)
    expect(new Set(evicted).size).toBe(2)
    expect(terminated.sort()).toEqual([1, 2])
  })

  test("idle-cap keeps a background-working isolate alive above capacity", async () => {
    let background = true
    const evicted: number[] = []
    const cap = createIdleIsolateCap({
      capacity: 1,
      onEvict: (entry) => {
        evicted.push(entry.worker.id)
      },
    })
    const workers = [1, 2].map((id) => {
      const inner: ExecutorWorker = {
        id,
        alive: true,
        execute: async () => {},
        cancel: async () => {},
        assertNotBusy: async () => false,
        resultFor: () => undefined,
        terminate: async () => {},
        hasActiveWork: async () => background,
      }
      return cap.track(inner)
    })

    await workers[0]!.execute({ sessionID: sid("ses_idle_cap_background_1"), depth: 0 })
    await workers[1]!.execute({ sessionID: sid("ses_idle_cap_background_2"), depth: 0 })

    // The oldest isolate still owns background work, so the cap keeps it alive
    // rather than killing a running job.
    await pollUntil(() => cap.idle === 2, { message: "the background-working isolate was not retained" })
    expect(cap.evictions).toBe(0)
    expect(evicted).toEqual([])

    // Once the background job finishes, the next settlement revisits the cap.
    background = false
    await workers[1]!.execute({ sessionID: sid("ses_idle_cap_background_2"), depth: 0 })
    await pollUntil(() => cap.idle === 1, { message: "the idle cap never revisited the finished background job" })
    expect(cap.evictions).toBe(1)
    expect(evicted).toEqual([1])
  })

  test("a throwing eviction hook still terminates and counts the eviction", async () => {
    const terminated: number[] = []
    const cap = createIdleIsolateCap({
      capacity: 1,
      onEvict: () => {
        throw new Error("retirement hook failed")
      },
    })
    const workers = [1, 2].map((id) => {
      const inner: ExecutorWorker = {
        id,
        alive: true,
        execute: async () => {},
        cancel: async () => {},
        assertNotBusy: async () => false,
        resultFor: () => undefined,
        terminate: async () => {
          terminated.push(id)
        },
      }
      return cap.track(inner)
    })

    await workers[0]!.execute({ sessionID: sid("ses_idle_cap_throwing_1"), depth: 0 })
    await workers[1]!.execute({ sessionID: sid("ses_idle_cap_throwing_2"), depth: 0 })

    await pollUntil(() => terminated.length === 1, { message: "the throwing retirement hook blocked eviction" })
    expect(cap.evictions).toBe(1)
    expect(cap.idle).toBe(1)
    expect(terminated).toEqual([1])
  })

  test("every existing production factory stays exported as an adapter", async () => {
    const module = await import("@/session/worker/worker-pools")

    for (const name of [
      "createProductionWorkerPool",
      "createBunExecutorWorkerPool",
      "createBunExecutorTransport",
      "spawnBunExecutorWorker",
      "createWorkerCap",
      "createIdleIsolateCap",
      "createSessionBoundExecutorWorker",
    ]) {
      expect(typeof (module as Record<string, unknown>)[name]).toBe("function")
    }
    expect(typeof module.WorkerPools.fromPool).toBe("function")
    expect(typeof module.WorkerPools.makeProduction).toBe("function")
  })

  test("the adapter factories are the same functions the module exports directly", () => {
    expect(typeof createProductionWorkerPool).toBe("function")
    expect(typeof createBunExecutorWorkerPool).toBe("function")
    expect(typeof createBunExecutorTransport).toBe("function")
    expect(typeof spawnBunExecutorWorker).toBe("function")
    expect(typeof createWorkerCap).toBe("function")
    expect(typeof createIdleIsolateCap).toBe("function")
    expect(typeof createSessionBoundExecutorWorker).toBe("function")
  })
})
