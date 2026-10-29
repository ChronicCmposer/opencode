// v2 per-session worker registry: lazy get-or-create, reuse, distinct sessions,
// monotonic ids, dispose/lookup.

import { describe, expect, test } from "bun:test"
import { createSessionWorkerRegistry } from "@/session/worker/session-registry"
import { SessionCancelledError } from "@/session/worker/dispatch"
import { createDispatcher } from "../../lib/dispatcher"
import { sid } from "../../lib/async"
import { flush } from "../../lib/async"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"

interface FakeWorker {
  readonly id: number
  readonly sessionID: string
}

describe("session worker registry", () => {
  test("materializes one worker per session on first get and reuses it", () => {
    const created: FakeWorker[] = []
    const registry = createSessionWorkerRegistry<FakeWorker>()
    const create = (id: number) => {
      const worker = { id, sessionID: "unset" }
      created.push(worker)
      return worker
    }

    const first = registry.getOrCreate(sid("ses_1"), create)
    const second = registry.getOrCreate(sid("ses_1"), create)

    expect(second).toBe(first)
    expect(created).toHaveLength(1)
    expect(registry.size).toBe(1)
  })

  test("distinct sessions get distinct workers with monotonic ids", () => {
    const registry = createSessionWorkerRegistry<FakeWorker>()
    const a = registry.getOrCreate(sid("ses_a"), (id) => ({ id, sessionID: "ses_a" }))
    const b = registry.getOrCreate(sid("ses_b"), (id) => ({ id, sessionID: "ses_b" }))
    const c = registry.getOrCreate(sid("ses_c"), (id) => ({ id, sessionID: "ses_c" }))

    expect([a.id, b.id, c.id]).toEqual([1, 2, 3])
    expect(new Set([a, b, c]).size).toBe(3)
    expect(registry.nextID()).toBe(4)
  })

  test("honours a first id and never reuses an id after dispose", () => {
    const registry = createSessionWorkerRegistry<FakeWorker>({ firstID: 10 })
    const a = registry.getOrCreate(sid("ses_a"), (id) => ({ id, sessionID: "ses_a" }))
    expect(a.id).toBe(10)

    expect(registry.dispose(sid("ses_a"))).toBe(a)
    expect(registry.get(sid("ses_a"))).toBeUndefined()

    const again = registry.getOrCreate(sid("ses_a"), (id) => ({ id, sessionID: "ses_a" }))
    expect(again.id).toBe(11)
  })

  test("looks up entries and clears the map", () => {
    const registry = createSessionWorkerRegistry<FakeWorker>()
    const a = registry.getOrCreate(sid("ses_a"), (id) => ({ id, sessionID: "ses_a" }))
    const b = registry.getOrCreate(sid("ses_b"), (id) => ({ id, sessionID: "ses_b" }))

    expect(registry.entry(sid("ses_a"))).toMatchObject({
      id: a.id,
      workerID: a.id,
      sessionID: sid("ses_a"),
      worker: a,
      state: "reserved",
    })
    expect(registry.entries().map((entry) => entry.sessionID)).toEqual([sid("ses_a"), sid("ses_b")])
    expect(registry.has(sid("ses_b"))).toBe(true)

    registry.clear()
    expect(registry.size).toBe(0)
    expect(registry.get(sid("ses_b"))).toBeUndefined()
  })

  test("dispose of an unknown session is a no-op", () => {
    const registry = createSessionWorkerRegistry<FakeWorker>()
    expect(registry.dispose(sid("ses_missing"))).toBeUndefined()
  })
})

describe("unified ownership registry", () => {
  test("a session has exactly one owner; workerForSession and sessionsOn agree", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const sessions = Array.from({ length: 25 }, (_, index) => sid(`ses_${index}`))
    const runs = sessions.map((sessionID) => dispatcher.dispatch({ sessionID }))
    await flush()

    const ids = sessions.map((sessionID) => dispatcher.workerForSession(sessionID))
    expect(ids.every((id) => id !== undefined)).toBe(true)
    expect(new Set(ids).size).toBe(sessions.length)
    expect(dispatcher.registry.size).toBe(sessions.length)

    sessions.forEach((sessionID, index) => {
      const id = ids[index]!
      expect(dispatcher.registry.get(sessionID)).toMatchObject({
        sessionID,
        workerID: id,
        state: "running",
      })
      expect(dispatcher.registry.sessionsOn(id)).toEqual([sessionID])
      expect(dispatcher.registry.worker(sessionID)).toBe(pool.spawned[index])
    })
    expect(dispatcher.registry.nextID()).toBe(sessions.length + 1)

    for (const [index, sessionID] of sessions.entries()) pool.spawned[index].finish(sessionID)
    await Promise.all(runs)
    await flush()

    // Completion releases the reservation but keeps the reusable worker entry.
    expect(dispatcher.registry.size).toBe(0)
    expect(dispatcher.workerForSession(sessions[0])).toBeUndefined()
    expect(dispatcher.registry.get(sessions[0])).toBeUndefined()
    expect(dispatcher.registry.worker(sessions[0])).toBe(pool.spawned[0])
    expect(dispatcher.registry.entries()).toHaveLength(sessions.length)
    await dispatcher.terminateAll()
  })

  test("ids are monotonic and never reused after a worker is retired", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const sessionID = sid("ses_a")

    const first = dispatcher.dispatch({ sessionID })
    await flush()
    expect(dispatcher.workerForSession(sessionID)).toBe(1)
    pool.spawned[0].finish(sessionID)
    await first
    await flush()

    // Reusing the idle worker must not advance the single id counter.
    const reused = dispatcher.dispatch({ sessionID })
    await flush()
    expect(dispatcher.workerForSession(sessionID)).toBe(1)
    expect(dispatcher.registry.nextID()).toBe(2)

    await dispatcher.cancel(sessionID)
    await expect(reused).rejects.toBeInstanceOf(SessionCancelledError)

    const replacement = dispatcher.dispatch({ sessionID })
    await flush()
    expect(dispatcher.workerForSession(sessionID)).toBe(2)
    expect(pool.spawned.map((worker) => worker.id)).toEqual([1, 2])
    pool.spawned[1].finish(sessionID)
    await replacement
    await dispatcher.terminateAll()
  })
})
