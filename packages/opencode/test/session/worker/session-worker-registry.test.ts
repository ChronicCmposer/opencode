// v2 per-session worker registry: lazy get-or-create, reuse, distinct sessions,
// monotonic ids, dispose/lookup.

import { describe, expect, test } from "bun:test"
import { createSessionWorkerRegistry } from "@/session/worker/session-registry"
import { sid } from "../../lib/async"

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

    expect(registry.entry(sid("ses_a"))).toEqual({ id: a.id, sessionID: sid("ses_a"), worker: a })
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
