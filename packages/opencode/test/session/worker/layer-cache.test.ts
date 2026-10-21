// Phase 6.6 — cache contract tests.

import { describe, expect, test } from "bun:test"
import { WorkerCache } from "@/session/worker/cache"

const clock = (start = 0) => {
  let now = start
  return { now: () => now, advance: (ms: number) => (now += ms) }
}

describe("WorkerCache", () => {
  test("immutable entries never expire and ignore epoch bumps", () => {
    const time = clock()
    const cache = new WorkerCache({ now: time.now, ttlMs: 10 })
    cache.defineImmutable("agent/plan", { name: "plan" })

    time.advance(10_000)
    cache.invalidateEpoch(5)
    expect(cache.get<{ name: string }>("agent/plan")).toEqual({ name: "plan" })
  })

  test("stale-tolerant entries are served within TTL and expire after it", () => {
    const time = clock()
    const cache = new WorkerCache({ now: time.now, ttlMs: 100 })
    cache.defineStale("config", { model: "a" })

    time.advance(50)
    expect(cache.get<{ model: string }>("config")).toEqual({ model: "a" })

    time.advance(60)
    expect(cache.get("config")).toBeUndefined()
    expect(cache.has("config")).toBe(false)
  })

  test("stale-tolerant entries are invalidated by an epoch bump", () => {
    const time = clock()
    const cache = new WorkerCache({ now: time.now, ttlMs: 10_000, epoch: 1 })
    cache.defineStale("auth", { token: "old" })

    expect(cache.get<{ token: string }>("auth")).toEqual({ token: "old" })
    cache.invalidateEpoch(2)
    expect(cache.get("auth")).toBeUndefined()
  })

  test("per-entry TTL overrides the default", () => {
    const time = clock()
    const cache = new WorkerCache({ now: time.now, ttlMs: 100 })
    cache.defineStale("short", 1, 10)
    cache.defineStale("long", 2, 1000)

    time.advance(50)
    expect(cache.get("short")).toBeUndefined()
    expect(cache.get<number>("long")).toBe(2)
  })

  test("read-through loads once then serves the cache", async () => {
    const cache = new WorkerCache()
    let loads = 0
    const loader = async () => {
      loads += 1
      return { id: "message-1" }
    }

    expect(await cache.readThrough("message:1", loader)).toEqual({ id: "message-1" })
    expect(await cache.readThrough("message:1", loader)).toEqual({ id: "message-1" })
    expect(loads).toBe(1)
  })

  test("read-through is invalidated by an own write but not by an epoch bump", async () => {
    const cache = new WorkerCache()
    let loads = 0
    const loader = async () => ({ id: ++loads })

    await cache.readThrough("part:1", loader)
    cache.invalidateEpoch(9)
    await cache.readThrough("part:1", loader)
    expect(loads).toBe(1)

    cache.invalidate("part:1")
    await cache.readThrough("part:1", loader)
    expect(loads).toBe(2)
  })

  test("invalidateReadThrough drops only read-through entries", async () => {
    const cache = new WorkerCache()
    cache.defineImmutable("model", "m")
    cache.defineStale("config", "c")
    await cache.readThrough("session:1", async () => "s")

    cache.invalidateReadThrough()
    expect(cache.get("session:1")).toBeUndefined()
    expect(cache.get<string>("model")).toBe("m")
    expect(cache.get<string>("config")).toBe("c")
  })

  test("evicts least-recently-used entries at capacity", () => {
    const cache = new WorkerCache({ capacity: 2 })
    cache.defineImmutable("a", 1)
    cache.defineImmutable("b", 2)
    cache.get("a") // touch a so b is now least-recent
    cache.defineImmutable("c", 3)

    expect(cache.size).toBe(2)
    expect(cache.get<number>("b")).toBeUndefined()
    expect(cache.get<number>("a")).toBe(1)
    expect(cache.get<number>("c")).toBe(3)
  })
})
