// Phase 4.2 — session worker pool core (plan task 4.2).
//
// K = `os.availableParallelism() − 2`, floor 1, a single worker below 4 cores,
// overridable with `OPENCODE_SESSION_WORKER_COUNT`. Cores are injected so pool
// sizing is deterministic; the spawned workers use the production bootstrap.

import { describe, expect, test } from "bun:test"
import { WORKER_COUNT_ENV, createWorkerPool, resolveWorkerCount } from "@/session/worker/pool"

describe("worker count sizing", () => {
  test("reserves two cores on four or more cores", () => {
    expect(resolveWorkerCount({ availableParallelism: () => 4, env: {} })).toBe(2)
    expect(resolveWorkerCount({ availableParallelism: () => 8, env: {} })).toBe(6)
  })

  test("uses a single worker below four cores", () => {
    for (const cores of [1, 2, 3]) {
      expect(resolveWorkerCount({ availableParallelism: () => cores, env: {} })).toBe(1)
    }
  })

  test("the env override wins over available parallelism", () => {
    expect(resolveWorkerCount({ availableParallelism: () => 8, env: { [WORKER_COUNT_ENV]: "3" } })).toBe(3)
    expect(resolveWorkerCount({ availableParallelism: () => 2, env: { [WORKER_COUNT_ENV]: "5" } })).toBe(5)
  })

  test("an invalid override or core count fails loud", () => {
    expect(() => resolveWorkerCount({ availableParallelism: () => 8, env: { [WORKER_COUNT_ENV]: "0" } })).toThrow(
      /OPENCODE_SESSION_WORKER_COUNT/,
    )
    expect(() => resolveWorkerCount({ availableParallelism: () => 8, env: { [WORKER_COUNT_ENV]: "abc" } })).toThrow(
      /OPENCODE_SESSION_WORKER_COUNT/,
    )
    expect(() => resolveWorkerCount({ availableParallelism: () => 0, env: {} })).toThrow(/availableParallelism/)
  })

  test("falls back to the real available parallelism when not injected", () => {
    expect(resolveWorkerCount({ env: {} })).toBeGreaterThanOrEqual(1)
  })
})

describe("worker pool spawn", () => {
  test("spawns K ready workers that run the production bootstrap Effect", async () => {
    await using pool = createWorkerPool({ availableParallelism: () => 4, env: {} })

    expect(pool.size).toBe(2)
    await pool.ready()
    expect(pool.workers).toHaveLength(2)
    expect(pool.workers.every((worker) => worker.alive)).toBe(true)

    const results = await Promise.all(pool.workers.map((worker) => worker.run(21)))
    expect(results).toEqual([42, 42])
  })

  test("the env override controls the spawned pool size", async () => {
    await using pool = createWorkerPool({ availableParallelism: () => 8, env: { [WORKER_COUNT_ENV]: "2" } })

    expect(pool.size).toBe(2)
    await pool.ready()
    expect(pool.workers).toHaveLength(2)
  })

  test("terminateAll stops every worker", async () => {
    const pool = createWorkerPool({ availableParallelism: () => 4, env: {} })
    await pool.ready()

    await pool.terminateAll()
    expect(pool.workers.every((worker) => !worker.alive)).toBe(true)
  })

  test("terminate resolves with a numeric exit code and is idempotent", async () => {
    const pool = createWorkerPool({ availableParallelism: () => 4, env: {} })
    await pool.ready()

    const worker = pool.workers[0]
    const first = await worker.terminate()
    expect(typeof first).toBe("number")

    // A second terminate reports the cached exit code, not the fallback.
    const second = await worker.terminate(99)
    expect(second).toBe(first)

    await pool.terminateAll()
  })
})
