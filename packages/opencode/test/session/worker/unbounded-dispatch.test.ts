// v2 unbounded dispatch: no admission cap, no headroom, no queue rejection.
// Every session is admitted on its own lazily-materialized worker.

import { describe, expect, test } from "bun:test"
import { createDispatcher } from "../../lib/dispatcher"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"
import { flush, sid } from "../../lib/async"

describe("unbounded per-session dispatch", () => {
  test("100+ concurrent sessions are all admitted without a queue error", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const count = 128

    const runs = Array.from({ length: count }, (_, index) =>
      dispatcher.dispatch({ sessionID: sid(`ses_${index}`) }),
    )
    await flush()

    expect(dispatcher.busySlotCount).toBe(count)
    expect(dispatcher.queuedCount).toBe(0)
    expect(pool.spawned).toHaveLength(count)
    expect(pool.spawned.every((worker) => worker.runs.length === 1)).toBe(true)

    for (let index = 0; index < count; index++) {
      pool.spawned[index].finish(sid(`ses_${index}`))
    }
    await Promise.all(runs)
    expect(dispatcher.busySlotCount).toBe(0)
    await dispatcher.terminateAll()
  })

  test("the same session reuses its worker across turns (one spawn per session)", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const first = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    pool.spawned[0].finish(sid("ses_1"))
    await first

    const second = dispatcher.dispatch({ sessionID: sid("ses_1") })
    await flush()
    expect(pool.spawned).toHaveLength(1)
    expect(pool.spawned[0].runs).toHaveLength(2)

    pool.spawned[0].finish(sid("ses_1"))
    await second
    await dispatcher.terminateAll()
  })

  test("a different run identity parks without a queue bound and runs after the first", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const root = dispatcher.dispatch({ sessionID: sid("ses_1") })
    const waiters = Array.from({ length: 200 }, (_, index) =>
      dispatcher.dispatch({ sessionID: sid("ses_1"), depth: index + 1 }),
    )
    await flush()
    expect(dispatcher.queuedCount).toBe(200)
    expect(pool.spawned).toHaveLength(1)

    pool.spawned[0].finish(sid("ses_1"))
    await root
    await flush()
    // The head waiter was promoted onto the same reused worker.
    expect(pool.spawned[0].runs).toHaveLength(2)
    expect(pool.spawned[0].runs[1].depth).toBe(1)

    await dispatcher.terminateAll()
    await Promise.allSettled(waiters)
  })
})
