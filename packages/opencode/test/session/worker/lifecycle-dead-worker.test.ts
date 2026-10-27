// A worker that dies while a graceful cancel is in flight must still be retired
// and cascade to its subagent children, even though the cancel owns the run's
// terminal decision. Otherwise the dead isolate is reused by the next turn.

import { describe, expect, test } from "bun:test"
import { capture, flush, sid } from "../../lib/async"
import { createFakeWorkerPool, type FakeSessionWorker } from "../../lib/fake-worker-pool"
import { createDispatcher, ParentCrashedError } from "@/session/worker/dispatch"

describe("dead-worker reuse guard (m8)", () => {
  test("a crash during cancel grace retires the worker and cascades to children", async () => {
    const pool = createFakeWorkerPool(1, { onCancel: "ignore" })
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        graceMs: 60_000,
        portFor: (worker) => (worker as FakeSessionWorker).cancelPort,
        childrenOf: (parent) => (parent === sid("ses_parent") ? [sid("ses_child")] : []),
      },
    })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    // Start the grace window, then kill the parent's isolate before it settles.
    const cancelling = dispatcher.cancelWithGrace(sid("ses_parent")).catch(() => undefined)
    await flush()
    const dead = pool.spawned[0]!
    dead.crash(sid("ses_parent"))
    await flush()

    const childResult = await child
    if (childResult.ok) throw new Error("expected the child to be cancelled")
    expect(childResult.error).toBeInstanceOf(ParentCrashedError)
    expect(dispatcher.registry.worker(sid("ses_parent"))).toBeUndefined()

    const parentResult = await parent
    expect(parentResult.ok).toBe(false)
    await cancelling
    await dispatcher.terminateAll()
  })
})
