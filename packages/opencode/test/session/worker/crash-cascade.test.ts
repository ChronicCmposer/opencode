// Parent CRASH = implicit cancel.
//
// A subagent parent that CRASHES must implicitly cancel its children, and the
// cascade is children-first, bounded by the same grace window as an explicit
// cancel. The dispatcher reads the `parentID → children` index through the
// `CancellationConfig.childrenOf` seam the caller supplies; no new index is
// introduced here.
//
// These tests drive the dispatcher against the shared in-memory `WorkerPool`
// fake, so the crash/cascade ordering is deterministic and no isolates are
// spawned.

import { describe, expect, test } from "bun:test"
import { SessionCancelledError, WorkerCrashedError, type CancellationConfig } from "@/session/worker/dispatch"
import { createDispatcher } from "../../lib/dispatcher"
import { SessionWorkerReplyTimeoutError } from "@/session/worker/execute"
import { capture, flush, sid } from "../../lib/async"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"

/** A `childrenOf` index backed by a plain map, plus the cancelled-session log. */
function cancellation(children: Record<string, string[]>): {
  config: CancellationConfig
  cancelled: string[]
} {
  const cancelled: string[] = []
  const config: CancellationConfig = {
    childrenOf: (parent) => (children[parent] ?? []).map(sid),
    onCancelled: (event) => cancelled.push(event.sessionID),
  }
  return { config, cancelled }
}

describe("9.6 parent CRASH implicitly cancels its children (children-first)", () => {
  test("a crashed parent cancels its running subagent child", async () => {
    const pool = createFakeWorkerPool(1)
    const { config, cancelled } = cancellation({ ses_parent: ["ses_child"] })
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    pool.spawned[0].crash(sid("ses_parent"))

    expect(await parent).toEqual({ ok: false, error: expect.any(WorkerCrashedError) })
    expect(await child).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(dispatcher.registry.has(sid("ses_child"))).toBe(false)
    expect(cancelled).toContain(sid("ses_child"))

    await dispatcher.terminateAll()
  })

  test("the cascade reaches grandchildren (children-first recursion)", async () => {
    const pool = createFakeWorkerPool(1)
    const { config, cancelled } = cancellation({
      ses_parent: ["ses_child"],
      ses_child: ["ses_grandchild"],
    })
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    const grandchild = capture(dispatcher.dispatch({ sessionID: sid("ses_grandchild"), depth: 2 }))
    await flush()

    pool.spawned[0].crash(sid("ses_parent"))

    expect(await parent).toEqual({ ok: false, error: expect.any(WorkerCrashedError) })
    expect(await child).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(await grandchild).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(cancelled).toContain(sid("ses_child"))
    expect(cancelled).toContain(sid("ses_grandchild"))

    await dispatcher.terminateAll()
  })

  test("no cancellation config means a crash never cascades (flag-off behavior)", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    await flush()

    pool.spawned[0].crash(sid("ses_parent"))
    expect(await parent).toEqual({ ok: false, error: expect.any(WorkerCrashedError) })

    await dispatcher.terminateAll()
  })

  test("a parent crash preserves the underlying isolate error", async () => {
    const pool = createFakeWorkerPool(1)
    const { config, cancelled } = cancellation({ ses_parent: ["ses_child"] })
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    const cause = new Error("worker isolate died: TypeError: db.select is not a function")
    pool.spawned[0].crashWith(sid("ses_parent"), cause)

    const settled = await parent
    if (settled.ok) throw new Error("expected the parent dispatch to crash")
    expect(settled.error).toBeInstanceOf(WorkerCrashedError)
    const error = settled.error as WorkerCrashedError
    expect(error.message).toContain("db.select is not a function")
    expect(error.cause).toBe(cause)
    expect(await child).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(cancelled).toContain(sid("ses_child"))

    await dispatcher.terminateAll()
  })

  test("a crash cascade reaps each cancelled child's worker", async () => {
    const pool = createFakeWorkerPool(1)
    const reaped: number[] = []
    const config: CancellationConfig = {
      childrenOf: (parent) => (parent === sid("ses_parent") ? [sid("ses_child")] : []),
      reap: (worker) => reaped.push(worker.id),
    }
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    pool.spawned[0].crash(sid("ses_parent"))

    expect(await parent).toEqual({ ok: false, error: expect.any(WorkerCrashedError) })
    expect(await child).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    // The child ran on its own per-session worker; the terminate path reaped it.
    expect(reaped).toEqual([pool.spawned[1].id])

    await dispatcher.terminateAll()
  })

  test("a parent reply-timeout cancels its running subagent child", async () => {
    const pool = createFakeWorkerPool(1)
    const { config, cancelled } = cancellation({ ses_parent: ["ses_child"] })
    const dispatcher = createDispatcher({ pool, cancellation: config })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    pool.spawned[0].fail(sid("ses_parent"), new SessionWorkerReplyTimeoutError(30_000))

    expect(await parent).toEqual({ ok: false, error: expect.any(SessionWorkerReplyTimeoutError) })
    expect(await child).toEqual({ ok: false, error: expect.any(SessionCancelledError) })
    expect(cancelled).toContain(sid("ses_child"))
    expect(dispatcher.registry.has(sid("ses_child"))).toBe(false)

    await dispatcher.terminateAll()
  })
})
