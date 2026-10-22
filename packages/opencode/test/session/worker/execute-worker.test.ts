// Phase 5.1–5.5 — session dispatch on the live V1 path, over real Bun workers.
//
// A real `createDispatcher` (Phase 4) schedules sessions onto executor workers
// (`execute.ts`) whose run-state machines live inside the isolates. The hub
// keeps only the key→worker map + admission; the hub event loop stays free while
// a worker runs a session.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import { createDispatcher, SessionCancelledError } from "@/session/worker/dispatch"
import {
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionDispatch,
  SessionBusyError,
} from "@/session/worker/execute"
import { createWorkerHarness, type WorkerHarness } from "../../lib/worker-harness"
import { spawnTransport } from "../../lib/transport"

const sid = (value: string) => value as SessionID

async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`timed out: ${message}`)
}

/** Settle a promise into its value-or-error so it can be asserted on. */
const capture = (promise: Promise<unknown>): Promise<unknown> =>
  promise.then(
    () => undefined,
    (error: unknown) => error,
  )

interface Rig extends AsyncDisposable {
  readonly harness: WorkerHarness
  readonly dispatch: ReturnType<typeof createSessionDispatch>
  readonly dispatcher: ReturnType<typeof createDispatcher>
  readonly pool: ReturnType<typeof createExecutorWorkerPool>
}

function makeRig(k: number): Rig {
  const harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
  const pool = createExecutorWorkerPool({
    k,
    spawn: (id) => createExecutorWorker({ id, transport: spawnTransport(harness), workKind: "slow" }),
  })
  const dispatcher = createDispatcher({ pool })
  const dispatch = createSessionDispatch({ dispatcher, pool })
  return {
    harness,
    dispatch,
    dispatcher,
    pool,
    async [Symbol.asyncDispose]() {
      await dispatcher.terminateAll().catch(() => undefined)
      await harness.terminateAll().catch(() => undefined)
    },
  }
}

describe("5.1 a V1 session run executes in a worker with the hub loop idle", () => {
  test("the hub ticks while the worker runs, then the result round-trips", async () => {
    await using rig = makeRig(1)
    let settled = false
    const running = rig.dispatch.ensureRunning(sid("ses_a")).then((value) => {
      settled = true
      return value
    })

    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(settled).toBe(false)

    const result = await running
    expect(result.sessionID).toBe("ses_a")
    expect(result.value).toBeDefined()
  })
})

describe("5.3 per-key serialization is preserved", () => {
  test("two dispatches for one key join a single worker execution", async () => {
    await using rig = makeRig(1)
    const first = rig.dispatch.ensureRunning(sid("ses_x"))
    const second = rig.dispatch.ensureRunning(sid("ses_x"))
    const [a, b] = await Promise.all([first, second])

    expect(a).toEqual(b)
    expect(rig.dispatcher.registry.size).toBe(0)
  })
})

describe("5.4 RunnerBusy / assertNotBusy hold worker-local; the hub forwards control", () => {
  test("assertNotBusy answers from the worker and startShell rejects while running", async () => {
    await using rig = makeRig(1)
    const running = rig.dispatch.ensureRunning(sid("ses_a"))
    await waitUntil(() => rig.dispatch.assertNotBusy(sid("ses_a")), "worker never became busy")

    expect(await capture(rig.dispatch.startShell(sid("ses_a")))).toBeInstanceOf(SessionBusyError)

    await running
    expect(await rig.dispatch.assertNotBusy(sid("ses_a"))).toBe(false)
  })
})

describe("5.5 different sessions run concurrently on different workers", () => {
  test("K=2 runs two sessions at once", async () => {
    await using rig = makeRig(2)
    const a = rig.dispatch.ensureRunning(sid("ses_a"))
    const b = rig.dispatch.ensureRunning(sid("ses_b"))

    await waitUntil(
      async () => (await rig.dispatch.assertNotBusy(sid("ses_a"))) && (await rig.dispatch.assertNotBusy(sid("ses_b"))),
      "both sessions were never concurrently busy",
    )

    const [ra, rb] = await Promise.all([a, b])
    expect(ra.sessionID).toBe("ses_a")
    expect(rb.sessionID).toBe("ses_b")
  })
})

describe("5.4 hub forwards cancellation", () => {
  test("cancelling a running session rejects the caller", async () => {
    await using rig = makeRig(1)
    const running = rig.dispatch.ensureRunning(sid("ses_a"))
    await waitUntil(() => rig.dispatch.assertNotBusy(sid("ses_a")), "worker never became busy")

    await rig.dispatch.cancel(sid("ses_a"))
    await expect(running).rejects.toBeInstanceOf(SessionCancelledError)
  })

  test("a cancelled worker session leaves the worker run-state clean and reusable", async () => {
    await using rig = makeRig(1)
    const running = rig.dispatch.ensureRunning(sid("ses_a"))
    await waitUntil(() => rig.dispatch.assertNotBusy(sid("ses_a")), "worker never became busy")

    await rig.dispatch.cancel(sid("ses_a"))
    await expect(running).rejects.toBeInstanceOf(SessionCancelledError)

    // The worker's per-key run-state is idle after the cancel, so a fresh
    // session runs to completion on the same worker.
    expect(await rig.dispatch.assertNotBusy(sid("ses_a"))).toBe(false)
    const next = await rig.dispatch.ensureRunning(sid("ses_b"))
    expect(next.sessionID).toBe("ses_b")
  })
})
