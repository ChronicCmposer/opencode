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
  SessionWorkerReplyTimeoutError,
  type ExecutorTransport,
} from "@/session/worker/execute"
import { createWorkerHarness, type WorkerHarness } from "../../lib/worker-harness"
import { spawnTransport } from "../../lib/transport"
import type { WorkerLogger } from "@/session/worker/log"

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

describe("12 B1 worker spawning over real worker handles", () => {
  test("every spawned executor worker logs 'worker spawning' with its id and kind", async () => {
    const harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const entries: Array<{ message: string; fields: Record<string, unknown> }> = []
    const log: WorkerLogger = (message, fields) => entries.push({ message, fields })
    const pool = createExecutorWorkerPool({
      k: 2,
      spawn: (id) => createExecutorWorker({ id, transport: spawnTransport(harness), workKind: "slow", log }),
      log,
    })

    try {
      const spawning = entries.filter((entry) => entry.message === "session workers: worker spawning")
      expect(spawning.map((entry) => entry.fields)).toEqual([
        { workerID: 1, kind: "slow", reason: "initial" },
        { workerID: 2, kind: "slow", reason: "initial" },
      ])

      pool.spawn("headroom")
      expect(entries.filter((entry) => entry.message === "session workers: worker spawning")).toHaveLength(3)
    } finally {
      await pool.terminateAll().catch(() => undefined)
      await harness.terminateAll().catch(() => undefined)
    }
  })

  test("a spawn bound to a session logs it with the given reason", () => {
    const entries: Array<{ message: string; fields: Record<string, unknown> }> = []
    const transport: ExecutorTransport = {
      post: () => {},
      waitFor: async () => {
        throw new Error("no reply expected before a post")
      },
      alive: true,
      terminate: async () => 0,
    }
    createExecutorWorker({
      id: 3,
      transport,
      workKind: "session",
      sessionID: sid("ses_spawn"),
      reason: "rebind",
      log: (message, fields) => entries.push({ message, fields }),
    })

    expect(entries).toEqual([
      {
        message: "session workers: worker spawning",
        fields: { workerID: 3, kind: "session", sessionID: "ses_spawn", reason: "rebind" },
      },
    ])
  })
})

describe("a run is never bounded by the control-frame deadline", () => {
  test("a slow run resolves despite a tiny timeoutMs", async () => {
    const harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const worker = createExecutorWorker({
      id: 1,
      transport: spawnTransport(harness),
      workKind: "slow",
      timeoutMs: 50,
    })
    try {
      // `slow` is 200ms, well past the 50ms control-frame budget: only an
      // unbounded reply wait lets this resolve instead of timing out.
      await worker.execute({ sessionID: sid("ses_slow"), depth: 0 })
      expect(worker.resultFor(sid("ses_slow"))?.sessionID).toBe("ses_slow")
    } finally {
      await harness.terminateAll().catch(() => undefined)
    }
  })

  test("a control frame still times out under the same tiny timeoutMs", async () => {
    const seen: Array<number | undefined> = []
    const transport: ExecutorTransport = {
      post: () => {},
      waitFor: (_match, options) => {
        seen.push(options?.timeoutMs)
        const waitMs = options?.timeoutMs ?? 30_000
        if (waitMs <= 0) return new Promise(() => {})
        return new Promise((_resolve, reject) =>
          setTimeout(() => reject(new SessionWorkerReplyTimeoutError(waitMs)), waitMs),
        )
      },
      alive: true,
      terminate: async () => 0,
    }
    const worker = createExecutorWorker({ id: 2, transport, workKind: "slow", timeoutMs: 50 })

    await expect(worker.assertNotBusy(sid("ses_busy"))).rejects.toBeInstanceOf(SessionWorkerReplyTimeoutError)
    expect(seen).toEqual([50])
  })
})

describe("liveness watchdog: a wedged run is surfaced, a healthy one is not", () => {
  /**
   * A transport whose worker never answers anything: the run reply never arrives
   * and a `ping` is never answered. A `timeoutMs <= 0` wait has no deadline (the
   * run reply); any positive budget elapses and rejects like the real transport.
   */
  const wedgedTransport = (): ExecutorTransport => ({
    post: () => {},
    waitFor: (_match, options) => {
      const waitMs = options?.timeoutMs ?? 30_000
      if (waitMs <= 0) return new Promise(() => {})
      return new Promise((_resolve, reject) =>
        setTimeout(() => reject(new SessionWorkerReplyTimeoutError(waitMs)), waitMs),
      )
    },
    alive: true,
    terminate: async () => 0,
  })

  test("a run whose worker never answers a ping rejects with SessionWorkerReplyTimeoutError", async () => {
    const worker = createExecutorWorker({ id: 1, transport: wedgedTransport(), workKind: "slow", runIdleTimeoutMs: 25 })

    expect(await capture(worker.execute({ sessionID: sid("ses_wedged"), depth: 0 }))).toBeInstanceOf(
      SessionWorkerReplyTimeoutError,
    )
  })

  test("a healthy slow run is kept alive by the worker's pongs", async () => {
    const harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const transport = spawnTransport(harness)
    // Boot the isolate before timing it: the watchdog's first probe must not race
    // module load, only measure whether a RUNNING worker still answers pings.
    await transport.waitFor((message) => message.type === "ready", { timeoutMs: 10_000 })
    // `slow` is 200ms, several intervals at 50ms: only a live worker answering
    // pings lets the run resolve instead of tripping the watchdog.
    const worker = createExecutorWorker({ id: 1, transport, workKind: "slow", runIdleTimeoutMs: 50 })
    try {
      await worker.execute({ sessionID: sid("ses_healthy"), depth: 0 })
      expect(worker.resultFor(sid("ses_healthy"))?.sessionID).toBe("ses_healthy")
    } finally {
      await harness.terminateAll().catch(() => undefined)
    }
  })

  test("runIdleTimeoutMs 0 disables the watchdog: a slow run still completes", async () => {
    const harness = createWorkerHarness({ cores: 4, timeoutMs: 10_000 })
    const worker = createExecutorWorker({
      id: 1,
      transport: spawnTransport(harness),
      workKind: "slow",
      runIdleTimeoutMs: 0,
    })
    try {
      await worker.execute({ sessionID: sid("ses_disabled"), depth: 0 })
      expect(worker.resultFor(sid("ses_disabled"))?.sessionID).toBe("ses_disabled")
    } finally {
      await harness.terminateAll().catch(() => undefined)
    }
  })
})
