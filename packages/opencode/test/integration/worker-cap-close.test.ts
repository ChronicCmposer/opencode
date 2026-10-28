// worker cap liveness/release and forced-close WAL release.
//
// 1. A foreground parent that awaits a `task` child must never deadlock the
//    cap: the child runs in depth headroom while the parent keeps its slot.
// 2. A forced terminate of a worker with an in-flight run must still close the
//    isolate's native WAL handle before the thread dies, so the hub can take a
//    `BEGIN IMMEDIATE` write lock without `SQLITE_BUSY`.
//
// The cap tests drive the production `createLazyExecutorWorkerPool` seam with
// scripted workers (the same seam the hub tests use); the forced-close test
// spawns a real Bun worker that holds a real `BEGIN IMMEDIATE` on a real WAL
// file, then terminates it through the production transport.

import { describe, expect, setDefaultTimeout, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { pathToFileURL } from "node:url"
import { spawnDepth } from "@/session/depth"
import { SessionCancelledError } from "@/session/worker/dispatch"
import {
  createExecutorWorker,
  SessionBusyError,
  SessionWorkerReplyTimeoutError,
  type ExecutorRun,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import {
  createBunExecutorTransport,
  createLazyExecutorWorkerPool,
  createProductionSessionHub,
  createWorkerCap,
} from "@/session/worker/hub"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { flush, pollUntil, settleOf, sid, withDeadline } from "../lib/async"
import { REAL_WORKER_TEST_TIMEOUT_MS } from "../lib/worker-timeouts"

// Bun resets the per-file default to 5s; this file boots real executor workers.
setDefaultTimeout(REAL_WORKER_TEST_TIMEOUT_MS)

/** A one-shot signal: `promise` resolves on the first `resolve()` call. */
function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolve!: () => void
  const promise = new Promise<void>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}

/** A settled phase set: the scripted pools report no commit/projector phases. */
const settled = Promise.resolve()
const settledPhases = () => ({ commits: settled, projectors: settled, tools: settled, promotion: settled })

/** A scripted `ExecutorWorker`: the cap wraps it exactly as production does. */
function scriptedWorker(id: number, execute: (run: ExecutorRun) => Promise<void>): ExecutorWorker {
  return {
    id,
    alive: true,
    execute,
    cancel: async () => {},
    assertNotBusy: async () => false,
    resultFor: () => undefined,
    terminate: async () => {},
  }
}

describe("worker cap: parent/child liveness (1 core)", () => {
  test("a depth-1 child starts while its parent holds the only foreground slot", async () => {
    const childStarted = deferred()
    const childRelease = deferred()
    let pool: ExecutorWorkerPool

    pool = createLazyExecutorWorkerPool({
      maxRunning: 1,
      create: (id) =>
        scriptedWorker(id, async (run) => {
          if (run.depth === 0) {
            // The foreground `task` child is dispatched and awaited while the
            // parent still owns its run slot.
            const child = pool.spawn(1000 + id)
            await child.execute({ sessionID: sid(`ses_child_${id}`), depth: 1 })
            return
          }
          childStarted.resolve()
          await childRelease.promise
        }),
    })

    const run = pool.spawn(1).execute({ sessionID: sid("ses_parent"), depth: 0 })
    await withDeadline(
      childStarted.promise,
      2_000,
      "cap deadlock: a depth-1 child never started while its parent held the only slot",
    )
    childRelease.resolve()
    await withDeadline(run, 2_000, "the parent run never completed after its child settled")
  })

  test("N parents each start their foreground child across one core (all complete)", async () => {
    const count = 6
    const completed: number[] = []
    const children: number[] = []
    let pool: ExecutorWorkerPool

    pool = createLazyExecutorWorkerPool({
      maxRunning: 1,
      create: (id) =>
        scriptedWorker(id, async (run) => {
          if (run.depth === 0) {
            const child = pool.spawn(1000 + id)
            await child.execute({ sessionID: sid(`ses_child_${id}`), depth: 1 })
            completed.push(id)
            return
          }
          children.push(id)
        }),
    })

    const runs = Array.from({ length: count }, (_, index) =>
      pool.spawn(index + 1).execute({ sessionID: sid(`ses_parent_${index}`), depth: 0 }),
    )
    await withDeadline(Promise.all(runs), 5_000, "cap deadlock: a parent never completed across N parents")

    expect(completed.sort((a, b) => a - b)).toEqual(Array.from({ length: count }, (_, index) => index + 1))
    expect(children).toHaveLength(count)
  })

  test("foreground saturation stays bounded while a child runs in headroom", async () => {
    const rootGate = deferred()
    const childStarted = deferred()
    const childRelease = deferred()
    const started: number[] = []
    let pool: ExecutorWorkerPool

    pool = createLazyExecutorWorkerPool({
      maxRunning: 2,
      create: (id) =>
        scriptedWorker(id, async (run) => {
          if (run.depth === 0) {
            started.push(id)
            await rootGate.promise
            return
          }
          childStarted.resolve()
          await childRelease.promise
        }),
    })

    const roots = [1, 2, 3].map((id) =>
      pool.spawn(id).execute({ sessionID: sid(`ses_root_${id}`), depth: 0 }),
    )
    await pollUntil(() => started.length === 2, { message: "two roots never occupied the cap" })
    expect(started).toHaveLength(2)

    // A child starts in headroom even though the foreground cap is saturated and
    // a third root is still queued; headroom never consumes a foreground slot.
    const child = pool.spawn(50).execute({ sessionID: sid("ses_child_saturated"), depth: 1 })
    await withDeadline(childStarted.promise, 2_000, "a child starved behind a saturated foreground cap")
    expect(started).toHaveLength(2)

    rootGate.resolve()
    childRelease.resolve()
    await Promise.all([...roots, child])
  })
})

describe("production dispatch depth (metadata.depth -> cap headroom)", () => {
  test("a parent spawning a depth-1 child under maxRunningWorkers:1 never deadlocks", async () => {
    const parentStarted = deferred()
    const parentRelease = deferred()
    const childStarted = deferred()
    const parentID = sid("ses_prod_parent")
    const childID = sid("ses_prod_child")
    // `Session.spawn` stamps this on the child; the production hub resolves it.
    const metadata = new Map([
      [parentID, {}],
      [childID, { depth: 1 }],
    ])
    const results = new Map<string, SessionExecutionResult>()
    const hub = createProductionSessionHub({
      init: sessionWorkerInit(),
      count: 1,
      maxRunningWorkers: 1,
      barrierPhases: settledPhases,
      depthOf: (sessionID) => spawnDepth(metadata.get(sessionID)),
      poolFactory: (_resolveInit, _sinks, runtime) =>
        createLazyExecutorWorkerPool({
          maxRunning: runtime.maxRunning,
          create: (id): ExecutorWorker => ({
            id,
            alive: true,
            execute: async (run) => {
              if (run.sessionID === parentID) {
                parentStarted.resolve()
                await parentRelease.promise
              } else {
                childStarted.resolve()
              }
              results.set(run.sessionID, { sessionID: run.sessionID, value: { ok: true } })
            },
            cancel: async () => {},
            assertNotBusy: async () => false,
            resultFor: (sessionID) => results.get(sessionID),
            terminate: async () => {},
          }),
        }),
    })
    try {
      const parent = settleOf(hub.ensureRunning(parentID))
      await withDeadline(parentStarted.promise, 2_000, "the parent never took the only foreground slot")
      // The parent still holds the cap's one foreground slot while this child
      // dispatches; depth 1 must put it in headroom, not behind the parent.
      await withDeadline(
        hub.ensureRunning(childID),
        2_000,
        "cap deadlock: the production child ran at depth 0 and never started",
      )
      await withDeadline(childStarted.promise, 2_000, "the child run never started")
      parentRelease.resolve()
      expect((await withDeadline(parent, 2_000, "the parent never completed")).ok).toBe(true)
    } finally {
      await hub.terminateAll()
    }
  })

  test("an admitted run queued in cap.acquire reports busy, not idle", async () => {
    const parentStarted = deferred()
    const parentRelease = deferred()
    const parentID = sid("ses_busy_parent")
    const queuedID = sid("ses_busy_queued")
    const results = new Map<string, SessionExecutionResult>()
    const hub = createProductionSessionHub({
      init: sessionWorkerInit(),
      count: 1,
      maxRunningWorkers: 1,
      barrierPhases: settledPhases,
      depthOf: () => 0,
      poolFactory: (_resolveInit, _sinks, runtime) =>
        createLazyExecutorWorkerPool({
          maxRunning: runtime.maxRunning,
          create: (id): ExecutorWorker => ({
            id,
            alive: true,
            execute: async (run) => {
              if (run.sessionID === parentID) {
                parentStarted.resolve()
                await parentRelease.promise
              }
              results.set(run.sessionID, { sessionID: run.sessionID, value: { ok: true } })
            },
            cancel: async () => {},
            assertNotBusy: async () => false,
            resultFor: (sessionID) => results.get(sessionID),
            terminate: async () => {},
          }),
        }),
    })
    try {
      const parent = settleOf(hub.ensureRunning(parentID))
      await withDeadline(parentStarted.promise, 2_000, "the parent never took the only foreground slot")
      // This run is admitted (registry `running`) but its worker is still parked
      // in `cap.acquire`; it has no isolate for the worker-local probe to read.
      const queued = settleOf(hub.ensureRunning(queuedID))
      await pollUntil(() => hub.dispatcher.registry.get(queuedID) !== undefined, {
        message: "the queued run was never admitted",
      })
      expect(await hub.assertNotBusy(queuedID)).toBe(true)
      // `startShell` performs its own busy assertion through SessionDispatch.
      await expect(hub.startShell(queuedID)).rejects.toBeInstanceOf(SessionBusyError)

      parentRelease.resolve()
      expect((await withDeadline(parent, 2_000, "the parent never completed")).ok).toBe(true)
      expect((await withDeadline(queued, 2_000, "the queued run never completed")).ok).toBe(true)
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("worker cap: slot release matrix", () => {
  const cases: ReadonlyArray<{ readonly name: string; readonly run: () => Promise<void>; readonly ok: boolean }> = [
    { name: "idle completion", run: async () => {}, ok: true },
    {
      name: "crash",
      run: async () => {
        throw new Error("worker crashed")
      },
      ok: false,
    },
    {
      name: "timeout",
      run: async () => {
        throw new SessionWorkerReplyTimeoutError(1)
      },
      ok: false,
    },
    {
      name: "cancel",
      run: async () => {
        throw new SessionCancelledError({ message: "cancelled", sessionID: "ses_cancelled" })
      },
      ok: false,
    },
    {
      name: "boot failure",
      run: async () => {
        throw new Error("worker boot failed")
      },
      ok: false,
    },
  ]

  for (const scenario of cases) {
    test(`${scenario.name} releases exactly one slot`, async () => {
      const gate = deferred()
      const sentinelStarted: string[] = []
      const pool = createLazyExecutorWorkerPool({
        maxRunning: 1,
        create: (id) =>
          scriptedWorker(id, id === 1 ? scenario.run : async (run) => {
            sentinelStarted.push(run.sessionID)
            await gate.promise
          }),
      })

      const first = await settleOf(
        pool.spawn(1).execute({ sessionID: sid("ses_failure"), depth: 0 }),
      )
      expect(first.ok).toBe(scenario.ok)

      // With one slot, only one sentinel may start. A leaked slot leaves both
      // queued (never start); a double release lets both start at once.
      const second = pool.spawn(2).execute({ sessionID: sid("ses_sentinel_a"), depth: 0 })
      const third = pool.spawn(3).execute({ sessionID: sid("ses_sentinel_b"), depth: 0 })
      await pollUntil(() => sentinelStarted.length === 1, { message: "the released slot never admitted a sentinel" })
      await flush()
      expect(sentinelStarted).toHaveLength(1)

      gate.resolve()
      await Promise.all([second, third])
      expect(sentinelStarted).toHaveLength(2)
    })
  }

  test("a release is idempotent and can never free a second slot", async () => {
    const cap = createWorkerCap(1)
    const release = await cap.acquire(0)
    expect(cap.running).toBe(1)
    release()
    release()
    expect(cap.running).toBe(0)

    let admitted = false
    const next = cap.acquire(0).then(() => {
      admitted = true
    })
    await flush()
    expect(admitted).toBe(true)
    expect(cap.running).toBe(1)
    await next
  })
})

/** A real Bun worker that holds a real WAL write lock until it is shut down. */
const WAL_HOLDER_SOURCE = `
import { parentPort, workerData } from "node:worker_threads"
import { Database } from "bun:sqlite"

const db = new Database(workerData.filename)
db.exec("PRAGMA journal_mode = WAL")
db.exec("PRAGMA busy_timeout = 0")
let inTransaction = false

parentPort.on("message", (message) => {
  if (message?.type === "run") {
    if (!inTransaction) {
      db.exec("BEGIN IMMEDIATE")
      inTransaction = true
    }
    parentPort.postMessage({ type: "pong", id: 999 })
    return
  }
  if (message?.type === "shutdown") {
    try {
      if (inTransaction) db.exec("ROLLBACK")
    } catch {}
    try {
      db.close()
    } catch {}
    parentPort.postMessage({ type: "shutdown-ack", id: typeof message.id === "number" ? message.id : 0 })
  }
})
`

describe("forced close: terminate with an in-flight run releases the WAL lock", () => {
  test("the hub closes the native handle before terminating the isolate", async () => {
    const dir = mkdtempSync(join(tmpdir(), "cap-close-wal-"))
    const filename = join(dir, "opencode.db")
    const entry = join(dir, "wal-holder.ts")
    await Bun.write(entry, WAL_HOLDER_SOURCE)

    const admin = new Database(filename)
    admin.exec("PRAGMA journal_mode = WAL")
    admin.exec("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
    admin.close()

    const transport = createBunExecutorTransport({
      entry: pathToFileURL(entry),
      workerData: { filename },
      timeoutMs: 2_000,
    })
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    let probe: Database | undefined
    try {
      const run = settleOf(worker.execute({ sessionID: sid("ses_mid_tool"), depth: 0 }))
      await transport.waitFor((message) => message.type === "pong" && message.id === 999, {
        timeoutMs: 5_000,
      })

      probe = new Database(filename)
      probe.exec("PRAGMA busy_timeout = 0")
      // Sanity: the mid-tool worker really owns the write lock.
      expect(() => probe!.exec("BEGIN IMMEDIATE")).toThrow(/SQLITE_BUSY|database is locked/)

      await worker.terminate()
      expect((await run).ok).toBe(false)

      // The orderly shutdown closed the worker's native handle, so the hub can
      // take the write lock without SQLITE_BUSY.
      expect(() => probe!.exec("BEGIN IMMEDIATE")).not.toThrow()
      probe.exec("ROLLBACK")
    } finally {
      probe?.close()
      await worker.terminate().catch(() => undefined)
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
