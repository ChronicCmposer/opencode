import { describe, expect, test } from "bun:test"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { createDispatcher, ParentCrashedError, SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
import { WORKER_CRASH_PHASES, crashes, record, type WorkerCrashPhase, type WorkerCrashed } from "@/session/ledger/crash"
import { LEDGER_SQL, type LedgerConnection } from "@/session/ledger/tool-call"
import { capture, flush, sid } from "../../lib/async"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"

function ledgerConnection(): LedgerConnection {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(LEDGER_SQL)
  return connection
}

function crashDetails(phase: WorkerCrashPhase, lastCommittedMessageID: string | null, inFlightCallIDs: string[]) {
  return { phase, lastCommittedMessageID, inFlightCallIDs }
}

describe("WorkerCrashed crash matrix (W3.3/W3.4 / D8)", () => {
  test("all five phases persist and return the same payload, retryable false", async () => {
    const connection = ledgerConnection()
    const recorded: WorkerCrashed[] = []
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      recordCrash: (payload) => {
        recorded.push(payload)
        connection.run(
          "INSERT INTO worker_crash (crash_id, session_id, worker_id, phase, last_committed_message_id, in_flight_call_ids, retryable, reason, time_created) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
          [
            `crash_${payload.phase}`,
            payload.sessionID,
            payload.workerID,
            payload.phase,
            payload.lastCommittedMessageID,
            JSON.stringify(payload.inFlightCallIDs),
            payload.reason,
            1,
          ],
        )
      },
      crashState: (sessionID) => {
        const phase = sessionID.replace("ses_", "").split("_")[0] as WorkerCrashPhase
        return crashDetails(phase, phase === "boot" ? null : "msg_1", phase === "tool" ? ["call_1"] : [])
      },
    })

    for (const phase of WORKER_CRASH_PHASES) {
      const sessionID = sid(`ses_${phase}_1`)
      const run = capture(dispatcher.dispatch({ sessionID }))
      await flush()
      pool.spawned[pool.spawned.length - 1]!.crash(sessionID)
      const settled = await run
      if (settled.ok) throw new Error("expected a crash")
      expect(settled.error).toBeInstanceOf(WorkerCrashedError)
      const error = settled.error as WorkerCrashedError
      expect(error.payload).toMatchObject({ sessionID, phase, retryable: false })
      expect(recorded[recorded.length - 1]?.phase).toBe(phase)
    }

    const stored = crashes(connection, "ses_stream_1")
    expect(stored).toHaveLength(1)
    expect(stored[0]).toMatchObject({ phase: "stream", lastCommittedMessageID: "msg_1", retryable: false })
    expect(recorded.find((payload) => payload.phase === "tool")?.inFlightCallIDs).toEqual(["call_1"])
    await dispatcher.terminateAll()
  })

  test("a crash after COMMIT keeps the committed message and never re-executes", async () => {
    const connection = ledgerConnection()
    const recorded: WorkerCrashed[] = []
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      recordCrash: (payload) => {
        recorded.push(payload)
        record(connection, { ...payload, timeCreated: 1 })
      },
      crashState: () => crashDetails("commit", "msg_committed", []),
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_commit") }))
    await flush()
    const worker = pool.spawned[0]!
    worker.crash(sid("ses_commit"))
    const settled = await run
    if (settled.ok) throw new Error("expected a crash")

    const payload = (settled.error as WorkerCrashedError).payload
    expect(payload).toMatchObject({
      phase: "commit",
      lastCommittedMessageID: "msg_committed",
      inFlightCallIDs: [],
      retryable: false,
    })
    expect(recorded).toEqual([payload!])
    expect(worker.runs).toHaveLength(1)
    expect(dispatcher.queuedCount).toBe(0)
    expect(crashes(connection, "ses_commit")).toHaveLength(1)
    expect(crashes(connection, "ses_commit")[0]).toMatchObject({
      phase: "commit",
      lastCommittedMessageID: "msg_committed",
    })

    await dispatcher.terminateAll()
  })

  test("a dead worker is replaced, never reused, and its id is monotonic", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool })
    const first = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    const dead = pool.spawned[0]!
    dead.crash(sid("ses_1"))
    await first

    const second = capture(dispatcher.dispatch({ sessionID: sid("ses_1") }))
    await flush()
    expect(pool.spawned).toHaveLength(2)
    expect(pool.spawned[1]).not.toBe(dead)
    expect(pool.spawned[1]!.id).toBeGreaterThan(dead.id)
    expect(pool.spawned[1]!.runs.map((run) => run.sessionID)).toEqual([sid("ses_1")])
    pool.spawned[1]!.finish(sid("ses_1"))
    await second
    await dispatcher.terminateAll()
  })

  test("a parent crash cancels its child with ParentCrashed", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({
      pool,
      cancellation: {
        childrenOf: (parent) => (parent === sid("ses_parent") ? [sid("ses_child")] : []),
      },
    })

    const parent = capture(dispatcher.dispatch({ sessionID: sid("ses_parent") }))
    const child = capture(dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
    await flush()

    pool.spawned[0]!.crash(sid("ses_parent"))
    const parentResult = await parent
    const childResult = await child
    if (parentResult.ok || childResult.ok) throw new Error("expected both runs to settle")
    expect(parentResult.error).toBeInstanceOf(WorkerCrashedError)
    expect(childResult.error).toBeInstanceOf(ParentCrashedError)
    expect(childResult.error).toBeInstanceOf(SessionCancelledError)
    expect(dispatcher.registry.has(sid("ses_child"))).toBe(false)
    await dispatcher.terminateAll()
  })

  test("an allowed auto-resume is reported once and never re-runs the tool", async () => {
    const pool = createFakeWorkerPool(1)
    const decisions: string[] = []
    const dispatcher = createDispatcher({
      pool,
      resumeDecision: (payload) =>
        payload.phase === "stream"
          ? { resume: true, sessionID: payload.sessionID, attempt: 1, phase: payload.phase, fromMessageID: "msg_1" }
          : { resume: false, sessionID: payload.sessionID, phase: payload.phase, reason: "phase" },
      onAutoResume: (decision) => decisions.push(`${decision.sessionID}:${decision.fromMessageID}`),
    })

    const run = capture(dispatcher.dispatch({ sessionID: sid("ses_auto") }))
    await flush()
    const worker = pool.spawned[0]!
    worker.crash(sid("ses_auto"))
    await run

    expect(decisions).toEqual(["ses_auto:msg_1"])
    expect(worker.runs).toHaveLength(1)
    expect(pool.spawned).toHaveLength(1)
    expect(dispatcher.queuedCount).toBe(0)
    await dispatcher.terminateAll()
  })
})
