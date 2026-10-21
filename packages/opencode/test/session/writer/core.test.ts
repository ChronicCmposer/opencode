// Phase 7 — writer core tests (plan tasks 7.1, 7.4, 7.5, 7.6, 7.8, 7.10, 7.11,
// 7.12, 7.14, 7.15, 7.17, 7.18, 7.19).
//
// The core is driven in-process against a real in-memory `bun:sqlite` connection so
// transaction and sequence semantics are exercised for real, without a Worker.

import { afterEach, describe, expect, test } from "bun:test"
import { createBunSqliteConnection } from "@/session/writer/connection"
import {
  AbortedWriteError,
  OwnerMismatchError,
  SequenceError,
  TransactionError,
  WriterBusyExhaustedError,
  WriterClosedError,
  createWriterCore,
  type WriterCore,
  type WriterCoreOptions,
} from "@/session/writer/core"
import { FAULTS_ENV } from "@/testing/faults"

const SCHEMA = `
CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT);
CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL, data TEXT NOT NULL);
CREATE TABLE projector_log (aggregate_id TEXT, seq INTEGER NOT NULL, type TEXT);
`

const originalGate = process.env[FAULTS_ENV]
afterEach(() => {
  if (originalGate === undefined) delete process.env[FAULTS_ENV]
  else process.env[FAULTS_ENV] = originalGate
})

interface Harness {
  readonly core: WriterCore
  readonly rows: (sql: string, params?: ReadonlyArray<unknown>) => ReadonlyArray<Record<string, unknown>>
}

function harness(options: Omit<WriterCoreOptions, "connection"> = {}): Harness {
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(SCHEMA)
  const core = createWriterCore({ connection, sleep: async () => {}, ...options })
  return { core, rows: (sql, params = []) => connection.all(sql, params) }
}

let eventCounter = 0
const ev = (type: string, aggregateID: string, data: Record<string, unknown> = {}) => ({
  id: `evt-${++eventCounter}`,
  type,
  aggregateID,
  data,
})

const sequenceOf = (rows: ReadonlyArray<Record<string, unknown>>): number[] => rows.map((row) => Number(row.seq))

describe("writer core: serialization and sequence allocation (7.1, 7.10, 7.17)", () => {
  test("concurrent publishes serialize with contiguous per-aggregate sequences", async () => {
    const { core, rows } = harness()
    await Promise.all([
      core.publish({ event: ev("t", "a"), durable: true }),
      core.publish({ event: ev("t", "b"), durable: true }),
      core.publish({ event: ev("t", "a"), durable: true }),
      core.publish({ event: ev("t", "b"), durable: true }),
      core.publish({ event: ev("t", "a"), durable: true }),
    ])

    const a = rows("SELECT seq FROM event WHERE aggregate_id = 'a' ORDER BY seq")
    const b = rows("SELECT seq FROM event WHERE aggregate_id = 'b' ORDER BY seq")
    expect(sequenceOf(a)).toEqual([0, 1, 2])
    expect(sequenceOf(b)).toEqual([0, 1])
  })

  test("the writer, not the caller, assigns the sequence", async () => {
    const { core } = harness()
    const first = await core.publish({ event: ev("t", "a"), durable: true })
    const second = await core.publish({ event: ev("t", "a"), durable: true })

    expect(first).toMatchObject({ seq: 0, committed: true })
    expect(second).toMatchObject({ seq: 1, committed: true })
  })

  test("a non-durable publish carries no sequence and writes nothing (7.5)", async () => {
    const { core, rows } = harness()
    const result = await core.publish({ event: ev("t", "a"), durable: false })

    expect(result).toEqual({ seq: null, committed: false, ordinal: 0, idempotent: false })
    expect(rows("SELECT seq FROM event_sequence")).toEqual([])
    expect(rows("SELECT id FROM event")).toEqual([])
  })

  test("only per-aggregate sequence state exists; no global column (7.17)", () => {
    const { rows } = harness()
    const columns = rows("PRAGMA table_info(event_sequence)").map((row) => row.name)
    expect(columns).toEqual(["aggregate_id", "seq", "owner_id"])
  })
})

describe("writer core: durable projectors are atomic (7.6, 7.12)", () => {
  test("a registered SQL projector commits with the event", async () => {
    const { core, rows } = harness()
    await core.registerProjector("log", "t", "INSERT INTO projector_log (aggregate_id, seq, type) VALUES ($aggregate_id, $seq, $type)")
    const result = await core.publish({ event: ev("t", "a"), durable: true, commitId: "log" })

    expect(result.seq).toBe(0)
    expect(rows("SELECT aggregate_id, seq, type FROM projector_log")).toEqual([
      { aggregate_id: "a", seq: 0, type: "t" },
    ])
  })

  test("a throwing projector rolls the whole commit back: no event, no sequence, no projection", async () => {
    const { core, rows } = harness()
    await core.registerProjector("log", "t", "INSERT INTO projector_log (aggregate_id, seq, type) VALUES ($aggregate_id, $seq, $type)")

    await expect(
      core.publish({
        event: ev("t", "a"),
        durable: true,
        commitId: "log",
        commit: () => {
          throw new Error("projector exploded")
        },
      }),
    ).rejects.toThrow("projector exploded")

    expect(rows("SELECT id FROM event")).toEqual([])
    expect(rows("SELECT seq FROM event_sequence")).toEqual([])
    expect(rows("SELECT aggregate_id FROM projector_log")).toEqual([])
  })
})

describe("writer core: owner fencing and steal (7.14)", () => {
  test("first owner wins; a mismatched live publish is rejected", async () => {
    const { core } = harness()
    await core.publish({ event: ev("t", "a"), durable: true, ownerID: "o1" })

    await expect(core.publish({ event: ev("t", "a"), durable: true, ownerID: "o2" })).rejects.toBeInstanceOf(
      OwnerMismatchError,
    )
    // no sequence was consumed by the rejected publish
    const result = await core.publish({ event: ev("t", "a"), durable: true, ownerID: "o1" })
    expect(result.seq).toBe(1)
  })

  test("strictOwner rejects a mismatch, and steal reassigns the owner", async () => {
    const { core, rows } = harness()
    await core.publish({ event: ev("t", "a"), durable: true, ownerID: "o1" })

    await expect(
      core.publish({ event: ev("t", "a"), durable: true, ownerID: "o2", strictOwner: true }),
    ).rejects.toBeInstanceOf(OwnerMismatchError)

    await core.steal("a", "o2")
    expect(rows("SELECT owner_id FROM event_sequence WHERE aggregate_id = 'a'")).toEqual([{ owner_id: "o2" }])
    const result = await core.publish({ event: ev("t", "a"), durable: true, ownerID: "o2" })
    expect(result.seq).toBe(1)
  })

  test("claim reassigns unconditionally (last wins)", async () => {
    const { core, rows } = harness()
    await core.publish({ event: ev("t", "a"), durable: true, ownerID: "o1" })
    await core.claim("a", "o3")
    expect(rows("SELECT owner_id FROM event_sequence WHERE aggregate_id = 'a'")).toEqual([{ owner_id: "o3" }])
  })
})

describe("writer core: SQLITE_BUSY retry (7.8)", () => {
  test("a forced busy at commit retries with exponential backoff then rejects typed", async () => {
    process.env[FAULTS_ENV] = "failCommitAt@writer:commit"
    const delays: number[] = []
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    connection.exec(SCHEMA)
    const core = createWriterCore({
      connection,
      sleep: async (ms) => {
        delays.push(ms)
      },
    })

    await expect(core.publish({ event: ev("t", "a"), durable: true })).rejects.toBeInstanceOf(
      WriterBusyExhaustedError,
    )
    expect(delays).toEqual([10, 100, 1000, 1000, 1000])
  })

  test("a busy that clears before exhaustion succeeds and consumes one sequence", async () => {
    process.env[FAULTS_ENV] = "failCommitAt@writer:commit"
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    connection.exec(SCHEMA)
    let attempts = 0
    const core = createWriterCore({
      connection,
      sleep: async () => {
        attempts += 1
        // clear the fault after the first backoff so the retry can commit
        delete process.env[FAULTS_ENV]
      },
    })

    const result = await core.publish({ event: ev("t", "a"), durable: true })
    expect(attempts).toBe(1)
    expect(result).toMatchObject({ seq: 0, committed: true })
  })
})

describe("writer core: reserved-handle transactions (7.4)", () => {
  test("a committed pinned transaction persists its writes", async () => {
    const { core, rows } = harness()
    const handle = await core.beginTransaction()
    await core.txQuery(handle, "run", "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      "x",
      "a",
      0,
      "t",
      "{}",
    ])
    await core.commitTransaction(handle)

    expect(rows("SELECT id FROM event")).toEqual([{ id: "x" }])
  })

  test("a rolled-back pinned transaction discards its writes", async () => {
    const { core, rows } = harness()
    const handle = await core.beginTransaction()
    await core.txQuery(handle, "run", "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      "x",
      "a",
      0,
      "t",
      "{}",
    ])
    await core.rollbackTransaction(handle)

    expect(rows("SELECT id FROM event")).toEqual([])
  })

  test("a second pinned transaction is rejected while one is open", async () => {
    const { core } = harness()
    await core.beginTransaction()
    await expect(core.beginTransaction()).rejects.toBeInstanceOf(TransactionError)
  })

  test("an idle pinned transaction is rolled back after the idle window", async () => {
    const { core, rows } = harness({ txIdleMs: 20 })
    const handle = await core.beginTransaction()
    await core.txQuery(handle, "run", "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
      "x",
      "a",
      0,
      "t",
      "{}",
    ])

    await new Promise((resolve) => setTimeout(resolve, 60))
    expect(rows("SELECT id FROM event")).toEqual([])
    await expect(core.txQuery(handle, "all", "SELECT 1")).rejects.toBeInstanceOf(TransactionError)
  })

  test("requester death mid-write aborts with no sequence consumed (7.11)", async () => {
    process.env[FAULTS_ENV] = "abortTxAfter@writer:tx"
    const { core, rows } = harness()
    await expect(core.publish({ event: ev("t", "a"), durable: true })).rejects.toBeInstanceOf(AbortedWriteError)
    expect(rows("SELECT seq FROM event_sequence")).toEqual([])

    delete process.env[FAULTS_ENV]
    const result = await core.publish({ event: ev("t", "a"), durable: true })
    expect(result.seq).toBe(0)
  })

  test("a requester death mid-transaction releases the pinned transaction", async () => {
    process.env[FAULTS_ENV] = "abortTxAfter@writer:tx"
    const { core, rows } = harness()
    const handle = await core.beginTransaction()
    await expect(
      core.txQuery(handle, "run", "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
        "x",
        "a",
        0,
        "t",
        "{}",
      ]),
    ).rejects.toBeInstanceOf(AbortedWriteError)

    // The transaction was abandoned and its write discarded; a fresh one can pin.
    delete process.env[FAULTS_ENV]
    expect(rows("SELECT id FROM event")).toEqual([])
    const next = await core.beginTransaction()
    expect(next).not.toBe(handle)
    await core.rollbackTransaction(next)
  })
})

describe("writer core: replay idempotency (7.19)", () => {
  test("replaying the latest sequence with identical data is a no-op", async () => {
    const { core, rows } = harness()
    const event = ev("t", "a", { v: 1 })
    await core.publish({ event, durable: true })

    const replay = await core.replay({ event, seq: 0 })
    expect(replay).toMatchObject({ seq: 0, committed: false, idempotent: true })
    expect(rows("SELECT id FROM event")).toHaveLength(1)
  })

  test("a diverged replay fails loud", async () => {
    const { core } = harness()
    const event = ev("t", "a", { v: 1 })
    await core.publish({ event, durable: true })

    await expect(core.replay({ event: ev("t", "a", { v: 2 }), seq: 0 })).rejects.toBeInstanceOf(SequenceError)
  })

  test("a replay beyond the next sequence is rejected, and the next one commits", async () => {
    const { core } = harness()
    const event = ev("t", "a", { v: 1 })
    await core.publish({ event, durable: true })

    await expect(core.replay({ event: ev("t", "a", { v: 9 }), seq: 5 })).rejects.toBeInstanceOf(SequenceError)
    const committed = await core.replay({ event: ev("t", "a", { v: 2 }), seq: 1 })
    expect(committed).toMatchObject({ seq: 1, committed: true })
  })
})

describe("writer core: write-through read cache (7.18)", () => {
  test("a repeated read is served from cache and invalidated by the writer's own write", async () => {
    const { core } = harness()
    const sql = "SELECT seq FROM event_sequence WHERE aggregate_id = ?"

    expect(await core.query("all", sql, ["a"])).toEqual([])
    expect(await core.query("all", sql, ["a"])).toEqual([])
    expect(core.cacheStats().hits).toBe(1)

    await core.publish({ event: ev("t", "a"), durable: true })
    expect(await core.query("all", sql, ["a"])).toEqual([{ seq: 0 }])
  })

  test("caching can be bypassed per query", async () => {
    const { core } = harness()
    const sql = "SELECT 1 AS one"
    await core.query("all", sql, [], { cache: false })
    await core.query("all", sql, [], { cache: false })
    expect(core.cacheStats().hits).toBe(0)
  })
})

describe("writer core: bounded queue backpressure (7.15)", () => {
  test("overflow callers backpressure instead of dropping work", async () => {
    const { core, rows } = harness({ maxQueue: 1 })
    let release: () => void = () => {}
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })

    const first = core.publish({ event: ev("t", "a"), durable: true, commit: async () => gate })
    const rest = [
      core.publish({ event: ev("t", "a"), durable: true }),
      core.publish({ event: ev("t", "a"), durable: true }),
      core.publish({ event: ev("t", "a"), durable: true }),
    ]

    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(core.pending().overflow).toBeGreaterThan(0)
    expect(core.depth()).toBeLessThanOrEqual(2)

    release()
    const results = await Promise.all([first, ...rest])
    expect(results.map((result) => result.seq)).toEqual([0, 1, 2, 3])
    expect(sequenceOf(rows("SELECT seq FROM event WHERE aggregate_id = 'a' ORDER BY seq"))).toEqual([0, 1, 2, 3])
  })
})

describe("writer core: remove (7.13)", () => {
  test("remove deletes both the events and the sequence", async () => {
    const { core, rows } = harness()
    await core.publish({ event: ev("t", "a"), durable: true })
    await core.publish({ event: ev("t", "a"), durable: true })
    await core.remove("a")

    expect(rows("SELECT id FROM event WHERE aggregate_id = 'a'")).toEqual([])
    expect(rows("SELECT seq FROM event_sequence WHERE aggregate_id = 'a'")).toEqual([])
  })
})

describe("writer core: lifecycle", () => {
  test("close rejects queued operations instead of orphaning them", async () => {
    const { core } = harness()
    await core.beginTransaction()
    const queued = core.query("all", "SELECT 1")

    await core.close()
    await expect(queued).rejects.toBeInstanceOf(WriterClosedError)
  })
})
