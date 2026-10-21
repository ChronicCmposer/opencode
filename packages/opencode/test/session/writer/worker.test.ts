// Phase 7 — writer Worker integration tests (plan tasks 7.2, 7.3, 7.5, 7.7, 7.9,
// 7.13, 7.16).
//
// Drives the REAL worker isolate over a temp file database: the remote-query proxy,
// the admin RPC, the reserved-handle transaction protocol, read-your-writes, and
// at-most-once crash semantics with generation-scoped respawn.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { WriterCrashedError, createWriterClient } from "@/session/writer/client"
import { createRemoteConnection } from "@/session/writer/sql-proxy"
import { FAULTS_ENV } from "@/testing/faults"
import { tmpdir } from "../../fixture/fixture"
import { SCHEMA, ev, healthyEnv } from "./fixtures"

describe("writer worker: admin RPC and remote-query proxy (7.2, 7.3, 7.13)", () => {
  test("opens, migrates, proxies reads/writes, and checkpoints", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })

    const opened = await client.open(path.join(tmp.path, "writer.db"))
    expect(opened.opened).toBe(true)
    await client.migrate(SCHEMA)

    const tables = (await client.query(
      "all",
      "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    )) as Array<{ name: string }>
    expect(tables.map((row) => row.name)).toContain("event_sequence")

    await client.query("run", "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?)", [
      "a",
      0,
      null,
    ])
    expect(await client.query("all", "SELECT seq FROM event_sequence WHERE aggregate_id = ?", ["a"])).toEqual([
      { seq: 0 },
    ])

    await client.checkpoint()
  })

  test("a remote connection proxy forwards {method, sql, params} and returns typed rows", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))
    await client.migrate(SCHEMA)

    const connection = createRemoteConnection(client)
    const outcome = await connection.run("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?)", [
      "a",
      0,
      null,
    ])
    expect(outcome.changes).toBe(1)
    expect(await connection.all("SELECT seq FROM event_sequence WHERE aggregate_id = ?", ["a"])).toEqual([{ seq: 0 }])
    expect(await connection.values("SELECT seq FROM event_sequence WHERE aggregate_id = ?", ["a"])).toEqual([[0]])
    await connection.exec("DELETE FROM event_sequence")
    expect(await connection.all("SELECT seq FROM event_sequence")).toEqual([])
  })
})

describe("writer worker: durable publish and read-your-writes (7.5, 7.7)", () => {
  test("a durable publish returns {seq, committed} and is visible to the next read", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))
    await client.migrate(SCHEMA)

    const result = await client.publish(ev("t", "a"), { durable: true })
    expect(result).toMatchObject({ seq: 0, committed: true })
    expect(result.ordinal).toBeGreaterThan(0)

    expect(await client.query("all", "SELECT seq, type FROM event WHERE aggregate_id = ?", ["a"])).toEqual([
      { seq: 0, type: "t" },
    ])
    expect(await client.query("all", "SELECT seq FROM event_sequence WHERE aggregate_id = ?", ["a"])).toEqual([
      { seq: 0 },
    ])
  })

  test("a registered projector commits atomically with the event", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))
    await client.migrate(SCHEMA)

    await client.register(
      "log",
      "t",
      "INSERT INTO projector_log (aggregate_id, seq, type) VALUES ($aggregate_id, $seq, $type)",
    )
    await client.publish(ev("t", "a"), { durable: true, commitId: "log" })

    expect(await client.query("all", "SELECT aggregate_id, seq, type FROM projector_log")).toEqual([
      { aggregate_id: "a", seq: 0, type: "t" },
    ])
  })

  test("a non-durable publish carries no sequence", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))
    await client.migrate(SCHEMA)

    const result = await client.publish(ev("t", "a"), { durable: false })
    expect(result).toEqual({ seq: null, committed: false, ordinal: null, idempotent: false })
  })
})

describe("writer worker: reserved-handle transactions (7.4)", () => {
  test("a transaction pinned over the port commits or rolls back", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))
    await client.migrate(SCHEMA)

    const committed = await client.begin()
    await client.txQuery(committed, "run", "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?)", [
      "a",
      0,
      null,
    ])
    await client.commit(committed)

    const rolledBack = await client.begin()
    await client.txQuery(rolledBack, "run", "INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?)", [
      "b",
      0,
      null,
    ])
    await client.rollback(rolledBack)

    expect(await client.query("all", "SELECT aggregate_id FROM event_sequence ORDER BY aggregate_id")).toEqual([
      { aggregate_id: "a" },
    ])
  })
})

describe("writer worker: crash semantics (7.9, 7.16)", () => {
  test("a mid-commit crash is atomic, at-most-once, and respawns with a new generation", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "writer.db")

    // Establish a committed baseline with a healthy writer.
    {
      await using client = createWriterClient({ env: healthyEnv })
      await client.open(file)
      await client.migrate(SCHEMA)
      await client.publish(ev("t", "a", { v: 0 }), { durable: true })
    }

    const crashed: string[] = []
    const client = createWriterClient({
      env: { [FAULTS_ENV]: "crashWorker@writer:commit" },
      onCrash: (error) => crashed.push(error.message),
    })
    await client.open(file)
    expect(client.generation).toBe(1)

    // The publish reaches the commit boundary, the isolate exits, and the caller is
    // rejected — never retried (at-most-once).
    await expect(client.publish(ev("t", "a", { v: 1 }), { durable: true })).rejects.toBeInstanceOf(WriterCrashedError)
    expect(crashed).toHaveLength(1)

    // The aborted commit left nothing partial behind.
    const inspect = createBunSqliteConnection({ filename: file })
    expect(inspect.all("SELECT seq FROM event WHERE aggregate_id = 'a' ORDER BY seq")).toEqual([{ seq: 0 }])
    expect(inspect.all("SELECT seq FROM event_sequence WHERE aggregate_id = 'a'")).toEqual([{ seq: 0 }])
    inspect.close()

    // A read (which never reaches the crash point) respawns the writer with a bumped
    // generation and observes the intact baseline.
    const rows = await client.query("all", "SELECT seq FROM event WHERE aggregate_id = 'a' ORDER BY seq")
    expect(client.generation).toBe(2)
    expect(rows).toEqual([{ seq: 0 }])

    await client.close()
  })
})
