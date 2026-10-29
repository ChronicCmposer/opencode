// Step 7a / F17: a pre-`worker_owner_id` journal has no worker lease. `reclaim`
// must refuse typed before any lookup or write; it must never mirror a worker
// generation into the v1 sync `owner_id` column, even when that column already
// holds an owner (the old `reclaimLease` fallback did exactly that).

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { DurableLog } from "../src/durable-log"

const LEGACY_DDL = `
  CREATE TABLE event_sequence (
    aggregate_id text PRIMARY KEY,
    seq integer NOT NULL,
    owner_id text
  );
  CREATE TABLE event (
    id text PRIMARY KEY,
    aggregate_id text NOT NULL,
    seq integer NOT NULL,
    type text NOT NULL,
    data text NOT NULL
  );
  CREATE UNIQUE INDEX event_aggregate_seq_idx ON event(aggregate_id, seq);
`

const directories: string[] = []
const natives: Database[] = []

function open() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-step7a-reclaim-"))
  directories.push(directory)
  const native = new Database(path.join(directory, "legacy.db"), { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(LEGACY_DDL)
  natives.push(native)
  return native
}

function countingConnection(native: Database): { connection: DurableLog.Connection; lookups: () => number } {
  let lookups = 0
  const connection: DurableLog.Connection = {
    all: (sql, params = []) => {
      lookups += 1
      return native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>
    },
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
  return { connection, lookups: () => lookups }
}

function seed(native: Database): void {
  native.query("INSERT INTO event_sequence (aggregate_id, seq, owner_id) VALUES (?, ?, ?)").run("s1", 4, "old-owner")
}

function row(native: Database) {
  return native.query("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = ?").get("s1")
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("Step 7a legacy reclaim refusal", () => {
  test("reclaim on a seeded legacy row fails typed before any lookup or write", () => {
    const native = open()
    seed(native)
    const { connection, lookups } = countingConnection(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-new-generation" })
    const baseline = lookups()

    expect(() =>
      bus.reclaim({
        aggregateID: "s1",
        expectedOwnerID: "old-owner",
        expectedSeq: 4,
        nextOwnerID: "worker-new-generation",
      }),
    ).toThrow(DurableLog.WorkerLeaseUnsupportedError)

    // The refusal precedes the row lookup: no SELECT and no UPDATE ran.
    expect(lookups()).toBe(baseline)
    expect(row(native)).toEqual({ seq: 4, owner_id: "old-owner" })
    expect(native.query("SELECT COUNT(*) AS count FROM event").get()).toEqual({ count: 0 })
  })

  test("ownerOf reports v1 sync state and cannot turn it into a worker lease", () => {
    const native = open()
    seed(native)
    const bus = DurableLog.create({ connection: countingConnection(native).connection, ownerID: "worker-new-generation" })

    // `ownerOf` is a read of the sync column on this schema; it is not a lease.
    expect(bus.workerLease).toBe(false)
    expect(bus.ownerOf("s1")).toBe("old-owner")
    expect(() =>
      bus.reclaim({
        aggregateID: "s1",
        expectedOwnerID: bus.ownerOf("s1")!,
        expectedSeq: bus.latest("s1"),
        nextOwnerID: "worker-new-generation",
      }),
    ).toThrow(DurableLog.WorkerLeaseUnsupportedError)
    expect(row(native)).toEqual({ seq: 4, owner_id: "old-owner" })
    expect(native.query("SELECT COUNT(*) AS count FROM event").get()).toEqual({ count: 0 })
  })
})
