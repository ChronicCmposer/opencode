// A hub restart must not strand a session behind the previous hub process's
// 30 s worker lease. Worker owner IDs carry the process PID
// (`worker-<id>-<pid>-<uuid>`); a lease whose PID is provably gone is
// reclaimable immediately, while a lease held by a live process generation is
// still fenced. Startup may also sweep every dead-generation lease in one
// transaction (`releaseDeadLeases`).

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "../src/durable-log"

const Note = Event.define({
  type: "test.bus-lease-restart.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: { sessionID: Schema.String, text: Schema.String },
})

const DDL = `
  CREATE TABLE event_sequence (
    aggregate_id text PRIMARY KEY,
    seq integer NOT NULL,
    owner_id text,
    worker_owner_id text,
    worker_heartbeat_at integer,
    worker_expires_at integer
  );
  CREATE TABLE event (
    id text PRIMARY KEY,
    aggregate_id text NOT NULL,
    seq integer NOT NULL,
    type text NOT NULL,
    data text NOT NULL
  );
  CREATE TABLE event_blob (
    aggregate_id text NOT NULL,
    seq integer NOT NULL,
    type text NOT NULL,
    hash text NOT NULL,
    size integer NOT NULL,
    data text,
    file_path text,
    PRIMARY KEY (aggregate_id, seq)
  );
`

/** A PID in the valid int32 range but far above Linux's max PID. */
const DEAD_PID = 2_000_000_000

const directories: string[] = []
const natives: Database[] = []

function open() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-bus-lease-restart-"))
  directories.push(directory)
  const native = new Database(path.join(directory, "lease.db"), { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec(DDL)
  natives.push(native)
  const connection: DurableLog.Connection = {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
  return { native, connection }
}

function bus(database: ReturnType<typeof open>, ownerID: string) {
  return DurableLog.create({
    connection: database.connection,
    ownerID,
    definitions: [Note],
    leaseTimeoutMs: 30_000,
    blobDir: path.join(path.dirname(database.native.filename!), "event-blobs"),
  })
}

const owner = (workerID: number, pid: number) => DurableLog.workerOwnerID(workerID, pid)

function lease(database: ReturnType<typeof open>, aggregateID: string) {
  return database.native
    .query("SELECT worker_owner_id, worker_expires_at FROM event_sequence WHERE aggregate_id = ?")
    .get(aggregateID) as { worker_owner_id: string; worker_expires_at: number } | null
}

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("DurableLog lease after a hub restart", () => {
  test("an unexpired lease from a dead process generation is reclaimable", () => {
    const database = open()
    const previousOwner = owner(1, DEAD_PID)
    const previous = bus(database, previousOwner)
    expect(previous.claim("s1")).toMatchObject({ ownerID: previousOwner, seq: -1 })
    expect(lease(database, "s1")?.worker_expires_at).toBeGreaterThan(Date.now())

    const successorOwner = owner(1, process.pid)
    const successor = bus(database, successorOwner)
    expect(
      successor.reclaim({
        aggregateID: "s1",
        expectedOwnerID: previousOwner,
        expectedSeq: -1,
        nextOwnerID: successorOwner,
      }),
    ).toEqual({ aggregateID: "s1", ownerID: successorOwner, seq: -1 })
    expect(lease(database, "s1")?.worker_owner_id).toBe(successorOwner)
  })

  test("a released lease is never re-stamped by a renewal", () => {
    const database = open()
    const ownerID = owner(1, process.pid)
    const log = bus(database, ownerID)
    log.claim("s1")

    log.release("s1")
    expect(lease(database, "s1")).toMatchObject({ worker_owner_id: null, worker_expires_at: 0 })

    // A heartbeat after release must fail rather than adopt the unowned row and
    // write the old owner/expiry back.
    expect(() => log.heartbeat("s1")).toThrow(DurableLog.StaleOwnerError)
    expect(lease(database, "s1")).toMatchObject({ worker_owner_id: null, worker_expires_at: 0 })
  })

  test("a startup-released dead lease is not revived by its old owner", () => {
    const database = open()
    const deadOwner = owner(1, DEAD_PID)
    const restarted = bus(database, deadOwner)
    restarted.claim("s1")
    expect(restarted.releaseDeadLeases()).toBe(1)
    expect(lease(database, "s1")).toMatchObject({ worker_owner_id: deadOwner, worker_expires_at: 0 })

    // The sweep keeps the owner but zeroes the expiry; a still-running old
    // generation could match the owner, so the expiry fence must refuse it.
    expect(() => restarted.heartbeat("s1")).toThrow(DurableLog.StaleOwnerError)
    expect(lease(database, "s1")).toMatchObject({ worker_owner_id: deadOwner, worker_expires_at: 0 })
  })

  test("a dead-looking PID from another host scope is never released by the sweep", () => {
    const database = open()
    const foreignOwner = `worker-1-${DEAD_PID}-deadbeef-${crypto.randomUUID()}`
    bus(database, foreignOwner).claim("s1")

    const restarted = bus(database, owner(1, process.pid))
    expect(restarted.releaseDeadLeases()).toBe(0)
    expect(lease(database, "s1")?.worker_expires_at).toBeGreaterThan(Date.now())
    expect(DurableLog.isDeadWorkerOwner(foreignOwner)).toBe(false)
    expect(DurableLog.isDeadWorkerOwner(owner(1, DEAD_PID))).toBe(true)
  })

  test("a legacy owner without a host scope is never judged dead", () => {
    const legacy = `worker-1-${DEAD_PID}-${crypto.randomUUID()}`
    expect(legacy.split("-")).toHaveLength(8)
    expect(DurableLog.isDeadWorkerOwner(legacy)).toBe(false)
  })

  test("startup clears only dead-generation leases and keeps live fencing", () => {
    const database = open()
    const deadOwner = owner(1, DEAD_PID)
    const liveOwner = owner(2, process.pid)
    bus(database, deadOwner).claim("s1")
    bus(database, liveOwner).claim("s2")

    const restarted = bus(database, owner(1, process.pid))
    expect(restarted.releaseDeadLeases()).toBe(1)
    expect(lease(database, "s1")?.worker_expires_at).toBe(0)
    expect(lease(database, "s2")?.worker_owner_id).toBe(liveOwner)
    expect(lease(database, "s2")?.worker_expires_at).toBeGreaterThan(Date.now())

    const successorOwner = owner(1, process.pid)
    expect(
      restarted.reclaim({
        aggregateID: "s1",
        expectedOwnerID: deadOwner,
        expectedSeq: -1,
        nextOwnerID: successorOwner,
      }),
    ).toMatchObject({ ownerID: successorOwner })
    expect(() =>
      restarted.reclaim({
        aggregateID: "s2",
        expectedOwnerID: liveOwner,
        expectedSeq: -1,
        nextOwnerID: successorOwner,
      }),
    ).toThrow(DurableLog.LeaseConflictError)
  })
})
