import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { Bus } from "@opencode-ai/core/bus"

const Note = Event.define({
  type: "test.bus.lease.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const note = (sessionID: string, text: string) => ({ sessionID, text })
const textOf = (event: Event.Payload) => (event.data as { text: string }).text

const DDL = `
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
    data text NOT NULL,
    CONSTRAINT fk_event_aggregate FOREIGN KEY (aggregate_id)
      REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE
  );
  CREATE UNIQUE INDEX event_aggregate_seq_idx ON event(aggregate_id, seq);
`

function connectionFrom(native: Database): Bus.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

let directory = ""
let nativeA: Database
let nativeB: Database
let busA: Bus.Interface
let busB: Bus.Interface

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "opencode-bus-lease-"))
  const filename = join(directory, "lease.db")
  nativeA = new Database(filename, { create: true })
  nativeA.exec(DDL)
  nativeA.exec("PRAGMA journal_mode = WAL")
  nativeA.exec("PRAGMA foreign_keys = ON")
  nativeB = new Database(filename)
  nativeB.exec("PRAGMA journal_mode = WAL")
  nativeB.exec("PRAGMA foreign_keys = ON")
  nativeB.exec("PRAGMA busy_timeout = 10")
  busA = Bus.create({ connection: connectionFrom(nativeA), ownerID: "worker-1/gen-1", definitions: [Note] })
  busB = Bus.create({ connection: connectionFrom(nativeB), ownerID: "worker-1/gen-2", definitions: [Note] })
})

afterEach(() => {
  nativeA.close()
  nativeB.close()
  rmSync(directory, { recursive: true, force: true })
})

describe("Bus W1.8 owner lease", () => {
  test("claim is exclusive and a stale owner cannot write", () => {
    expect(busA.claim("s1")).toEqual({ aggregateID: "s1", ownerID: "worker-1/gen-1", seq: -1 })
    expect(() => busB.claim("s1")).toThrow(Bus.StaleOwnerError)
    expect(() => busB.publish([{ definition: Note, data: note("s1", "stolen") }])).toThrow(Bus.StaleOwnerError)

    expect(busA.publish([{ definition: Note, data: note("s1", "owned") }])[0]?.durable?.seq).toBe(0)
    expect(busA.read({ aggregateID: "s1" }).map((event) => textOf(event))).toEqual(["owned"])
  })

  test("claiming a fresh aggregate permits the first publish at sequence zero", () => {
    expect(busA.latest("s1")).toBe(-1)
    busA.claim("s1")
    expect(busA.latest("s1")).toBe(-1)
    expect(busA.publish([{ definition: Note, data: note("s1", "first") }])[0]?.durable?.seq).toBe(0)
  })

  test("reclaim bumps the generation only at the expected sequence", () => {
    busA.claim("s1")
    busA.publish([{ definition: Note, data: note("s1", "a") }])

    expect(() =>
      busB.reclaim({
        aggregateID: "s1",
        expectedOwnerID: "worker-1/gen-1",
        expectedSeq: 0,
        nextOwnerID: "worker-1/gen-2",
      }),
    ).not.toThrow()
    expect(() => busA.publish([{ definition: Note, data: note("s1", "late") }])).toThrow(Bus.StaleOwnerError)
    expect(busB.publish([{ definition: Note, data: note("s1", "b") }])[0]?.durable?.seq).toBe(1)
  })

  test("reclaim rejects when the sequence advanced, proving no in-flight commit was dropped", () => {
    busA.claim("s1")
    busA.publish([{ definition: Note, data: note("s1", "a") }])

    expect(() =>
      busB.reclaim({
        aggregateID: "s1",
        expectedOwnerID: "worker-1/gen-1",
        expectedSeq: -1,
        nextOwnerID: "worker-1/gen-2",
      }),
    ).toThrow(Bus.LeaseConflictError)
    expect(busA.publish([{ definition: Note, data: note("s1", "b") }])[0]?.durable?.seq).toBe(1)
  })

  test("an in-flight writer holds the lock and a later reclaim observes the commit", () => {
    busA.claim("s1")
    busA.publish([{ definition: Note, data: note("s1", "a") }])

    nativeA.exec("BEGIN IMMEDIATE")
    expect(busA.publishInTransaction([{ definition: Note, data: note("s1", "b") }])[0]?.durable?.seq).toBe(1)
    expect(() =>
      busB.reclaim({
        aggregateID: "s1",
        expectedOwnerID: "worker-1/gen-1",
        expectedSeq: 0,
        nextOwnerID: "worker-1/gen-2",
      }),
    ).toThrow()
    nativeA.exec("COMMIT")

    expect(() =>
      busB.reclaim({
        aggregateID: "s1",
        expectedOwnerID: "worker-1/gen-1",
        expectedSeq: 0,
        nextOwnerID: "worker-1/gen-2",
      }),
    ).toThrow(Bus.LeaseConflictError)
    expect(
      busB.reclaim({
        aggregateID: "s1",
        expectedOwnerID: "worker-1/gen-1",
        expectedSeq: 1,
        nextOwnerID: "worker-1/gen-2",
      }),
    ).toEqual({ aggregateID: "s1", ownerID: "worker-1/gen-2", seq: 1 })
    expect(busB.publish([{ definition: Note, data: note("s1", "c") }])[0]?.durable?.seq).toBe(2)
  })

  test("two writers cannot both own the aggregate", () => {
    busA.claim("s1")
    expect(() => busB.claim("s1")).toThrow(Bus.StaleOwnerError)
    expect(() => busB.publish([{ definition: Note, data: note("s1", "b") }])).toThrow(Bus.StaleOwnerError)
    busA.publish([{ definition: Note, data: note("s1", "a") }])
    expect(nativeA.query("SELECT aggregate_id, seq, owner_id FROM event_sequence").all()).toEqual([
      { aggregate_id: "s1", seq: 0, owner_id: "worker-1/gen-1" },
    ])
    expect(nativeA.query("SELECT aggregate_id, seq FROM event").all()).toEqual([{ aggregate_id: "s1", seq: 0 }])
  })
})
