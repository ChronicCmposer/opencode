import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"

const Note = Event.define({
  type: "test.bus.seq.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const note = (sessionID: string, text: string) => ({ sessionID, text })

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

const open = () => {
  const native = new Database(":memory:")
  native.exec(DDL)
  native.exec("PRAGMA foreign_keys = ON")
  return { native, connection: connectionFrom(native) }
}

function connectionFrom(native: Database): DurableLog.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

const databases = new Array<Database>()
afterEach(() => {
  for (const native of databases.splice(0)) native.close()
})

describe("DurableLog sequence allocation", () => {
  test("keeps contiguous per-aggregate sequences across interleaved batches", () => {
    const { native, connection } = open()
    databases.push(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note] })

    bus.publish([{ definition: Note, data: note("a", "a0") }])
    bus.publish([
      { definition: Note, data: note("b", "b0") },
      { definition: Note, data: note("b", "b1") },
    ])
    bus.publish([
      { definition: Note, data: note("a", "a1") },
      { definition: Note, data: note("a", "a2") },
    ])

    const sequences = (aggregateID: string) => bus.read({ aggregateID }).map((event) => event.durable?.seq)

    expect(sequences("a")).toEqual([0, 1, 2])
    expect(sequences("b")).toEqual([0, 1])
    expect(bus.latest("a")).toBe(2)
    expect(bus.latest("b")).toBe(1)
  })

  test("enforces unique (aggregate_id, seq) at the database boundary", () => {
    const { native, connection } = open()
    databases.push(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note] })
    bus.publish([{ definition: Note, data: note("s1", "a") }])

    expect(() =>
      connection.run("INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
        "evt_duplicate",
        "s1",
        0,
        "test.bus.seq.note.1",
        JSON.stringify(note("s1", "duplicate")),
      ]),
    ).toThrow()
  })

  test("reserves the whole block with one upsert-returning statement", () => {
    const { native, connection } = open()
    databases.push(native)
    const statements = new Array<string>()
    const tracked: DurableLog.Connection = {
      all: (sql, params = []) => {
        statements.push(sql)
        return connection.all(sql, params)
      },
      run: (sql, params = []) => connection.run(sql, params),
      exec: (sql) => connection.exec(sql),
    }
    const bus = DurableLog.create({ connection: tracked, ownerID: "worker-1", definitions: [Note] })

    bus.publish([
      { definition: Note, data: note("s1", "a") },
      { definition: Note, data: note("s1", "b") },
      { definition: Note, data: note("s1", "c") },
    ])

    const reservations = statements.filter((sql) => /INSERT\s+INTO\s+event_sequence[\s\S]*RETURNING\s+seq/i.test(sql))
    expect(reservations).toHaveLength(1)
    expect(bus.latest("s1")).toBe(2)
  })

  test("does not advance the sequence when an uncommitted block is rolled back", () => {
    const { native, connection } = open()
    databases.push(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note] })
    let fail = true
    bus.project(Note, () => {
      if (fail) throw new Error("rollback")
    })

    expect(() => bus.publish([{ definition: Note, data: note("s1", "a") }])).toThrow("rollback")
    fail = false
    expect(bus.publish([{ definition: Note, data: note("s1", "b") }])[0]?.durable?.seq).toBe(0)
  })
})
