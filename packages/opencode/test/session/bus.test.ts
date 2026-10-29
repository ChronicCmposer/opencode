import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"

const Note = Event.define({
  type: "test.bus.note",
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
  CREATE TABLE projection (
    session_id text NOT NULL,
    seq integer NOT NULL,
    body text NOT NULL,
    PRIMARY KEY (session_id, seq)
  );
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

describe("DurableLog", () => {
  test("publishes a batch with contiguous durable sequences", () => {
    const { native, connection } = open()
    databases.push(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note] })

    const first = bus.publish([
      { definition: Note, data: note("s1", "a") },
      { definition: Note, data: note("s1", "b") },
    ])
    expect(first.map((event) => event.durable?.seq)).toEqual([0, 1])

    const second = bus.publish([{ definition: Note, data: note("s1", "c") }])
    expect(second[0]?.durable?.seq).toBe(2)
    expect(bus.read({ aggregateID: "s1" }).map((event) => textOf(event))).toEqual(["a", "b", "c"])
  })

  test("runs registered projectors inside the committing transaction", () => {
    const { native, connection } = open()
    databases.push(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note] })
    bus.project(Note, (event) => {
      connection.run("INSERT INTO projection (session_id, seq, body) VALUES (?, ?, ?)", [
        event.data.sessionID,
        event.durable?.seq,
        textOf(event),
      ])
    })

    bus.publish([{ definition: Note, data: note("s1", "projected") }])

    expect(connection.all("SELECT * FROM projection")).toEqual([{ session_id: "s1", seq: 0, body: "projected" }])
  })

  test("rolls back the event, sequence block, and projector when a projector fails", () => {
    const { native, connection } = open()
    databases.push(native)
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note] })
    let fail = true
    bus.project(Note, (event) => {
      if (fail) throw new Error("projector failed")
      connection.run("INSERT INTO projection (session_id, seq, body) VALUES (?, ?, ?)", [
        event.data.sessionID,
        event.durable?.seq,
        textOf(event),
      ])
    })

    expect(() => bus.publish([{ definition: Note, data: note("s1", "lost") }])).toThrow("projector failed")
    expect(connection.all("SELECT * FROM event")).toHaveLength(0)
    expect(connection.all("SELECT * FROM event_sequence")).toHaveLength(0)

    fail = false
    const committed = bus.publish([{ definition: Note, data: note("s1", "kept") }])
    expect(committed[0]?.durable?.seq).toBe(0)
    expect(connection.all("SELECT * FROM event_sequence")).toEqual([
      { aggregate_id: "s1", seq: 0, owner_id: "worker-1" },
    ])
  })
})

describe("DurableLog cache contract", () => {
  test("is event-invalidated, state-version fenced, and restart-safe", () => {
    const { native, connection } = open()
    databases.push(native)
    const cache = new DurableLog.StateCache<string>()
    const bus = DurableLog.create({ connection, ownerID: "worker-1", definitions: [Note], cache })

    bus.publish([{ definition: Note, data: note("s1", "a") }])
    const version = cache.version
    cache.set("s1", 0, "a", version)
    expect(cache.get("s1", 0)).toBe("a")

    bus.publish([{ definition: Note, data: note("s1", "b") }])
    expect(cache.get("s1", 0)).toBeUndefined()
    expect(cache.version).toBe(version + 1)
    expect(() => cache.set("s1", 1, "stale", version)).toThrow(DurableLog.StaleStateVersionError)

    const restarted = new DurableLog.StateCache<string>()
    expect(restarted.get("s1", 0)).toBeUndefined()
    for (const event of bus.read({ aggregateID: "s1" })) {
      restarted.set("s1", event.durable?.seq ?? -1, textOf(event))
    }
    expect(restarted.get("s1", 0)).toBe("a")
    expect(restarted.get("s1", 1)).toBe("b")
  })
})
