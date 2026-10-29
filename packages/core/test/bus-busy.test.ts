// M9 — the DurableLog publish path retries `SQLITE_BUSY` as a whole transaction within
// a bounded budget and surfaces a typed exhaustion. A busy failure must never
// leave a half-open transaction that makes the next attempt fail with
// "cannot start a transaction within a transaction".

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { Node } from "../src/effect/app-node"
import { LayerNode } from "../src/effect/layer-node"
import { DurableLog } from "../src/durable-log"
import { BusyExhaustedError } from "../src/database/transaction"

const Note = Event.define({
  type: "test.bus.busy.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: { sessionID: Schema.String, text: Schema.String },
})

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
    data text NOT NULL
  );
`

const busy = () => Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })

const directories: string[] = []
const natives: Database[] = []

function native(): Database {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-bus-busy-"))
  directories.push(directory)
  const handle = new Database(path.join(directory, "busy.db"), { create: true })
  handle.exec(DDL)
  natives.push(handle)
  return handle
}

function connection(native: Database): DurableLog.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

/** Fail the first `failures` calls that touch `match` with a native busy error. */
function flaky(target: DurableLog.Connection, match: string, failures: number): DurableLog.Connection {
  let remaining = failures
  return {
    all: (sql, params) => target.all(sql, params),
    run: (sql, params) => target.run(sql, params),
    exec: (sql) => {
      if (sql.startsWith(match) && remaining > 0) {
        remaining -= 1
        throw busy()
      }
      return target.exec(sql)
    },
  }
}

afterEach(() => {
  for (const handle of natives.splice(0)) handle.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("DurableLog publish busy retry (M9 / D5)", () => {
  test("retries a busy BEGIN and publishes once without a nested transaction", () => {
    const handle = native()
    const bus = DurableLog.create({
      connection: flaky(connection(handle), "BEGIN IMMEDIATE", 2),
      ownerID: "gen-1",
      definitions: [Note],
    })

    const published = bus.publish([{ definition: Note, data: { sessionID: "s1", text: "busy" } }])
    expect(published[0]?.durable?.seq).toBe(0)
    expect(handle.query("SELECT COUNT(*) AS count FROM event").get()).toEqual({ count: 1 })
  })

  test("retries a busy statement body as a whole transaction", () => {
    const handle = native()
    const target = connection(handle)
    let runs = 0
    const flakyRun: DurableLog.Connection = {
      all: target.all,
      run: (sql, params) => {
        if (sql.startsWith("INSERT INTO event ") && runs === 0) {
          runs += 1
          throw busy()
        }
        return target.run(sql, params)
      },
      exec: target.exec,
    }
    const bus = DurableLog.create({ connection: flakyRun, ownerID: "gen-1", definitions: [Note] })

    expect(bus.publish([{ definition: Note, data: { sessionID: "s1", text: "body" } }])[0]?.durable?.seq).toBe(0)
    expect(handle.query("SELECT COUNT(*) AS count FROM event").get()).toEqual({ count: 1 })
    expect(handle.query("SELECT COUNT(*) AS count FROM event_sequence").get()).toEqual({ count: 1 })
  })

  test("exhaustion is typed and terminal", () => {
    const handle = native()
    const bus = DurableLog.create({
      connection: flaky(connection(handle), "BEGIN IMMEDIATE", 1_000),
      ownerID: "gen-1",
      definitions: [Note],
    })

    const error = (() => {
      try {
        bus.publish([{ definition: Note, data: { sessionID: "s1", text: "never" } }])
      } catch (cause) {
        return cause
      }
    })()

    expect(error).toBeInstanceOf(BusyExhaustedError)
    expect((error as BusyExhaustedError).attempts).toBeGreaterThan(1)
  })

  test("a non-busy failure is not retried", () => {
    const handle = native()
    const target = connection(handle)
    const failing: DurableLog.Connection = {
      all: target.all,
      run: target.run,
      exec: (sql) => {
        if (sql.startsWith("BEGIN IMMEDIATE")) throw new Error("no such table: event")
        return target.exec(sql)
      },
    }
    const bus = DurableLog.create({ connection: failing, ownerID: "gen-1", definitions: [Note] })

    expect(() => bus.publish([{ definition: Note, data: { sessionID: "s1", text: "x" } }])).toThrow(/no such table/)
  })
})

describe("DurableLog service idiom", () => {
  test("Service tag, make adapter equivalence, and global node shape", async () => {
    const fromFactory = DurableLog.create({ connection: connection(native()), ownerID: "gen-service" })
    const fromMake = await Effect.runPromise(DurableLog.make({ connection: connection(native()), ownerID: "gen-service" }))

    expect(DurableLog.Service.key).toBe("@opencode/DurableLog")
    expect(Object.keys(fromMake).sort()).toEqual(Object.keys(fromFactory).sort())

    const node = DurableLog.nodeWith({ connection: connection(native()), ownerID: "gen-service" })
    expect(node.kind).toBe("layer")
    expect(node.name).toBe("@opencode/DurableLog")
    expect(node.service).toBe(DurableLog.Service)
    expect(node.tag).toBe(Node.tags.values.global)
    expect(node.dependencies).toEqual([])
  })

  test("layer and node provide a DurableLog equivalent to create", async () => {
    const viaLayer = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* DurableLog.Service
      }).pipe(
        Effect.provide(
          DurableLog.layer({
            connection: connection(native()),
            ownerID: "gen-service",
            definitions: [Note],
          }),
        ),
      ),
    )
    const viaNode = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* DurableLog.Service
      }).pipe(
        Effect.provide(
          LayerNode.compile(
            DurableLog.nodeWith({
              connection: connection(native()),
              ownerID: "gen-service",
              definitions: [Note],
            }),
          ),
        ),
      ),
    )

    expect(viaLayer.publish([{ definition: Note, data: { sessionID: "s1", text: "layer" } }])[0]?.durable?.seq).toBe(0)
    expect(viaNode.publish([{ definition: Note, data: { sessionID: "s1", text: "node" } }])[0]?.durable?.seq).toBe(0)
  })
})
