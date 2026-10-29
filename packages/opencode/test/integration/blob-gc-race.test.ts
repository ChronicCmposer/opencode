// Blob GC must never unlink a file a concurrent publisher is about to reference.
//
// The interleaving under test is deterministic and real: a publisher stages its
// content-addressed file (before `BEGIN IMMEDIATE`), then blocks on the write
// lock while a GC deletes the last other reference and unlinks the file. The
// publisher then acquires the lock and commits a row referencing that hash.
//
// The fix makes the refcount check + unlink atomic under the same write lock and
// re-creates a staged file inside the publisher's transaction, so a committed
// row always has its file.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { DurableLog } from "@opencode-ai/core/durable-log"

const Note = Event.define({
  type: "test.blob-gc-race.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
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
    data text NOT NULL,
    CONSTRAINT fk_event_aggregate FOREIGN KEY (aggregate_id)
      REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE
  );
  CREATE UNIQUE INDEX event_aggregate_seq_idx ON event(aggregate_id, seq);
  CREATE TABLE event_blob (
    aggregate_id text NOT NULL,
    seq integer NOT NULL,
    type text NOT NULL,
    hash text NOT NULL,
    size integer NOT NULL,
    data text,
    file_path text,
    PRIMARY KEY (aggregate_id, seq),
    CONSTRAINT fk_event_blob_aggregate FOREIGN KEY (aggregate_id)
      REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE
  );
`

const directories: string[] = []
const natives: Database[] = []

function connectionFrom(native: Database): DurableLog.Connection {
  return {
    all: (sql, params = []) => native.query(sql).all(...(params as never[])) as Array<Record<string, unknown>>,
    run: (sql, params = []) => ({ changes: Number(native.query(sql).run(...(params as never[])).changes) }),
    exec: (sql) => native.exec(sql),
  }
}

function open() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-blob-gc-race-"))
  directories.push(directory)
  const filename = path.join(directory, "race.db")
  const native = new Database(filename, { create: true })
  native.exec(DDL)
  native.exec("PRAGMA foreign_keys = ON")
  natives.push(native)
  return { directory, filename, blobDir: path.join(directory, "blobs"), native, connection: connectionFrom(native) }
}

const textOf = (event: Event.Payload) => (event.data as { text: string }).text
const oversized = (seed: string) => `${seed}:${"x".repeat(4_096)}`

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("blob GC race", () => {
  test("a concurrent publisher recreates the file GC unlinked before its commit", () => {
    const database = open()
    const text = oversized("race")
    const blobDir = database.blobDir

    // Seed the same aggregate with the same content: its only reference is the
    // row the GC will delete, and the concurrent publisher re-uses that hash.
    const seedBus = DurableLog.create({
      connection: database.connection,
      ownerID: "gen-seed",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })
    seedBus.publish([{ definition: Note, data: { sessionID: "s1", text } }])
    const [file] = fs.readdirSync(blobDir)
    expect(file).toBeDefined()
    expect(database.connection.all("SELECT COUNT(*) AS count FROM event_blob")).toEqual([{ count: 1 }])

    // A separate, uninstrumented DurableLog performs the GC so the hook below cannot
    // recurse into its own `BEGIN IMMEDIATE`.
    const gcBus = DurableLog.create({
      connection: database.connection,
      ownerID: "gen-gc",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })

    let armed = true
    let collected = 0
    const publisherConnection: DurableLog.Connection = {
      ...database.connection,
      exec: (sql) => {
        if (armed && sql === "BEGIN IMMEDIATE") {
          armed = false
          // The publisher has staged the file and is waiting for the lock. GC
          // commits the last-reference delete and unlinks the file first.
          collected = gcBus.deleteAggregate("s1")
        }
        database.connection.exec(sql)
      },
    }
    const publisher = DurableLog.create({
      connection: publisherConnection,
      ownerID: "gen-publisher",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })

    publisher.publish([{ definition: Note, data: { sessionID: "s1", text } }])

    expect(collected).toBe(1)
    // The publisher's row committed with its file present.
    expect(fs.existsSync(path.join(blobDir, file!))).toBe(true)
    const rows = database.connection.all("SELECT hash, file_path FROM event_blob WHERE aggregate_id = ?", [
      "s1",
    ]) as ReadonlyArray<{ hash: string; file_path: string }>
    expect(rows).toHaveLength(1)
    expect(rows[0]!.file_path).toBe(rows[0]!.hash)
    expect(publisher.read({ aggregateID: "s1" }).map(textOf)).toEqual([text])
  })

  test("retention GC cannot unlink a file referenced by a concurrent commit", () => {
    const database = open()
    const text = oversized("retention-race")
    const blobDir = database.blobDir

    const seedBus = DurableLog.create({
      connection: database.connection,
      ownerID: "gen-seed",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })
    seedBus.publish([{ definition: Note, data: { sessionID: "s1", text } }])

    const gcBus = DurableLog.create({
      connection: database.connection,
      ownerID: "gen-gc",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })

    let armed = true
    let collected = 0
    const publisherConnection: DurableLog.Connection = {
      ...database.connection,
      exec: (sql) => {
        if (armed && sql === "BEGIN IMMEDIATE") {
          armed = false
          collected = gcBus.collectBlobs({ aggregateID: "s1", beforeSeq: 1 })
        }
        database.connection.exec(sql)
      },
    }
    const publisher = DurableLog.create({
      connection: publisherConnection,
      ownerID: "gen-seed",
      definitions: [Note],
      blobDir,
      blobLimitBytes: 64,
    })

    publisher.publish([{ definition: Note, data: { sessionID: "s1", text } }])
    expect(collected).toBe(1)
    expect(publisher.read({ aggregateID: "s1" }).map(textOf)).toEqual([text])
  })
})
