// Durable metadata survives a republish.
//
// A durable event's committed position — `durable.aggregateID` and
// `durable.seq` — is what the workspace `sync` envelope needs to order a
// worker-originated commit on the hub. A republish (live-only: the commit
// already happened in the originating isolate) must carry that position
// verbatim; the core live path may not re-derive or drop it.
//
// The hub-side sink (`hub.ts`) and the `event-v2-bridge.ts` publisher are the
// cross-lane seams that forward a worker frame into this path; this test pins
// the core guarantee those seams rely on.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import { Event } from "@opencode-ai/schema/event"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { EventV2 } from "@opencode-ai/core/event"

const Note = Event.define({
  type: "test.durable-metadata.note",
  durable: { version: 1, aggregate: "sessionID" },
  schema: {
    sessionID: Schema.String,
    text: Schema.String,
  },
})

const Manifest = { definitions: Event.durable([Note]), schema: Schema.Any }
const directories: string[] = []

function databasePath(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-durable-metadata-"))
  directories.push(directory)
  return path.join(directory, "opencode.db")
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("durable metadata on republish", () => {
  test("a live-only republish preserves the committed aggregate and sequence", async () => {
    const filename = databasePath()

    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* EventV2.Service
          const { db } = yield* DatabaseService.Service
          const seen: EventV2.Payload[] = []
          yield* events.listen((event) => Effect.sync(() => void seen.push(event)))

          yield* events.publish(Note, { sessionID: "session-a", text: "committed" })
          // The hub reads the committed event and republishes it live-only; the
          // payload it reads is the exact one a worker frame reconstructed.
          const page = yield* EventV2.readAggregate(db, {
            aggregateID: "session-a",
            limit: 10,
            manifest: Manifest,
          })
          const original = page.events[0]!
          yield* events.publishLive(original)

          return { seen, original }
        }).pipe(
          Effect.provide(
            EventV2.layerWith().pipe(
              Layer.provide(DurableLog.layerFromDatabase),
              Layer.provideMerge(DatabaseService.layerFromPath(filename)),
            ),
          ),
        ),
      ),
    )

    expect(outcome.original.durable).toEqual({ aggregateID: "session-a", seq: 0, version: 1 })
    expect(outcome.seen).toHaveLength(2)
    // The republished payload is not a new commit and keeps the original position.
    expect(outcome.seen[1]?.durable).toEqual({ aggregateID: "session-a", seq: 0, version: 1 })
    expect(outcome.seen[1]?.id).toBe(outcome.original.id)
  })
})
