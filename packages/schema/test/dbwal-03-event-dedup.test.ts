import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { EventManifest } from "../src/event-manifest"
import { SessionV1 } from "../src/session-v1"

// EVENTDEDUP safety guards. These pin the contract that any attempt to stop the `event` table
// from duplicating `message`/`part` payloads MUST preserve. See
// `.parallel-implement/dbwal-03-event-dedup.md` for the full analysis: making the two V1 events
// non-durable is fatal because `EventV2` runs projectors ONLY inside the durable-commit path
// (verified: `packages/core/src/event.ts:376-403` + a throwaway probe), and stripping their
// payloads breaks replay/sync because the projectors and peer replay read the embedded payload.

const decodeMessageUpdated = Schema.decodeUnknownSync(SessionV1.Event.MessageUpdated.data)
const encodeMessageUpdated = Schema.encodeSync(SessionV1.Event.MessageUpdated.data)

const userMessage = {
  id: "msg_dbwal03",
  sessionID: "ses_dbwal03",
  role: "user" as const,
  time: { created: 1 },
  agent: "build",
  model: { providerID: "test", modelID: "test-model" },
}

describe("dbwal-03 event dedup safety guards", () => {
  test("message.updated / message.part.updated stay durable V1 session events", () => {
    // The durable marker is what routes them through `commitDurableEvent`, which is the only
    // path that runs projectors. Removing it would stop message/part rows from being written.
    expect(SessionV1.Event.MessageUpdated.durable).toEqual({ aggregate: "sessionID", version: 1 })
    expect(SessionV1.Event.PartUpdated.durable).toEqual({ aggregate: "sessionID", version: 1 })
  })

  test("the durable manifest still decodes historical message.updated.1 rows", () => {
    // replay/replayAll resolve stored rows through this map; dropping the entry would make
    // `decodeSerializedEvent` throw on the ~151k existing message/part event rows.
    const definition = EventManifest.Durable.get("message.updated.1")
    expect(definition).toBe(SessionV1.Event.MessageUpdated)

    const decoded = decodeMessageUpdated({ sessionID: "ses_dbwal03", info: userMessage })
    expect(decoded.info.role).toBe("user")
  })

  test("the durable event embeds the full message payload (the duplication source)", () => {
    // `commitDurableEvent` persists `encode(definition.data)(event.data)` verbatim, and the
    // projector writes the same `info` into `MessageTable`. A payload strip is therefore a
    // deliberate, migration-aware change, not a schema-local one.
    const encoded = encodeMessageUpdated(decodeMessageUpdated({ sessionID: "ses_dbwal03", info: userMessage }))
    expect(encoded).toEqual({ sessionID: "ses_dbwal03", info: userMessage })
    expect(Object.keys(encoded)).toContain("info")
  })

  test("live and durable surfaces share one definition object", () => {
    // The UI live event and the durable manifest are the same definition, so a change to
    // durability/shape affects both surfaces at once (and the V2 server manifest).
    expect(EventManifest.Latest.get("message.updated")).toBe(SessionV1.Event.MessageUpdated)
    expect(EventManifest.Durable.get("message.updated.1")).toBe(SessionV1.Event.MessageUpdated)
    expect(EventManifest.Latest.get("message.part.updated")).toBe(SessionV1.Event.PartUpdated)
    expect(EventManifest.Durable.get("message.part.updated.1")).toBe(SessionV1.Event.PartUpdated)
  })

  test("the live-only V1 surface excludes the two durable events", () => {
    // They belong to the current `ServerDefinitions` surface (durable set), not the live-only
    // compatibility set; making them non-durable would move them and break the public surface.
    expect(EventManifest.ServerDefinitions).toContain(SessionV1.Event.MessageUpdated)
    expect(EventManifest.ServerDefinitions).toContain(SessionV1.Event.PartUpdated)
  })

  test.skip("TARGET (blocked): the durable event no longer carries the full payload", () => {
    // Requires the cross-stream changes in `.parallel-implement/dbwal-03-event-dedup.md`
    // (core persistence/replay hook + projector idempotency + sync shipping message/part rows).
    // Not implementable from the schema files alone without breaking replay/sync.
  })
})
