// A hub-delivered durable commit is republished into the worker isolate
// live-only. Its committed position — `durable.aggregateID` / `durable.seq` — is
// what the isolate's durable subscribers and the workspace sync envelope use to
// order a worker-originated commit, so the live republish must keep the original
// position instead of fabricating or dropping it.
//
// `republishDurableCommit` reads the definition version from this build's
// manifest; an unknown type has no committed position to stamp and crosses
// live-only.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { SessionV1 } from "@opencode-ai/schema/session-v1"
import { Latest } from "@/event-manifest"
import type { DurableCommit } from "@/session/egress/mesh"
import { republishDurableCommit } from "@/session/worker/layer"

/** Publishing live-only never reads `db`, so a stub is enough to build the layer. */
const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(databaseStub))

/** A real durable event type: `durable.aggregate === "sessionID"`, version 1. */
const DURABLE_TYPE = SessionV1.Event.MessageRemoved.type

/** The definition version this build stamps on a republished commit. */
function durableVersionOf(type: string): number {
  const version = Latest.get(type)?.durable?.version
  if (version === undefined) throw new Error(`test fixture requires a durable definition for ${type}`)
  return version
}
const DURABLE_VERSION = durableVersionOf(DURABLE_TYPE)

const aggregateID = "ses_republish_durable"
const seq = 7

const commit = (overrides?: { type?: string; aggregateID?: string; seq?: number }): DurableCommit => ({
  meta: { aggregateID: overrides?.aggregateID ?? aggregateID, seq: overrides?.seq ?? seq },
  event: {
    id: "evt_republish_durable",
    type: overrides?.type ?? DURABLE_TYPE,
    properties: { sessionID: overrides?.aggregateID ?? aggregateID, messageID: "msg_1" },
  },
})

/** Surface `republishDurableCommit` to `events.listen`; returns the observed payloads. */
async function republishObserved(durableCommit: DurableCommit): Promise<EventV2.Payload[]> {
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        const seen: EventV2.Payload[] = []
        yield* events.listen((event) => Effect.sync(() => void seen.push(event)))
        republishDurableCommit(events, durableCommit)
        return seen
      }).pipe(Effect.provide(eventLayer)),
    ),
  )
}

describe("republishDurableCommit committed position", () => {
  test("a republished durable commit is observed with its original aggregate and sequence", async () => {
    const seen = await republishObserved(commit())

    expect(seen).toHaveLength(1)
    expect(seen[0]).toMatchObject({
      id: "evt_republish_durable",
      type: DURABLE_TYPE,
      durable: { aggregateID, seq, version: DURABLE_VERSION },
    })
  })

  test("a different aggregate and sequence cross verbatim", async () => {
    const seen = await republishObserved(commit({ aggregateID: "ses_other", seq: 11 }))

    expect(seen[0]?.durable).toEqual({
      aggregateID: "ses_other",
      seq: 11,
      version: DURABLE_VERSION,
    })
  })

  test("an unknown type is republished live-only without a fabricated position", async () => {
    const seen = await republishObserved(commit({ type: "not.a.durable.event" }))

    expect(seen).toHaveLength(1)
    expect(seen[0]?.type).toBe("not.a.durable.event")
    expect(seen[0]?.durable).toBeUndefined()
  })
})
