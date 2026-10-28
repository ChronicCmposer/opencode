// Durable metadata survives the hub's live-only republish.
//
// `createLiveEventPublisher` is the hub's live republish seam: a durable worker
// event already committed in its isolate, so the hub must never re-commit it
// (`publishLive` has no durable path). It must still carry the committed
// `(aggregateID, seq)` into the EventV2 `Payload`; otherwise the bridge's
// `GlobalBus` listener sees a non-durable payload and never emits the `sync`
// envelope that orders the event on the workspace stream.
//
// The database is a stub: the live path must not touch it, which also proves the
// republish is a passthrough and not a second commit.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, ManagedRuntime, Schema } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { GlobalBus, type GlobalEvent } from "@/bus/global"
import { createLiveEventPublisher, EventV2Bridge, type LiveEvent } from "@/event-v2-bridge"

/** The live republish path must never read `db`; a stub catches a stray commit. */
const databaseStub = Layer.succeed(Database.Service, { db: {} as never })

/**
 * Build the production bridge layer so republishes reach `GlobalBus` through the
 * bridge's own `EventV2.listen` listener.
 */
function bridgeRuntime() {
  const layer = AppNodeBuilder.build(LayerNode.group([EventV2.node, EventV2Bridge.node]), [
    [Database.node, databaseStub],
  ])
  return ManagedRuntime.make(layer)
}

interface Captured {
  /** The plain `{ id, type, properties }` envelope. */
  readonly regular: GlobalEvent["payload"][]
  /** The `{ type: "sync", syncEvent: ... }` envelope. */
  readonly sync: GlobalEvent["payload"][]
  readonly off: () => void
}

/** Capture only the envelopes carrying this event id (GlobalBus is process-wide). */
function captureGlobalBus(id: string): Captured {
  const regular: GlobalEvent["payload"][] = []
  const sync: GlobalEvent["payload"][] = []
  const listener = (event: GlobalEvent) => {
    const payload = event.payload
    if (payload?.type === "sync" && payload.syncEvent?.id === id) sync.push(payload)
    else if (payload?.id === id) regular.push(payload)
  }
  GlobalBus.on("event", listener)
  return { regular, sync, off: () => GlobalBus.off("event", listener) }
}

describe("event-v2-bridge live republish", () => {
  test("a republished durable event keeps aggregateID/seq for subscribers and the sync envelope", async () => {
    const Note = EventV2.define({
      type: "test.event-v2-bridge.note",
      durable: { version: 1, aggregate: "sessionID" },
      schema: { sessionID: Schema.String, text: Schema.String },
    })
    const id = "evt_bridge_durable"
    const data = { sessionID: "ses_bridge", text: "committed in the worker" }
    const runtime = bridgeRuntime()
    const bus = captureGlobalBus(id)
    try {
      await runtime.runPromise(Effect.void)
      const events = runtime.runSync(EventV2.Service)
      const seen: EventV2.Payload[] = []
      runtime.runSync(events.listen((event) => Effect.sync(() => void seen.push(event))))

      const publisher = createLiveEventPublisher(events)
      publisher.publish({
        id,
        type: Note.type,
        data,
        durable: { aggregateID: "ses_bridge", seq: 7, version: 1 },
      } satisfies LiveEvent)

      // The EventV2 subscriber sees the committed durable position verbatim.
      expect(seen).toHaveLength(1)
      expect(seen[0]?.durable).toEqual({ aggregateID: "ses_bridge", seq: 7, version: 1 })

      // The bridge listener turns that payload into the ordered `sync` envelope.
      expect(bus.sync).toHaveLength(1)
      expect(bus.sync[0]?.syncEvent).toEqual({
        id,
        type: "test.event-v2-bridge.note.1",
        seq: 7,
        aggregateID: "ses_bridge",
        data,
      })
      // The plain envelope still crosses unchanged.
      expect(bus.regular).toContainEqual({ id, type: "test.event-v2-bridge.note", properties: data })
    } finally {
      bus.off()
      await runtime.dispose()
    }
  })

  test("a non-durable republish is unchanged and emits no sync envelope", async () => {
    const Plain = EventV2.define({
      type: "test.event-v2-bridge.plain",
      schema: { text: Schema.String },
    })
    const id = "evt_bridge_plain"
    const data = { text: "live only" }
    const runtime = bridgeRuntime()
    const bus = captureGlobalBus(id)
    try {
      await runtime.runPromise(Effect.void)
      const events = runtime.runSync(EventV2.Service)
      const seen: EventV2.Payload[] = []
      runtime.runSync(events.listen((event) => Effect.sync(() => void seen.push(event))))

      createLiveEventPublisher(events).publish({ id, type: Plain.type, data })

      expect(seen).toHaveLength(1)
      expect(seen[0]?.durable).toBeUndefined()
      expect(bus.sync).toHaveLength(0)
      expect(bus.regular).toContainEqual({ id, type: "test.event-v2-bridge.plain", properties: data })
    } finally {
      bus.off()
      await runtime.dispose()
    }
  })
})
