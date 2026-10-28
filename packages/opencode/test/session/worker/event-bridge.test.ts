// Worker-side EventV2 → events-port bridge (spec Q6/Q13/Q15).
//
// The bridge subscribes to the worker's `EventV2.Service.listen` and forwards only
// the events the UI renders: session-scoped payloads (those carrying a sessionID),
// skipping the primary hub-proxied `session.status` (Q15) and worker-local
// telemetry (Q13). Each forwarded event keeps the EventV2 shape VERBATIM
// (`{ id, type, properties }`) and is handed to the sink; the egress transport
// frames it. `dispose` unsubscribes.

import { describe, expect } from "bun:test"
import { Effect, Layer, Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import {
  DURABLE_WAKEUP_METADATA_KEY,
  HUB_PROXIED_EVENT_TYPES,
  createWorkerEventBridge,
  isSessionScopedEvent,
  toEventV2Event,
  type EventSubscriber,
  type WorkerEventSink,
} from "@/session/worker/event-bridge"
import type { EventV2Event } from "@/session/worker/frame"
import { testEffect } from "../../lib/effect"

/** Non-durable publishes never read `db`, so a stub is enough to build the layer. */
const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(databaseStub))
const it = testEffect(eventLayer)

// A session-scoped event (has sessionID), worker-local telemetry (no sessionID),
// and the hub-proxied status event the worker must not echo back.
const SessionThing = EventV2.define({
  type: "test.worker.session",
  schema: { sessionID: Schema.String, text: Schema.String },
})
const Telemetry = EventV2.define({
  type: "test.worker.telemetry",
  schema: { threadId: Schema.Number },
})
const Status = EventV2.define({
  type: "session.status",
  schema: { sessionID: Schema.String },
})

/** A sink that records the verbatim events it is handed. */
function capture(): { readonly events: EventV2Event[]; readonly sink: WorkerEventSink } {
  const events: EventV2Event[] = []
  return { events, sink: { emit: (event) => events.push(event) } }
}

describe("toEventV2Event", () => {
  it.effect("projects a payload onto the verbatim { id, type, properties } shape", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const payload = yield* events.publish(SessionThing, { sessionID: "ses_1", text: "hi" })
      const wire = toEventV2Event(payload)
      expect(wire).toEqual({ id: payload.id, type: "test.worker.session", properties: payload.data })
      expect(wire.id).toStartWith("evt_")
      expect("data" in wire).toBe(false)
    }),
  )
})

describe("isSessionScopedEvent", () => {
  it.effect("accepts a payload whose data carries a sessionID", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const payload = yield* events.publish(SessionThing, { sessionID: "ses_1", text: "hi" })
      expect(isSessionScopedEvent(payload)).toBe(true)
    }),
  )

  it.effect("rejects worker-local telemetry with no sessionID", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const payload = yield* events.publish(Telemetry, { threadId: 7 })
      expect(isSessionScopedEvent(payload)).toBe(false)
    }),
  )
})

describe("createWorkerEventBridge", () => {
  it.effect("forwards a session-scoped event verbatim as { id, type, properties }", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { events: forwarded, sink } = capture()
      const bridge = yield* createWorkerEventBridge(events, sink)

      yield* events.publish(SessionThing, { sessionID: "ses_1", text: "hello" })

      expect(forwarded).toHaveLength(1)
      const event = forwarded[0]
      expect(event.type).toBe("test.worker.session")
      expect(event.properties).toEqual({ sessionID: "ses_1", text: "hello" })
      expect(event.id).toStartWith("evt_")
      // Verbatim: `properties`, never a writer-style `data` field.
      expect("data" in event).toBe(false)

      yield* bridge.dispose
    }),
  )

  it.effect("routes a durable event to emitDurable when the sink provides it", () =>
    Effect.gen(function* () {
      const plain: EventV2Event[] = []
      const durable: EventV2Event[] = []
      let emitToListener: EventV2.Subscriber | undefined
      const fake: EventSubscriber = {
        listen: (listener) =>
          Effect.sync(() => {
            emitToListener = listener
            return Effect.void
          }),
      }
      const sink: WorkerEventSink = {
        emit: (event) => plain.push(event),
        emitDurable: (event) => durable.push(event),
      }
      const bridge = yield* createWorkerEventBridge(fake, sink)

      yield* emitToListener!({
        id: EventV2.ID.create(),
        type: "test.worker.session",
        data: { sessionID: "ses_1", text: "durable" },
        durable: { aggregateID: "ses_1", seq: 1, version: 1 },
      })

      expect(durable).toHaveLength(1)
      expect(plain).toHaveLength(0)
      expect(durable[0]).toMatchObject({
        type: "test.worker.session",
        properties: { sessionID: "ses_1", text: "durable" },
      })
      yield* bridge.dispose
    }),
  )

  it.effect("never downgrades a durable event to a non-durable emit", () =>
    Effect.gen(function* () {
      const plain: EventV2Event[] = []
      let emitToListener: EventV2.Subscriber | undefined
      const fake: EventSubscriber = {
        listen: (listener) =>
          Effect.sync(() => {
            emitToListener = listener
            return Effect.void
          }),
      }
      // A sink that can only carry the verbatim event: forwarding a durable
      // event through it would silently drop `(aggregateID, seq)`, so the bridge
      // must refuse rather than downgrade durability.
      const bridge = yield* createWorkerEventBridge(fake, { emit: (event) => plain.push(event) })

      expect(() =>
        Effect.runSync(
          emitToListener!({
            id: EventV2.ID.create(),
            type: "test.worker.session",
            data: { sessionID: "ses_1", text: "durable" },
            durable: { aggregateID: "ses_1", seq: 4, version: 1 },
          }),
        ),
      ).toThrow(/durable/)
      expect(plain).toHaveLength(0)
      yield* bridge.dispose
    }),
  )

  it.effect("drops worker-local telemetry (no sessionID)", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { events: forwarded, sink } = capture()
      const bridge = yield* createWorkerEventBridge(events, sink)

      yield* events.publish(Telemetry, { threadId: 7 })

      expect(forwarded).toEqual([])
      yield* bridge.dispose
    }),
  )

  it.effect("does not forward the hub-proxied session.status event", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { events: forwarded, sink } = capture()
      const bridge = yield* createWorkerEventBridge(events, sink)

      yield* events.publish(Status, { sessionID: "ses_1" })

      expect(HUB_PROXIED_EVENT_TYPES.has("session.status")).toBe(true)
      expect(forwarded).toEqual([])
      yield* bridge.dispose
    }),
  )

  // Regression: the hub mesh delivers a worker its OWN durable commit back
  // (pattern `"*"`). `republishDurableCommit` marks that live-only republish with
  // `DURABLE_WAKEUP_METADATA_KEY`; the bridge must not re-forward it, or the hub
  // would publish the same assistant part/text update twice.
  it.effect("does not re-forward a durable-wakeup republish", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { events: forwarded, sink } = capture()
      const bridge = yield* createWorkerEventBridge(events, sink)

      yield* events.publishLive({
        id: EventV2.ID.create(),
        type: SessionThing.type,
        data: { sessionID: "ses_1", text: "woken" },
        metadata: { [DURABLE_WAKEUP_METADATA_KEY]: true },
      })

      expect(forwarded).toEqual([])
      yield* bridge.dispose
    }),
  )

  it.effect("still forwards a live publish that is not a wakeup republish", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { events: forwarded, sink } = capture()
      const bridge = yield* createWorkerEventBridge(events, sink)

      yield* events.publishLive({
        id: EventV2.ID.create(),
        type: SessionThing.type,
        data: { sessionID: "ses_1", text: "live" },
      })

      expect(forwarded).toHaveLength(1)
      expect(forwarded[0].properties).toEqual({ sessionID: "ses_1", text: "live" })
      yield* bridge.dispose
    }),
  )

  it.effect("dispose stops forwarding and is idempotent", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { events: forwarded, sink } = capture()
      const bridge = yield* createWorkerEventBridge(events, sink)

      yield* events.publish(SessionThing, { sessionID: "ses_1", text: "before" })
      expect(forwarded).toHaveLength(1)

      yield* bridge.dispose
      yield* bridge.dispose
      yield* events.publish(SessionThing, { sessionID: "ses_1", text: "after" })

      expect(forwarded).toHaveLength(1)
      expect(forwarded[0].properties).toEqual({ sessionID: "ses_1", text: "before" })
    }),
  )
})
