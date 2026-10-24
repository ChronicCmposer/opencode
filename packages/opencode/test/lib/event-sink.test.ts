// Wave 1 — `event-sink` unit tests (spec Q1/Q12 parity harness support).
//
// The sink subscribes to the REAL `EventV2.Service` (or `GlobalBus`) and records
// each emission as `{ id, type, properties }`. Durable events touch the database,
// but every event here is live-only, so a stub `Database.Service` is sufficient
// and the test needs no filesystem.

import { describe, expect, it } from "bun:test"
import { Effect, Layer, ManagedRuntime, Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { GlobalBus } from "@/bus/global"
import { createEventSink } from "./event-sink"

const LiveMessage = EventV2.define({
  type: "test.live.message",
  schema: { text: Schema.String },
})

const LiveCount = EventV2.define({
  type: "test.live.count",
  schema: { count: Schema.Number },
})

/** Non-durable publishes never read `db`, so a stub is enough to build the layer. */
const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(databaseStub))

function makeRuntime() {
  return ManagedRuntime.make(eventLayer)
}

function publish(runtime: ManagedRuntime.ManagedRuntime<EventV2.Service, never>) {
  return runtime.runPromise(
    EventV2.Service.use((events) =>
      Effect.gen(function* () {
        yield* events.publish(LiveMessage, { text: "hello" })
        yield* events.publish(LiveCount, { count: 2 })
      }),
    ),
  )
}

describe("event-sink", () => {
  it("captures published EventV2 events in order as { id, type, properties }", async () => {
    const runtime = makeRuntime()
    const sink = createEventSink(runtime)
    try {
      await publish(runtime)

      const events = sink.events()
      expect(events.map((event) => event.type)).toEqual(["test.live.message", "test.live.count"])
      expect(events[0]?.properties).toEqual({ text: "hello" })
      expect(events[1]?.properties).toEqual({ count: 2 })
      expect(events[0]?.id).toStartWith("evt_")
      expect(sink.types()).toEqual(["test.live.message", "test.live.count"])
    } finally {
      await sink.dispose()
      await runtime.dispose()
    }
  })

  it("clear() drops captured events and events()/types() return defensive copies", async () => {
    const runtime = makeRuntime()
    const sink = createEventSink(runtime)
    try {
      await publish(runtime)
      const first = sink.events()
      const second = sink.events()
      expect(first).not.toBe(second)
      expect(first).toEqual(second)
      expect(first).toHaveLength(2)

      sink.clear()
      expect(sink.events()).toEqual([])
      expect(sink.types()).toEqual([])
    } finally {
      await sink.dispose()
      await runtime.dispose()
    }
  })

  it("dispose() stops capturing further events", async () => {
    const runtime = makeRuntime()
    const sink = createEventSink(runtime)
    try {
      await publish(runtime)
      expect(sink.events()).toHaveLength(2)

      await sink.dispose()
      await publish(runtime)
      expect(sink.events()).toHaveLength(2)
    } finally {
      await runtime.dispose()
    }
  })

  it("accepts a Layer directly (not only a runtime)", async () => {
    const sink = createEventSink(eventLayer)
    try {
      await publish(sink.runtime)
      expect(sink.events().map((event) => event.type)).toEqual(["test.live.message", "test.live.count"])
    } finally {
      await sink.dispose()
    }
  })

  it("can subscribe to GlobalBus emissions instead", async () => {
    const runtime = makeRuntime()
    const sink = createEventSink(runtime, { origin: "global-bus" })
    try {
      GlobalBus.emit("event", {
        payload: { id: "evt_global", type: "test.global", properties: { visible: true } },
      })

      // Other test files may share the process-wide GlobalBus, so assert containment.
      expect(sink.events()).toContainEqual({ id: "evt_global", type: "test.global", properties: { visible: true } })
    } finally {
      await sink.dispose()
      await runtime.dispose()
    }
  })

  it("surfaces teardown errors instead of swallowing them", async () => {
    // A runtime-shaped double whose unsubscribe path rejects.
    const failingRuntime = {
      runSync: () => Effect.void,
      runPromise: () => Promise.reject(new Error("teardown boom")),
      runFork: () => undefined,
    } as unknown as ManagedRuntime.ManagedRuntime<EventV2.Service, never>

    const sink = createEventSink(failingRuntime)

    await expect(sink.dispose()).rejects.toThrow("teardown boom")
  })
})
