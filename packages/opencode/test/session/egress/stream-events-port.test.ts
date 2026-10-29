// The events port has ONE live implementation: a `Stream.callback` listener
// registered by `Effect.acquireRelease`, driven sequentially by
// `Stream.runForEach`. This suite pins the lifecycle and every preserved
// semantic of that single path: arrival order, duplicate/stale drop, gap
// healing on the same port, blob resolution before the ack, fail-loud close with
// exactly one observed error, and non-durable backpressure drop.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { type DurableResyncPayload, decodeFrame, encodeFrame } from "@/session/egress/frame"
import { createEgressPort, createEgressReceiver } from "@/session/egress/receiver"
import { type HubEventSink, serveEventsPort } from "@/session/worker/hub"
import {
  ackFrame,
  type EventV2Event,
  eventFrame,
  endFrame,
  type EventsFrame,
  type EventsResyncPayload,
} from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"

const delta = (id: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: { id },
})

const durable = (id: string, aggregateID: string, seq: number): EventsFrame =>
  eventFrame({ event: { id, type: "message.updated", properties: { id } }, aggregateID, seq } as never)

interface SinkRecorder {
  readonly sink: HubEventSink
  readonly events: EventV2Event[]
  readonly ops: string[]
}

function recordingSink(
  options: {
    readonly resolveBlob?: HubEventSink["resolveBlob"]
    readonly heal?: HubEventSink["heal"]
  } = {},
): SinkRecorder {
  const events: EventV2Event[] = []
  const ops: string[] = []
  let ended = false
  let resyncs = 0
  let acks = 0
  const sink: HubEventSink = {
    ...(options.resolveBlob === undefined ? {} : { resolveBlob: options.resolveBlob }),
    ...(options.heal === undefined ? {} : { heal: options.heal }),
    event(event) {
      events.push(event)
      ops.push(`event:${event.id}`)
    },
    end() {
      ended = true
      ops.push("end")
    },
    resync() {
      resyncs++
      ops.push("resync")
    },
    ack(payload) {
      acks++
      ops.push(`ack:${payload?.count ?? "?"}`)
    },
    get ended() {
      return ended
    },
    get resyncs() {
      return resyncs
    },
    get acks() {
      return acks
    },
  }
  return { sink, events, ops }
}

describe("events stream lifecycle", () => {
  test("acquires onmessage/start once and releases the listener and port once", async () => {
    const channel = new MessageChannel()
    const port = channel.port1
    let starts = 0
    let closes = 0
    const start = port.start.bind(port)
    const close = port.close.bind(port)
    port.start = () => {
      starts++
      start()
    }
    port.close = () => {
      closes++
      close()
    }
    const { sink, events } = recordingSink()
    const dispose = serveEventsPort(port, sink)
    try {
      await pollUntil(() => typeof port.onmessage === "function", { message: "listener was never acquired" })
      expect(starts).toBe(1)
      channel.port2.postMessage(encodeFrame(eventFrame({ event: delta("e1") })))
      await pollUntil(() => events.length === 1, { message: "event never reached the sink" })

      dispose()
      expect(closes).toBe(1)
      expect(port.onmessage).toBeNull()
      // Idempotent dispose: the listener/port are released exactly once.
      dispose()
      expect(closes).toBe(1)

      channel.port2.postMessage(encodeFrame(eventFrame({ event: delta("e2") })))
      await pollUntil(() => false, { timeoutMs: 20 }).catch(() => {})
      expect(events.map((event) => event.id)).toEqual(["e1"])
    } finally {
      channel.port2.close()
    }
  })

  test("a malformed frame is dropped, reported once, and answered with a resync while the port stays open", async () => {
    const channel = new MessageChannel()
    const errors: Error[] = []
    const requests: unknown[] = []
    channel.port2.onmessage = (event) => void requests.push(decodeFrame(event.data as string))
    const { sink, events } = recordingSink()
    const dispose = serveEventsPort(channel.port1, sink, (error) => errors.push(error))
    try {
      channel.port2.postMessage("{not json")
      await pollUntil(() => errors.length === 1, { message: "malformed frame was silently swallowed" })
      await pollUntil(() => requests.length === 1, { message: "no resync was requested" })
      expect((requests[0] as { kind: string }).kind).toBe("resync")
      expect(channel.port1.onmessage).not.toBeNull()
      channel.port2.postMessage(encodeFrame(eventFrame({ event: delta("late") })))
      await pollUntil(() => events.length === 1, { message: "later frame was not delivered" })
      expect(errors.length).toBe(1)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a malformed frame closes a receiver that has no rejection handler", async () => {
    const channel = new MessageChannel()
    const errors: Error[] = []
    const connection = createEgressPort(channel.port1, { onFrame: () => {}, onError: (error) => errors.push(error) })
    const fiber = Effect.runFork(connection.program.pipe(Effect.catch(() => Effect.void)))
    try {
      channel.port2.postMessage("{not json")
      await pollUntil(() => errors.length === 1, { message: "malformed frame was silently swallowed" })
      expect(channel.port1.onmessage).toBeNull()
    } finally {
      connection.close()
      fiber.interruptUnsafe()
      channel.port2.close()
    }
  })
})

describe("events stream ordering", () => {
  test("delivers frames in arrival order and ends after the end frame", async () => {
    const channel = new MessageChannel()
    const { sink, ops } = recordingSink()
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      channel.port2.postMessage(encodeFrame(eventFrame({ event: delta("a") })))
      channel.port2.postMessage(encodeFrame(durable("b", "agg", 1)))
      channel.port2.postMessage(encodeFrame(eventFrame({ event: delta("c") })))
      channel.port2.postMessage(encodeFrame(endFrame()))
      await pollUntil(() => ops.length === 4, { message: "frames were not delivered in order" })
      expect(ops).toEqual(["event:a", "event:b", "event:c", "end"])
      // After `end`, later messages are ignored but the port stays usable.
      channel.port2.postMessage(encodeFrame(eventFrame({ event: delta("d") })))
      await pollUntil(() => false, { timeoutMs: 20 }).catch(() => {})
      expect(ops.length).toBe(4)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("drops duplicate/stale durables without rewinding the cursor or resyncing", async () => {
    const channel = new MessageChannel()
    const { sink, events } = recordingSink()
    const dispose = serveEventsPort(
      channel.port1,
      sink,
      undefined,
      undefined,
    )
    try {
      channel.port2.postMessage(encodeFrame(durable("d1", "agg", 1)))
      channel.port2.postMessage(encodeFrame(durable("d1-stale", "agg", 1)))
      channel.port2.postMessage(encodeFrame(durable("d2", "agg", 2)))
      await pollUntil(() => events.length === 2, { message: "stale durable was delivered" })
      expect(events.map((event) => event.id)).toEqual(["d1", "d2"])
      expect(sink.resyncs).toBe(0)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})

describe("events stream gap healing", () => {
  test("heals a gap and posts a positional resync back on the same port", async () => {
    const channel = new MessageChannel()
    const healed: Array<{ aggregateID: string; seq: number; after?: number }> = []
    const { sink, events } = recordingSink({
      heal: (position) => {
        healed.push(position)
      },
    })
    const resyncs: DurableResyncPayload[] = []
    channel.port2.onmessage = (event) => {
      const frame = decodeFrame(event.data)
      if (frame.kind === "resync" && frame.payload !== undefined) resyncs.push(frame.payload)
    }
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      channel.port2.postMessage(encodeFrame(durable("d1", "agg", 1)))
      channel.port2.postMessage(encodeFrame(durable("d3", "agg", 3)))
      await pollUntil(() => healed.length === 1 && resyncs.length === 1, {
        message: "gap was not healed on the same port",
      })
      expect(healed[0]).toEqual({ aggregateID: "agg", seq: 3, after: 1 })
      expect(resyncs[0]).toEqual({ aggregateID: "agg", seq: 3 })

      // The port stayed open and the observed cursor advanced; delivery continues.
      channel.port2.postMessage(encodeFrame(durable("d4", "agg", 4)))
      await pollUntil(() => events.length === 3, { message: "stream stopped after a gap" })
      expect(events.map((event) => event.id)).toEqual(["d1", "d3", "d4"])
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})

describe("events stream blob-before-ack", () => {
  test("resolves a blob reference before the event and the batch ack", async () => {
    const channel = new MessageChannel()
    let releaseBlob: (value: Record<string, unknown> | undefined) => void = () => {}
    const blob = new Promise<Record<string, unknown> | undefined>((resolve) => {
      releaseBlob = resolve
    })
    const { sink, ops } = recordingSink({
      resolveBlob: () => blob,
    })
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      channel.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: {
              id: "big",
              type: "message.updated",
              properties: { __eventBlobPosition: { aggregateID: "agg", seq: 1 }, frameBytes: 1 },
            },
            aggregateID: "agg",
            seq: 1,
          } as never),
        ),
      )
      channel.port2.postMessage(encodeFrame(ackFrame({ count: 1 })))
      await pollUntil(() => false, { timeoutMs: 20 }).catch(() => {})
      // The unresolved blob blocks both its own event and the following ack.
      expect(ops).toEqual([])

      releaseBlob({ id: "big", resolved: true })
      await pollUntil(() => ops.length === 2, { message: "blob reference was not delivered before its ack" })
      expect(ops).toEqual(["event:big", "ack:1"])
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})

describe("events stream backpressure", () => {
  test("drops a non-durable delta under pressure and keeps the durable", async () => {
    const channel = new MessageChannel()
    const frames: EventsFrame[] = []
    const resyncs: (EventsResyncPayload | undefined)[] = []
    const receiver = createEgressReceiver({
      port: channel.port2,
      onFrame: (frame) => frames.push(frame),
      onResync: (payload) => resyncs.push(payload),
      pressure: () => true,
    })
    try {
      channel.port1.postMessage(encodeFrame(eventFrame({ event: delta("dropped") })))
      channel.port1.postMessage(encodeFrame(durable("kept", "agg", 1)))
      await pollUntil(() => receiver.dropped === 1 && frames.length === 1, {
        message: "pressure did not drop the delta while keeping the durable",
      })
      expect(frames).toEqual([durable("kept", "agg", 1)])
      expect(receiver.resyncs).toBe(1)
      expect(resyncs).toEqual([undefined])
    } finally {
      receiver.close()
      channel.port1.close()
    }
  })
})
