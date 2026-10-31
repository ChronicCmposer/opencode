// The hub end of the events port over byte frames (D15): properties stay raw `LazyJson` from the port to the
// live publisher, durable meta comes from the envelope, and resync/ack replies are transferred byte frames.

import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import { jsDecoder } from "@/session/egress/scan"
import { createHubEventSink, serveEventsPort, type HubEventSink } from "@/session/worker/hub"
import { eventFrame } from "@/session/worker/frame"
import { LazyJson } from "@/util/lazy-json"
import { pollUntil } from "../../lib/poll"

type Published = Parameters<Parameters<typeof createHubEventSink>[0]["publisher"]["publish"]>[0]

const valueDescriptor = Object.getOwnPropertyDescriptor(LazyJson.prototype, "value")!
let reads = 0

// The base `jsDecoder` parses while scanning, so only reads after a frame reaches the sink are counted.
beforeEach(() => {
  reads = 0
  Object.defineProperty(LazyJson.prototype, "value", {
    configurable: true,
    get() {
      reads++
      return valueDescriptor.get!.call(this)
    },
  })
})

afterEach(() => {
  Object.defineProperty(LazyJson.prototype, "value", valueDescriptor)
})

function setup(options: { sink?: Partial<HubEventSink>; onError?: (error: Error) => void } = {}) {
  const published: Published[] = []
  const readsAtPublish: number[] = []
  const base = createHubEventSink({
    publisher: {
      publish: (event: Published) => {
        readsAtPublish.push(reads)
        published.push(event)
      },
    } as never,
  })
  const sink: HubEventSink = { ...base, ...options.sink } as HubEventSink
  const channel = new MessageChannel()
  const events: Array<{ properties: unknown; readsOnArrival: number }> = []
  const observed: HubEventSink = {
    ...sink,
    event(event, meta) {
      events.push({ properties: event.properties, readsOnArrival: reads })
      sink.event(event, meta)
    },
    get ended() {
      return sink.ended
    },
    get resyncs() {
      return sink.resyncs
    },
    get acks() {
      return sink.acks
    },
  }
  const dispose = serveEventsPort(channel.port1, observed, options.onError)
  return {
    published,
    readsAtPublish,
    events,
    sink: observed,
    port: channel.port2,
    close() {
      dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

const send = (port: MessagePort, frame: Parameters<typeof encodeEventsFrameBytes>[0]) => {
  const buffer = encodeEventsFrameBytes(frame)
  port.postMessage(buffer, [buffer])
}

describe("serveEventsPort over byte frames", () => {
  test("a delta reaches the sink as LazyJson and is never parsed on the delivery path", async () => {
    const harness = setup()
    try {
      send(harness.port, eventFrame({ event: { id: "e1", type: "message.part.delta", properties: { delta: "hi" } } }))
      await pollUntil(() => harness.published.length === 1)
      const data = harness.published[0]?.data
      expect(data).toBeInstanceOf(LazyJson)
      expect((data as LazyJson).text).toBe('{"delta":"hi"}')
      expect(harness.events[0]?.readsOnArrival).toBe(harness.readsAtPublish[0])
      expect(reads).toBe(harness.readsAtPublish[0] ?? -1)
    } finally {
      harness.close()
    }
  })

  test("durable meta comes from the envelope", async () => {
    const harness = setup()
    try {
      send(
        harness.port,
        eventFrame({
          event: { id: "e1", type: "message.updated", properties: { sessionID: "ses_1" } },
          aggregateID: "ses_1",
          seq: 4,
        } as never),
      )
      await pollUntil(() => harness.published.length === 1)
      expect(harness.published[0]).toMatchObject({ id: "e1", durable: { aggregateID: "ses_1", seq: 4 } })
    } finally {
      harness.close()
    }
  })

  test("a blob reference is resolved by position and delivered as LazyJson", async () => {
    const positions: unknown[] = []
    const harness = setup({
      sink: {
        resolveBlob: async (position) => {
          positions.push(position)
          return { text: "big" }
        },
      },
    })
    try {
      send(
        harness.port,
        eventFrame({
          event: {
            id: "e1",
            type: "session.next.prompted",
            properties: { __eventBlobPosition: { aggregateID: "ses_1", seq: 2 }, frameBytes: 9_000_000 },
          },
          aggregateID: "ses_1",
          seq: 2,
        } as never),
      )
      await pollUntil(() => harness.published.length === 1)
      expect(positions).toEqual([{ aggregateID: "ses_1", seq: 2 }])
      const data = harness.published[0]?.data
      expect(data).toBeInstanceOf(LazyJson)
      expect((data as LazyJson).value).toEqual({ text: "big" })
    } finally {
      harness.close()
    }
  })

  test("a malformed frame takes the rejection path and asks the worker to resync with a byte frame", async () => {
    const errors: Error[] = []
    const harness = setup({ onError: (error) => errors.push(error) })
    try {
      const reply = new Promise<unknown>((resolve) => (harness.port.onmessage = (event) => resolve(event.data)))
      harness.port.postMessage(new Uint8Array([9, 0, 0, 0, 1]).buffer)
      await pollUntil(() => errors.length === 1)
      const data = await reply
      expect(data).toBeInstanceOf(ArrayBuffer)
      expect(jsDecoder.decode(new Uint8Array(data as ArrayBuffer))).toMatchObject({ kind: "resync" })
    } finally {
      harness.close()
    }
  })

  test("a durable gap heals and the resync request is a byte frame carrying the position", async () => {
    const harness = setup()
    try {
      const reply = new Promise<unknown>((resolve) => (harness.port.onmessage = (event) => resolve(event.data)))
      const durable = (seq: number) =>
        eventFrame({ event: { id: `e${seq}`, type: "message.updated", properties: {} }, aggregateID: "ses_1", seq } as never)
      send(harness.port, durable(1))
      send(harness.port, durable(3))
      const data = await reply
      expect(data).toBeInstanceOf(ArrayBuffer)
      expect(jsDecoder.decode(new Uint8Array(data as ArrayBuffer))).toMatchObject({
        kind: "resync",
        payload: { aggregateID: "ses_1", seq: 3 },
      })
    } finally {
      harness.close()
    }
  })
})
