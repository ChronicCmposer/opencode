// W2 — worker-side egress sender over the shared JSON events transport (Q7/Q16).
//
// The sender turns coalesced batches of `event` frames into shared-envelope JSON
// strings, one port message per frame, bounded at 4 MiB per frame and per batch.
// Durable frames bypass the coalescing window and are stamped with a monotonic
// `(generation, ordinal)` when no writer meta is supplied.

import { describe, expect, test } from "bun:test"
import { FrameTooLargeError, decodeFrame } from "@/session/egress/frame"
import { createEgressSender } from "@/session/egress/sender"
import { type EventV2Event, ackFrame, endFrame, eventFrame } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"

const deltaEvent = (id: string, text?: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: text === undefined ? { id } : { id, text },
})

function asText(data: unknown): string {
  if (typeof data === "string") return data
  throw new Error(`expected a JSON string, got ${Object.prototype.toString.call(data)}`)
}

function captureRaw(channel: MessageChannel): string[] {
  const chunks: string[] = []
  channel.port2.onmessage = (event: MessageEvent) => chunks.push(asText(event.data))
  channel.port2.start()
  return chunks
}

describe("egress sender", () => {
  test("posts a coalesced delta batch as one JSON message per event frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1, maxBatch: 3 })

    sender.pushDelta(deltaEvent("a", "Hel"))
    sender.pushDelta(deltaEvent("b", "lo "))
    sender.pushDelta(deltaEvent("c", "world"))

    await pollUntil(() => chunks.length === 3)
    expect(chunks.map(decodeFrame)).toEqual([
      eventFrame({ event: deltaEvent("a", "Hel") }),
      eventFrame({ event: deltaEvent("b", "lo ") }),
      eventFrame({ event: deltaEvent("c", "world") }),
    ])
  })

  test("durable frames bypass the window, are stamped with monotonic ordinals, and are acked", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    const first = { id: "d1", type: "message.updated", properties: {} }
    const second = { id: "d2", type: "message.updated", properties: {} }
    sender.pushDurable(first, { aggregateID: "agg", seq: 1 })
    sender.pushDurable(second, { aggregateID: "agg", seq: 2 })

    await pollUntil(() => chunks.length === 4)
    // Each durable hand-off is followed by an ack frame carrying the durable count.
    expect(chunks.map(decodeFrame)).toEqual([
      eventFrame({ event: first, aggregateID: "agg", seq: 1 } as Parameters<typeof eventFrame>[0]),
      ackFrame({ count: 1 }),
      eventFrame({ event: second, aggregateID: "agg", seq: 2 } as Parameters<typeof eventFrame>[0]),
      ackFrame({ count: 1 }),
    ])
  })

  test("an explicit aggregate meta is stamped on the durable frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    const event = { id: "d", type: "message.updated", properties: {} }
    sender.pushDurable(event, { aggregateID: "s1", seq: 9 })

    await pollUntil(() => chunks.length === 2)
    expect(decodeFrame(chunks[0]!)).toEqual(
      eventFrame({ event, aggregateID: "s1", seq: 9 } as Parameters<typeof eventFrame>[0]),
    )
    expect(decodeFrame(chunks[1]!)).toEqual(ackFrame({ count: 1 }))
  })

  test("rejects an over-bound durable before posting it", () => {
    const channel = new MessageChannel()
    const sender = createEgressSender({ port: channel.port1 })
    const huge = { id: "big", type: "message.updated", properties: { pad: "x".repeat(4 * 1024 * 1024) } }
    expect(() => sender.pushDurable(huge, { aggregateID: "agg", seq: 1 })).toThrow(FrameTooLargeError)
    channel.port1.close()
  })

  test("a delta-only batch is not acked", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    sender.pushDelta(deltaEvent("a"))
    sender.end()

    await pollUntil(() => chunks.length === 2)
    // Only the delta frame and the end frame — no ack for non-durable deltas.
    expect(chunks.map(decodeFrame)).toEqual([eventFrame({ event: deltaEvent("a") }), endFrame()])
  })

  test("end flushes buffered deltas then posts the end frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    sender.pushDelta(deltaEvent("a"))
    expect(sender.buffered).toBe(1)
    sender.end()

    await pollUntil(() => chunks.length === 2)
    expect(chunks.map(decodeFrame)).toEqual([eventFrame({ event: deltaEvent("a") }), endFrame()])
    expect(sender.ended).toBe(true)
  })
})
