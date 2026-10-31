// Worker-side egress sender over the shared JSON events transport (Q7/Q16).
//
// The sender turns coalesced batches of `event` frames into shared-envelope JSON
// strings, one port message per frame, bounded at 4 MiB per frame and per batch.
// Durable frames bypass the coalescing window and are stamped with a monotonic
// `(generation, ordinal)` when no writer meta is supplied.

import { describe, expect, test } from "bun:test"
import { recordingBridgeFrom } from "../../lib/recorders"
import { createEgressReceiver } from "@/session/egress/receiver"
import { createEgressSender } from "@/session/egress/sender"
import {
  type EventV2Event,
  ackFrame,
  endFrame,
  eventFrame,
  MAX_WORKER_FRAME_BYTES,
} from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"
import { readByteFrame } from "./byte-frames"

const deltaEvent = (id: string, text?: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: text === undefined ? { id } : { id, text },
})

function asBuffer(data: unknown): ArrayBuffer {
  if (data instanceof ArrayBuffer) return data
  throw new Error(`expected an ArrayBuffer byte frame, got ${Object.prototype.toString.call(data)}`)
}

function captureRaw(channel: MessageChannel): ArrayBuffer[] {
  const chunks: ArrayBuffer[] = []
  channel.port2.onmessage = (event: MessageEvent) => chunks.push(asBuffer(event.data))
  channel.port2.start()
  return chunks
}

describe("egress sender", () => {
  test("transfers every posted buffer to the port instead of copying it", () => {
    const posted: Array<{ readonly buffer: ArrayBuffer; readonly transfer: readonly unknown[]; readonly lengthAtPost: number }> = []
    const port = {
      postMessage: (buffer: ArrayBuffer, transfer: readonly unknown[]) => {
        posted.push({ buffer, transfer, lengthAtPost: buffer.byteLength })
        // The structured clone is what detaches the sender's copy.
        structuredClone(buffer, { transfer: [...transfer] as ArrayBuffer[] })
      },
    } as unknown as MessagePort
    const sender = createEgressSender({ port, maxBatch: 1 })

    sender.pushDurable({ id: "d", type: "message.updated", properties: {} }, { aggregateID: "agg", seq: 1 })
    sender.end()

    expect(posted.length).toBe(4)
    for (const message of posted) {
      expect(message.lengthAtPost).toBeGreaterThan(0)
      expect(message.transfer).toEqual([message.buffer])
      expect(message.buffer.byteLength).toBe(0)
    }
  })

  test("posts a coalesced delta batch as one ArrayBuffer message per event frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1, maxBatch: 3 })

    sender.pushDelta(deltaEvent("a", "Hel"))
    sender.pushDelta(deltaEvent("b", "lo "))
    sender.pushDelta(deltaEvent("c", "world"))

    await pollUntil(() => chunks.length === 3)
    expect(chunks.map(readByteFrame)).toEqual([
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
    expect(chunks.map(readByteFrame)).toEqual([
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
    expect(readByteFrame(chunks[0]!)).toEqual(
      eventFrame({ event, aggregateID: "s1", seq: 9 } as Parameters<typeof eventFrame>[0]),
    )
    expect(readByteFrame(chunks[1]!)).toEqual(ackFrame({ count: 1 }))
  })

  test("splits an over-cap batch before posting so every message stays within 4 MiB", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1, maxBatch: 2 })
    const pad = "x".repeat(2.5 * 1024 * 1024)

    // Two individually valid deltas whose combined wire size exceeds the batch
    // cap. The sender must split them rather than lose or throw away either one.
    sender.pushDelta(deltaEvent("a", pad))
    sender.pushDelta(deltaEvent("b", pad))

    await pollUntil(() => chunks.length === 2)
    expect(chunks.every((buffer) => buffer.byteLength <= 4 * 1024 * 1024)).toBe(true)
    expect(chunks.map((buffer) => readByteFrame(buffer).kind)).toEqual(["event", "event"])
    channel.port1.close()
  })

  test("represents an over-bound durable as a blob-position reference before posting", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })
    const huge = { id: "big", type: "message.updated", properties: { pad: "x".repeat(4 * 1024 * 1024) } }
    // A committed durable event must never be lost to the frame cap: it crosses
    // as a small position reference the hub resolves from the shared blob store.
    expect(() => sender.pushDurable(huge, { aggregateID: "agg", seq: 1 })).not.toThrow()
    await pollUntil(() => chunks.length === 2)
    expect(chunks.every((buffer) => buffer.byteLength <= 4 * 1024 * 1024)).toBe(true)
    const frame = readByteFrame(chunks[0]!)
    expect(frame.kind === "event" ? frame.payload.event.properties : undefined).toMatchObject({
      __eventBlobPosition: { aggregateID: "agg", seq: 1 },
    })
    expect(readByteFrame(chunks[1]!)).toEqual(ackFrame({ count: 1 }))
    channel.port1.close()
  })

  test("a delta-only batch carries the run-end flush ack but no durable ack", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    sender.pushDelta(deltaEvent("a"))
    sender.end()

    await pollUntil(() => chunks.length === 3)
    // The delta, the zero-count run-end boundary, then the end frame — no ack
    // counting a durable delivery.
    expect(chunks.map(readByteFrame)).toEqual([
      eventFrame({ event: deltaEvent("a") }),
      ackFrame({ count: 0 }),
      endFrame(),
    ])
  })

  test("end flushes buffered deltas then posts the flush ack and end frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    sender.pushDelta(deltaEvent("a"))
    expect(sender.buffered).toBe(1)
    sender.end()

    await pollUntil(() => chunks.length === 3)
    expect(chunks.map(readByteFrame)).toEqual([
      eventFrame({ event: deltaEvent("a") }),
      ackFrame({ count: 0 }),
      endFrame(),
    ])
    expect(sender.ended).toBe(true)
    sender.end()
    expect(chunks.length).toBe(3)
  })

  test("drops only an oversized non-durable delta and still posts the rest, ack and end", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const warnings: Array<{ readonly message: string; readonly level: string | undefined }> = []
    const sender = createEgressSender({
      port: channel.port1,
      maxBatch: 2,
      bridge: recordingBridgeFrom((line) => void warnings.push({ message: line.message, level: line.level })),
    })
    try {
      const huge: EventV2Event = {
        id: "huge",
        type: "message.part.delta",
        properties: { id: "huge", text: "x".repeat(MAX_WORKER_FRAME_BYTES) },
      }
      // The advisory delta alone exceeds the frame bound. It must not take the
      // whole batch (or the run boundary) down with it.
      sender.pushDelta(huge)
      sender.pushDelta(deltaEvent("kept", "still delivered"))
      sender.end()

      await pollUntil(() => chunks.length === 3, {
        message: "the run-end frame was lost with the oversized delta",
      })

      expect(chunks.map(readByteFrame)).toEqual([
        eventFrame({ event: deltaEvent("kept", "still delivered") }),
        ackFrame({ count: 0 }),
        endFrame(),
      ])
      expect(warnings).toHaveLength(1)
      expect(warnings[0]?.level).toBe("warn")
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })
})
