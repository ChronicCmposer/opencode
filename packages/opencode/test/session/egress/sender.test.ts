// W2 — worker-side egress sender over the events envelope (spec Q7/Q16).
//
// The sender turns coalesced batches of `event` frames into ONE length-prefixed,
// versioned byte buffer posted with a transferred buffer (zero-copy). Durable
// frames bypass the coalescing window and are stamped with a monotonic
// `(generation, ordinal)` when no writer meta is supplied.

import { describe, expect, test } from "bun:test"
import { decodeFrames } from "@/session/egress/frame"
import { createEgressSender } from "@/session/egress/sender"
import { type EventV2Event, endFrame, eventFrame } from "@/session/worker/frame"

async function pollUntil(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

const deltaEvent = (id: string, text?: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: text === undefined ? { id } : { id, text },
})

function asBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data
  throw new Error(`expected a Uint8Array chunk, got ${Object.prototype.toString.call(data)}`)
}

function captureRaw(channel: MessageChannel): Uint8Array[] {
  const chunks: Uint8Array[] = []
  channel.port2.onmessage = (event: MessageEvent) => chunks.push(asBytes(event.data))
  channel.port2.start()
  return chunks
}

describe("egress sender", () => {
  test("posts a coalesced delta batch as ONE buffer carrying every event frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1, maxBatch: 3 })

    sender.pushDelta(deltaEvent("a", "Hel"))
    sender.pushDelta(deltaEvent("b", "lo "))
    sender.pushDelta(deltaEvent("c", "world"))

    await pollUntil(() => chunks.length === 1)
    expect(decodeFrames(chunks[0]).frames).toEqual([
      eventFrame({ event: deltaEvent("a", "Hel") }),
      eventFrame({ event: deltaEvent("b", "lo ") }),
      eventFrame({ event: deltaEvent("c", "world") }),
    ])
  })

  test("durable frames bypass the window and are stamped with monotonic ordinals", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    const first = { id: "d1", type: "message.updated", properties: {} }
    const second = { id: "d2", type: "message.updated", properties: {} }
    sender.pushDurable(first)
    sender.pushDurable(second)

    await pollUntil(() => chunks.length === 2)
    const decoded = chunks.map((chunk) => decodeFrames(chunk).frames[0])
    expect(decoded).toEqual([
      eventFrame({ event: first, generation: 1, ordinal: 1 }),
      eventFrame({ event: second, generation: 1, ordinal: 2 }),
    ])
  })

  test("an explicit writer meta overrides the fallback ordinal", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    const event = { id: "d", type: "message.updated", properties: {} }
    sender.pushDurable(event, { generation: 4, ordinal: 9 })

    await pollUntil(() => chunks.length === 1)
    expect(decodeFrames(chunks[0]).frames[0]).toEqual(eventFrame({ event, generation: 4, ordinal: 9 }))
  })

  test("end flushes buffered deltas then posts the end frame", async () => {
    const channel = new MessageChannel()
    const chunks = captureRaw(channel)
    const sender = createEgressSender({ port: channel.port1 })

    sender.pushDelta(deltaEvent("a"))
    expect(sender.buffered).toBe(1)
    sender.end()

    await pollUntil(() => chunks.length === 2)
    expect(chunks.flatMap((chunk) => decodeFrames(chunk).frames)).toEqual([
      eventFrame({ event: deltaEvent("a") }),
      endFrame(),
    ])
    expect(sender.ended).toBe(true)
  })
})
