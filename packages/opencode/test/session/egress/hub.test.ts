// Phase 8.1 + 8.7 + 8.12 — hub egress over a real `MessagePort` (plan tasks 8.1, 8.7, 8.12).
//
// A `MessageChannel` stands in for the worker↔hub egress port on the main
// thread, exercising the actual port delivery semantics: framing/reassembly,
// commit-order delivery, the per-client byte stream (what an SSE route writes),
// and hub-owned termination.

import { describe, expect, test } from "bun:test"
import { EgressStreamOverflowError, createEgressHub } from "./hub"
import { decodeFrame, encodeFrame, type EgressFrame } from "@/session/egress/frame"
import { createEgressSender } from "@/session/egress/sender"
import { endFrame } from "@/session/worker/frame"
import { delta, deltasFrame, durable, durableFrame } from "../../lib/egress-fixtures"

async function pollUntil(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

function setup(options: { windowMs?: number; maxBatch?: number } = {}) {
  const channel = new MessageChannel()
  const hub = createEgressHub({ egressPort: channel.port2 })
  const sender = createEgressSender({ port: channel.port1, ...options })
  return { hub, sender, channel }
}

async function startConsumer(hub: ReturnType<typeof createEgressHub>, id: string) {
  const frames: EgressFrame[] = []
  const done = (async () => {
    for await (const bytes of hub.stream(id)) frames.push(decodeFrame(bytes))
  })()
  await pollUntil(() => hub.fanout.subscribers.length === 1)
  return { frames, done }
}

describe("egress hub over MessagePort", () => {
  test("coalesced deltas arrive in order", async () => {
    const { hub, sender } = setup({ windowMs: 16 })
    const subscriber = hub.fanout.subscribe("c")
    sender.pushDelta(delta("1"))
    sender.pushDelta(delta("2"))
    await pollUntil(() => hub.frames === 2)
    expect(subscriber.drain()).toEqual([deltasFrame("1"), deltasFrame("2")])
    // The hub forwarded the worker's bytes verbatim — no re-encode for clients.
    expect(hub.fanout.encodeCount).toBe(0)
    hub.close()
  })

  test("reassembles a frame split across two port deliveries", async () => {
    const { hub, channel } = setup()
    const subscriber = hub.fanout.subscribe("c")
    const bytes = encodeFrame(durableFrame("d", 1))
    const split = bytes.byteLength - 5
    channel.port1.postMessage(bytes.subarray(0, split))
    channel.port1.postMessage(bytes.subarray(split))
    await pollUntil(() => hub.frames === 1)
    expect(hub.receiver.pendingBytes).toBe(0)
    expect(subscriber.drain()).toEqual([durableFrame("d", 1)])
    hub.close()
  })

  test("preserves commit order for durable frames", async () => {
    const { hub, sender } = setup()
    const subscriber = hub.fanout.subscribe("c")
    sender.pushDurable(durable("a"), { generation: 1, ordinal: 1 })
    sender.pushDurable(durable("b"), { generation: 1, ordinal: 2 })
    await pollUntil(() => hub.frames === 2)
    const frames = subscriber.drain()
    expect(frames.map((frame) => (frame.kind === "event" ? frame.payload.ordinal ?? -1 : -1))).toEqual([1, 2])
    hub.close()
  })

  test("fails loud on an out-of-order durable ordinal", async () => {
    const { hub, sender } = setup()
    sender.pushDurable(durable("a"), { generation: 1, ordinal: 2 })
    sender.pushDurable(durable("b"), { generation: 1, ordinal: 1 })
    await pollUntil(() => hub.receiver.lastError !== undefined)
    expect(hub.receiver.lastError?.message).toContain("out-of-order")
  })

  test("an SSE consumer receives the full streamed session in order", async () => {
    const { hub, sender } = setup()
    const consumer = await startConsumer(hub, "sse")
    sender.pushDelta(delta("1"))
    sender.pushDurable(durable("done"), { generation: 1, ordinal: 1 })
    sender.end()
    await consumer.done
    expect(consumer.frames).toEqual([deltasFrame("1"), durableFrame("done", 1), endFrame()])
    hub.close()
  })

  test("terminate flushes durable, drops buffered non-durable, then ends", async () => {
    const { hub, sender } = setup({ maxBatch: 1 })
    const subscriber = hub.fanout.subscribe("sse")
    sender.pushDelta(delta("dropped"))
    sender.pushDurable(durable("kept"), { generation: 1, ordinal: 1 })
    await pollUntil(() => hub.frames === 2)
    hub.close()
    expect(subscriber.drain()).toEqual([durableFrame("kept", 1), endFrame()])
  })

  test("close is idempotent", () => {
    const { hub } = setup()
    hub.close()
    hub.close()
    expect(hub.closed).toBe(true)
  })

  test("a subscriber that arrives after termination still receives the end frame", async () => {
    const { hub } = setup()
    hub.close()

    const frames: EgressFrame[] = []
    for await (const bytes of hub.stream("late")) frames.push(decodeFrame(bytes))

    // The late SSE client must observe the terminal marker, not an empty stream.
    expect(frames).toEqual([endFrame()])
  })

  test("a subscriber that arrives after the worker's end frame still receives it", async () => {
    const { hub, sender } = setup()
    sender.end()
    await pollUntil(() => hub.closed)

    const frames: EgressFrame[] = []
    for await (const bytes of hub.stream("late")) frames.push(decodeFrame(bytes))
    expect(frames).toEqual([endFrame()])
  })

  test("surfaces a typed error when a subscriber's durable queue overflows", async () => {
    const channel = new MessageChannel()
    const hub = createEgressHub({ egressPort: channel.port2, bound: 1 })
    const sender = createEgressSender({ port: channel.port1 })
    // Occupy the only slot and never drain, so the second durable saturates it.
    const occupied = hub.fanout.subscribe("c")
    sender.pushDurable(durable("a"), { generation: 1, ordinal: 1 })
    sender.pushDurable(durable("b"), { generation: 1, ordinal: 2 })
    await pollUntil(() => hub.frames === 2)
    expect(occupied.overflowed).toBe(true)

    const consumer = (async () => {
      for await (const _bytes of hub.stream("c")) {
        // Overflow is terminal: the stream must reject before delivering more.
      }
    })()
    await expect(consumer).rejects.toThrow(EgressStreamOverflowError)
    hub.close()
  })
})
