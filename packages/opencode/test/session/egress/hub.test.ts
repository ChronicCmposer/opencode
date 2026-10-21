// Phase 8.1 + 8.7 + 8.12 — hub egress over a real `MessagePort` (plan tasks 8.1, 8.7, 8.12).
//
// A `MessageChannel` stands in for the worker↔hub egress port on the main
// thread, exercising the actual port delivery semantics: framing/reassembly,
// commit-order delivery, the per-client byte stream (what an SSE route writes),
// and hub-owned termination.

import { describe, expect, test } from "bun:test"
import { createEgressHub } from "@/session/egress/hub"
import { decodeFrame, encodeFrame, type EgressEvent, type EgressFrame } from "@/session/egress/frame"
import { createEgressSender } from "@/session/egress/sender"

const delta = (id: string): EgressEvent => ({ id, type: "message.part.updated", data: { id } })
const durable = (id: string): EgressEvent => ({ id, type: "message.updated", data: { id } })

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
  test("coalesced deltas arrive as one frame", async () => {
    const { hub, sender } = setup({ windowMs: 16 })
    const subscriber = hub.fanout.subscribe("c")
    sender.pushDelta(delta("1"))
    sender.pushDelta(delta("2"))
    await pollUntil(() => hub.frames === 1)
    expect(subscriber.drain()).toEqual([{ kind: "deltas", v: 1, events: [delta("1"), delta("2")] }])
    // The hub forwarded the worker's bytes verbatim — no re-encode for clients.
    expect(hub.fanout.encodeCount).toBe(0)
    hub.close()
  })

  test("reassembles a frame split across two port deliveries", async () => {
    const { hub, channel } = setup()
    const subscriber = hub.fanout.subscribe("c")
    const bytes = encodeFrame({ kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: durable("d") })
    const split = bytes.byteLength - 5
    channel.port1.postMessage(bytes.subarray(0, split))
    channel.port1.postMessage(bytes.subarray(split))
    await pollUntil(() => hub.frames === 1)
    expect(hub.receiver.pendingBytes).toBe(0)
    expect(subscriber.drain()).toEqual([
      { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: durable("d") },
    ])
    hub.close()
  })

  test("preserves commit order for durable frames", async () => {
    const { hub, sender } = setup()
    const subscriber = hub.fanout.subscribe("c")
    sender.pushDurable(durable("a"), { generation: 1, ordinal: 1 })
    sender.pushDurable(durable("b"), { generation: 1, ordinal: 2 })
    await pollUntil(() => hub.frames === 2)
    const frames = subscriber.drain()
    expect(frames.map((frame) => (frame.kind === "durable" ? frame.meta.ordinal : -1))).toEqual([1, 2])
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
    expect(consumer.frames).toEqual([
      { kind: "deltas", v: 1, events: [delta("1")] },
      { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: durable("done") },
      { kind: "end", v: 1 },
    ])
    hub.close()
  })

  test("terminate flushes durable, drops buffered non-durable, then ends", async () => {
    const { hub, sender } = setup({ maxBatch: 1 })
    const subscriber = hub.fanout.subscribe("sse")
    sender.pushDelta(delta("dropped"))
    sender.pushDurable(durable("kept"), { generation: 1, ordinal: 1 })
    await pollUntil(() => hub.frames === 2)
    hub.close()
    expect(subscriber.drain()).toEqual([
      { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: durable("kept") },
      { kind: "end", v: 1 },
    ])
  })

  test("close is idempotent", () => {
    const { hub } = setup()
    hub.close()
    hub.close()
    expect(hub.closed).toBe(true)
  })
})
