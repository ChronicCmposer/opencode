// Step 32 — durable identity is the aggregate log's `(aggregateID, seq)`.
//
// There is no sender-global ordinal: a durable frame that omits its aggregate
// identity is rejected, and ordering is checked per aggregate, so two interleaved
// aggregates never look like a gap in each other's stream.

import { describe, expect, test } from "bun:test"
import { type DurableMeta, encodeFrame } from "@/session/egress/frame"
import { createEgressReceiver, DurableOrderError } from "@/session/egress/receiver"
import { createEgressSender } from "@/session/egress/sender"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { type EventV2Event, type EventsFrame, eventFrame } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"

const event = (id: string): EventV2Event => ({ id, type: "message.updated", properties: { id } })

/** Build a durable frame carrying aggregate identity (frame.ts fields). */
function aggregateFrame(eventID: string, meta: DurableMeta): EventsFrame {
  return eventFrame({ event: event(eventID), aggregateID: meta.aggregateID, seq: meta.seq } as never)
}

interface Harness {
  readonly port1: MessagePort
  readonly frames: EventsFrame[]
  readonly errors: Error[]
  readonly receiver: ReturnType<typeof createEgressReceiver>
  close(): void
}

function setup(): Harness {
  const channel = new MessageChannel()
  const frames: EventsFrame[] = []
  const errors: Error[] = []
  const receiver = createEgressReceiver({
    port: channel.port2,
    onFrame: (frame) => frames.push(frame),
    onError: (error) => errors.push(error),
  })
  return {
    port1: channel.port1,
    frames,
    errors,
    receiver,
    close() {
      receiver.close()
      channel.port1.close()
    },
  }
}

describe("durable aggregate identity", () => {
  test("the sender has no global ordinal fallback: durable meta is mandatory", () => {
    const channel = new MessageChannel()
    const sender = createEgressSender({ port: channel.port1 })
    const pushWithoutMeta = sender.pushDurable as unknown as (event: EventV2Event) => void
    expect(() => pushWithoutMeta.call(sender, event("d"))).toThrow(/aggregateID/)
    channel.port1.close()
  })

  test("a durable without (aggregateID, seq) is rejected at the boundary", () => {
    const harness = setup()
    // A half-specified identity must never be accepted as a durable.
    harness.port1.postMessage(
      encodeFrame(eventFrame({ event: event("d"), aggregateID: "a" } as never)),
    )
    harness.port1.postMessage(
      encodeFrame(eventFrame({ event: event("d"), seq: 1 } as never)),
    )
    return pollUntil(() => harness.errors.length >= 1).then(() => {
      expect(harness.frames).toEqual([])
      harness.close()
    })
  })

  test("interleaved aggregates are ordered per aggregate, never as a false gap", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(aggregateFrame("a1", { aggregateID: "a", seq: 1 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("b1", { aggregateID: "b", seq: 1 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("a2", { aggregateID: "a", seq: 2 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("b2", { aggregateID: "b", seq: 2 })))
    await pollUntil(() => harness.frames.length === 4)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastDurable("a")).toEqual({ aggregateID: "a", seq: 2 })
    expect(harness.receiver.lastDurable("b")).toEqual({ aggregateID: "b", seq: 2 })
    harness.close()
  })

  test("a real gap within one aggregate is still rejected", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(aggregateFrame("a1", { aggregateID: "a", seq: 1 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("b1", { aggregateID: "b", seq: 1 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("a3", { aggregateID: "a", seq: 3 })))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/gap/)
    harness.close()
  })

  test("a stale seq within one aggregate is out-of-order even when interleaved", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(aggregateFrame("a2", { aggregateID: "a", seq: 2 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("b5", { aggregateID: "b", seq: 5 })))
    harness.port1.postMessage(encodeFrame(aggregateFrame("a1", { aggregateID: "a", seq: 1 })))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/out-of-order/)
    harness.close()
  })

  test("the legitimate zero-based first sequence (seq 0) is a durable frame", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(aggregateFrame("a0", { aggregateID: "a", seq: 0 })))
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastDurable("a")).toEqual({ aggregateID: "a", seq: 0 })
    harness.close()
  })

  test("the client forwards the first durable of a fresh aggregate (seq 0)", async () => {
    const channel = new MessageChannel()
    const chunks: string[] = []
    channel.port2.onmessage = (message: MessageEvent) => chunks.push(message.data as string)
    channel.port2.start()
    const client = createWorkerEgressClient(channel.port1, { windowMs: 0 })
    client.emitDurable(event("d0"), { aggregateID: "agg", seq: 0 })
    await pollUntil(() => chunks.length >= 1)
    const decoded = JSON.parse(chunks[0]!) as { payload: Record<string, unknown> }
    expect(decoded.payload["aggregateID"]).toBe("agg")
    expect(decoded.payload["seq"]).toBe(0)
    client.dispose()
  })

  test("the egress client forwards explicit aggregate identity over a real port", async () => {
    const channel = new MessageChannel()
    const chunks: string[] = []
    channel.port2.onmessage = (message: MessageEvent) => chunks.push(message.data as string)
    channel.port2.start()
    const client = createWorkerEgressClient(channel.port1)
    client.emitDurable(event("d1"), { aggregateID: "agg", seq: 7 })
    await pollUntil(() => chunks.length >= 1)
    const decoded = JSON.parse(chunks[0]!) as { payload: Record<string, unknown> }
    expect(decoded.payload["aggregateID"]).toBe("agg")
    expect(decoded.payload["seq"]).toBe(7)
    expect(decoded.payload["generation"]).toBeUndefined()
    expect(decoded.payload["ordinal"]).toBeUndefined()
    client.dispose()
  })
})
