// W2 — hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).
//
// Incoming chunks are reassembled with the length-prefixed codec. Event frames
// are delivered to `onFrame` in arrival order; durable `(generation, ordinal)`
// pairs are checked for monotonicity and fail loud if they regress. The
// `end`/`resync`/`ack` control frames are surfaced distinctly, and backpressure
// drops non-durable deltas (never durables) while emitting a resync.

import { describe, expect, test } from "bun:test"
import { encodeFrame, type EgressFrame } from "@/session/egress/frame"
import { createEgressReceiver } from "@/session/egress/receiver"
import { type EventsFrame, ackFrame, endFrame, eventFrame, resyncFrame } from "@/session/worker/frame"

async function pollUntil(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

const deltaEvent = (id: string) => ({ id, type: "message.part.delta", properties: { id } })
const durableEvent = (id: string) => ({ id, type: "message.updated", properties: { id } })

interface ReceiverHarness {
  readonly port1: MessagePort
  readonly frames: EgressFrame[]
  readonly ends: number
  readonly resyncs: (EventsFrame & { readonly kind: "resync" })["payload"][]
  readonly acks: (EventsFrame & { readonly kind: "ack" })["payload"][]
  readonly errors: Error[]
  readonly receiver: ReturnType<typeof createEgressReceiver>
  close(): void
}

function setup(options: { pressure?: () => boolean } = {}): ReceiverHarness {
  const channel = new MessageChannel()
  const frames: EgressFrame[] = []
  const errors: Error[] = []
  const resyncs: ReceiverHarness["resyncs"] = []
  const acks: ReceiverHarness["acks"] = []
  let ends = 0
  const receiver = createEgressReceiver({
    port: channel.port2,
    onFrame: (frame) => frames.push(frame),
    onEnd: () => {
      ends++
    },
    onResync: (payload) => resyncs.push(payload),
    onAck: (payload) => acks.push(payload),
    onError: (error) => errors.push(error),
    ...(options.pressure === undefined ? {} : { pressure: options.pressure }),
  })
  return {
    port1: channel.port1,
    frames,
    resyncs,
    acks,
    errors,
    receiver,
    get ends() {
      return ends
    },
    close() {
      receiver.close()
      channel.port1.close()
    },
  }
}

describe("egress receiver over event frames", () => {
  test("delivers event frames verbatim with their exact wire bytes", async () => {
    const harness = setup()
    const frame = eventFrame({ event: deltaEvent("a") })
    harness.port1.postMessage(encodeFrame(frame))
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.frames[0]).toEqual(frame)
    expect(harness.receiver.received).toBe(1)
    expect(harness.receiver.pendingBytes).toBe(0)
    harness.close()
  })

  test("reassembles a frame split across deliveries", async () => {
    const harness = setup()
    const bytes = encodeFrame(eventFrame({ event: deltaEvent("a") }))
    harness.port1.postMessage(bytes.subarray(0, bytes.byteLength - 2))
    harness.port1.postMessage(bytes.subarray(bytes.byteLength - 2))
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.frames[0]).toEqual(eventFrame({ event: deltaEvent("a") }))
    harness.close()
  })

  test("surfaces end, resync, and ack distinctly (never swallows them)", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(resyncFrame({ pattern: "message.*" })))
    harness.port1.postMessage(encodeFrame(ackFrame({ count: 3 })))
    harness.port1.postMessage(encodeFrame(endFrame()))
    await pollUntil(() => harness.ends === 1 && harness.resyncs.length === 1 && harness.acks.length === 1)
    expect(harness.receiver.ended).toBe(true)
    expect(harness.resyncs).toEqual([{ pattern: "message.*" }])
    expect(harness.acks).toEqual([{ count: 3 }])
    // Control frames are not data-plane frames.
    expect(harness.frames).toEqual([])
    harness.close()
  })

  test("enforces durable (generation, ordinal) monotonicity and fails loud", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("d2"), generation: 1, ordinal: 2 })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("d1"), generation: 1, ordinal: 1 })))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]?.message).toMatch(/out-of-order durable/)
    expect(harness.receiver.lastOrdinal()).toEqual({ generation: 1, ordinal: 2 })
    harness.close()
  })

  test("accepts a higher generation even when the ordinal resets", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("a"), generation: 1, ordinal: 9 })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("b"), generation: 2, ordinal: 1 })))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastOrdinal()).toEqual({ generation: 2, ordinal: 1 })
    harness.close()
  })

  test("backpressure drops non-durable deltas, keeps durables, and emits a resync", async () => {
    const harness = setup({ pressure: () => true })
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("dropped") })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("kept"), generation: 1, ordinal: 1 })))
    await pollUntil(() => harness.receiver.dropped === 1 && harness.frames.length === 1)
    expect(harness.frames).toEqual([eventFrame({ event: durableEvent("kept"), generation: 1, ordinal: 1 })])
    expect(harness.receiver.dropped).toBe(1)
    expect(harness.resyncs).toEqual([undefined])
    harness.close()
  })

  test("without pressure no delta is dropped", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("a") })))
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.receiver.dropped).toBe(0)
    expect(harness.resyncs).toEqual([])
    harness.close()
  })

  test("a malformed frame fails loud and stops the receiver", async () => {
    const harness = setup()
    const body = new TextEncoder().encode("{not json")
    const bytes = new Uint8Array(4 + body.byteLength)
    new DataView(bytes.buffer).setUint32(0, body.byteLength, false)
    bytes.set(body, 4)
    harness.port1.postMessage(bytes)
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.receiver.lastError).toBeDefined()
    expect(harness.receiver.pendingBytes).toBe(0)
    harness.close()
  })
})
