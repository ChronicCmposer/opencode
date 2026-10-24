// W2 — hub-side egress receiver over the events envelope (spec Q6/Q16/Q21).
//
// Incoming chunks are reassembled with the length-prefixed codec. Event frames
// are delivered to `onFrame` in arrival order; durable `(generation, ordinal)`
// pairs are checked for monotonicity and fail loud if they regress. The
// `end`/`resync`/`ack` control frames are surfaced distinctly, and backpressure
// drops non-durable deltas (never durables) while emitting a resync.

import { describe, expect, test } from "bun:test"
import { type FrameChunk, createFrameDecoder, encodeFrame } from "@/session/egress/frame"
import { createEgressReceiver, DurableOrderError } from "@/session/egress/receiver"
import { type EventsFrame, ackFrame, endFrame, eventFrame, resyncFrame } from "@/session/worker/frame"
import { deltasFrame, durableFrame } from "../../lib/egress-fixtures"
import { pollUntil } from "../../lib/poll"

const deltaEvent = (id: string) => ({ id, type: "message.part.delta", properties: { id } })
const durableEvent = (id: string) => ({ id, type: "message.updated", properties: { id } })

interface ReceiverHarness {
  readonly port1: MessagePort
  readonly frames: EventsFrame[]
  readonly ends: number
  readonly resyncs: (EventsFrame & { readonly kind: "resync" })["payload"][]
  readonly acks: (EventsFrame & { readonly kind: "ack" })["payload"][]
  readonly errors: Error[]
  readonly receiver: ReturnType<typeof createEgressReceiver>
  close(): void
}

function setup(options: { pressure?: () => boolean } = {}): ReceiverHarness {
  const channel = new MessageChannel()
  const frames: EventsFrame[] = []
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
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/out-of-order durable/)
    expect(harness.receiver.lastOrdinal()).toEqual({ generation: 1, ordinal: 2 })
    harness.close()
  })

  test("detects an ordinal gap within a generation and fails loud", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("a"), generation: 1, ordinal: 1 })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("c"), generation: 1, ordinal: 3 })))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/gap durable/)
    // The gap is not accepted as the new last ordinal.
    expect(harness.receiver.lastOrdinal()).toEqual({ generation: 1, ordinal: 1 })
    harness.close()
  })

  test("a generation bump resets the sequence, so no gap is reported across generations", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("a"), generation: 1, ordinal: 9 })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("b"), generation: 2, ordinal: 5 })))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.errors).toEqual([])
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

describe("egress receiver pressure-resync contract", () => {
  test("every dropped delta yields exactly one resync and durables are never dropped", async () => {
    let underPressure = true
    const harness = setup({ pressure: () => underPressure })

    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("d1") })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("d2") })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: durableEvent("keep"), generation: 1, ordinal: 1 })))
    await pollUntil(() => harness.receiver.dropped === 2 && harness.frames.length === 1)

    // Under sustained pressure each dropped delta surfaces exactly one overflow
    // resync, while the durable is delivered and never counted as dropped.
    expect(harness.receiver.dropped).toBe(2)
    expect(harness.receiver.resyncs).toBe(2)
    expect(harness.resyncs).toEqual([undefined, undefined])
    expect(harness.frames).toEqual([eventFrame({ event: durableEvent("keep"), generation: 1, ordinal: 1 })])

    // Once pressure clears, deltas flow again with no further drop or resync.
    underPressure = false
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("d3") })))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.receiver.dropped).toBe(2)
    expect(harness.receiver.resyncs).toBe(2)
    harness.close()
  })
})

describe("egress receiver framing boundaries", () => {
  test("accepts an ArrayBuffer chunk", async () => {
    const channel = new MessageChannel()
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: () => {} })
    const bytes = encodeFrame(endFrame())
    channel.port1.postMessage(bytes.buffer)
    await pollUntil(() => receiver.ended)
    expect(receiver.lastError).toBeUndefined()
    receiver.close()
    channel.port1.close()
  })

  test("accepts a typed-array view over a larger buffer", async () => {
    const channel = new MessageChannel()
    const frames: EventsFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const framed = encodeFrame(deltasFrame("v1"))
    const padded = new Uint8Array(framed.byteLength + 8)
    padded.set(framed, 4)
    const view = padded.subarray(4, 4 + framed.byteLength)
    channel.port1.postMessage(view)
    await pollUntil(() => frames.length === 1)
    expect(receiver.lastError).toBeUndefined()
    expect(frames[0]).toEqual(deltasFrame("v1"))
    receiver.close()
    channel.port1.close()
  })

  test("does not commit trailing bytes when a batch fails mid-way", async () => {
    const channel = new MessageChannel()
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: () => {} })
    const first = encodeFrame(durableFrame("d2", 2))
    const stale = encodeFrame(durableFrame("d1", 1))
    const partial = first.subarray(0, first.byteLength - 2)
    channel.port1.postMessage(new Uint8Array([...first, ...stale, ...partial]))
    await pollUntil(() => receiver.lastError !== undefined)
    expect(receiver.pendingBytes).toBe(0)
    receiver.close()
    channel.port1.close()
  })

  test("reassembles a frame delivered one byte at a time", async () => {
    const harness = setup()
    const frame = eventFrame({ event: deltaEvent("tiny") })
    const bytes = encodeFrame(frame)
    for (let offset = 0; offset < bytes.byteLength; offset++) {
      harness.port1.postMessage(bytes.subarray(offset, offset + 1))
    }
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.frames[0]).toEqual(frame)
    expect(harness.receiver.pendingBytes).toBe(0)
    harness.close()
  })
})

describe("egress frame decoder buffering", () => {
  test("accumulates an incomplete frame until its remainder arrives", () => {
    const decoder = createFrameDecoder(8)
    const bytes = encodeFrame(eventFrame({ event: { id: "big", type: "t", properties: { text: "x".repeat(2048) } } }))
    const split = bytes.byteLength - 3

    expect(decoder.push(bytes.subarray(0, split))).toEqual([])
    expect(decoder.pending).toBe(split)

    const [chunk] = decoder.push(bytes.subarray(split))
    expect(chunk?.bytes).toEqual(bytes)
    expect(decoder.pending).toBe(0)
  })

  test("reassembles a frame split into single bytes from a preallocated buffer", () => {
    const decoder = createFrameDecoder(8)
    const frame = eventFrame({ event: { id: "byte", type: "t", properties: { text: "y".repeat(4096) } } })
    const bytes = encodeFrame(frame)

    const chunks: FrameChunk[] = []
    for (let offset = 0; offset < bytes.byteLength; offset++) {
      chunks.push(...decoder.push(bytes.subarray(offset, offset + 1)))
    }
    expect(chunks).toHaveLength(1)
    expect(chunks[0]?.frame).toEqual(frame)
    expect(chunks[0]?.bytes).toEqual(bytes)
    expect(decoder.pending).toBe(0)
  })

  test("reset discards buffered bytes", () => {
    const decoder = createFrameDecoder(8)
    decoder.push(encodeFrame(endFrame()).subarray(0, 2))
    expect(decoder.pending).toBe(2)
    decoder.reset()
    expect(decoder.pending).toBe(0)
  })
})
