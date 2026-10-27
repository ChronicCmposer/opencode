// W2 — hub-side egress receiver over the shared JSON events transport (Q6/Q16/Q21).
//
// Every port message is one shared-envelope JSON string. Event frames are
// delivered to `onFrame` in arrival order; durable `(aggregateID, seq)` pairs
// are checked for monotonicity and fail loud if they regress. The
// `end`/`resync`/`ack` control frames are surfaced distinctly, and backpressure
// drops non-durable deltas (never durables) while emitting a resync.

import { describe, expect, test } from "bun:test"
import { MAX_FRAME_BYTES, encodeFrame } from "@/session/egress/frame"
import { createEgressReceiver, DurableOrderError } from "@/session/egress/receiver"
import { ProtocolError, type EventsFrame, ackFrame, endFrame, eventFrame, resyncFrame } from "@/session/worker/frame"
import { deltasFrame, durableFrame } from "../../lib/egress-fixtures"
import { pollUntil } from "../../lib/poll"

const deltaEvent = (id: string) => ({ id, type: "message.part.delta", properties: { id } })
const durableEvent = (id: string) => ({ id, type: "message.updated", properties: { id } })

/** A durable frame carrying its aggregate identity. */
const durableFrameOf = (id: string, aggregateID: string, seq: number) =>
  eventFrame({ event: durableEvent(id), aggregateID, seq } as never)

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
  test("delivers event frames verbatim with their exact JSON wire text", async () => {
    const harness = setup()
    const frame = eventFrame({ event: deltaEvent("a") })
    const text = encodeFrame(frame)
    harness.port1.postMessage(text)
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.frames[0]).toEqual(frame)
    expect(harness.receiver.received).toBe(1)
    expect(harness.receiver.pendingBytes).toBe(0)
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

  test("enforces per-aggregate durable monotonicity and fails loud", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(durableFrameOf("d2", "a", 2)))
    harness.port1.postMessage(encodeFrame(durableFrameOf("d1", "a", 1)))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/out-of-order durable/)
    expect(harness.receiver.lastDurable("a")).toEqual({ aggregateID: "a", seq: 2 })
    harness.close()
  })

  test("detects a seq gap within one aggregate and fails loud", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(durableFrameOf("a", "agg", 1)))
    harness.port1.postMessage(encodeFrame(durableFrameOf("c", "agg", 3)))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/gap durable/)
    // The gap is not accepted as the new last position.
    expect(harness.receiver.lastDurable("agg")).toEqual({ aggregateID: "agg", seq: 1 })
    harness.close()
  })

  test("each aggregate has its own cursor, so interleaving is never a gap", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(durableFrameOf("a", "a", 9)))
    harness.port1.postMessage(encodeFrame(durableFrameOf("b", "b", 5)))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastDurable("a")).toEqual({ aggregateID: "a", seq: 9 })
    expect(harness.receiver.lastDurable("b")).toEqual({ aggregateID: "b", seq: 5 })
    harness.close()
  })

  test("a new aggregate accepts any positive seq as its first position", async () => {
    const harness = setup()
    harness.port1.postMessage(encodeFrame(durableFrameOf("a", "a", 9)))
    harness.port1.postMessage(encodeFrame(durableFrameOf("b", "b", 1)))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastDurable("b")).toEqual({ aggregateID: "b", seq: 1 })
    harness.close()
  })

  test("backpressure drops non-durable deltas, keeps durables, and emits a resync", async () => {
    const harness = setup({ pressure: () => true })
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("dropped") })))
    harness.port1.postMessage(encodeFrame(durableFrameOf("kept", "agg", 1)))
    await pollUntil(() => harness.receiver.dropped === 1 && harness.frames.length === 1)
    expect(harness.frames).toEqual([durableFrameOf("kept", "agg", 1)])
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

  test("a malformed JSON frame fails loud with a typed rejection and stops the receiver", async () => {
    const harness = setup()
    harness.port1.postMessage("{not json")
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(ProtocolError)
    expect(harness.receiver.lastError).toBeDefined()
    expect(harness.receiver.pendingBytes).toBe(0)
    harness.close()
  })

  test("a non-string message fails loud instead of being coerced", async () => {
    const harness = setup()
    harness.port1.postMessage(new Uint8Array([1, 2, 3]))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(ProtocolError)
    harness.close()
  })

  test("an over-bound frame is rejected before it is parsed", async () => {
    const harness = setup()
    harness.port1.postMessage("x".repeat(MAX_FRAME_BYTES + 1))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.receiver.lastError?.name).toBe("FrameTooLargeError")
    harness.close()
  })
})

describe("egress receiver pressure-resync contract", () => {
  test("every dropped delta yields exactly one resync and durables are never dropped", async () => {
    let underPressure = true
    const harness = setup({ pressure: () => underPressure })

    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("d1") })))
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("d2") })))
    harness.port1.postMessage(encodeFrame(durableFrameOf("keep", "agg", 1)))
    await pollUntil(() => harness.receiver.dropped === 2 && harness.frames.length === 1)

    // Under sustained pressure each dropped delta surfaces exactly one overflow
    // resync, while the durable is delivered and never counted as dropped.
    expect(harness.receiver.dropped).toBe(2)
    expect(harness.receiver.resyncs).toBe(2)
    expect(harness.resyncs).toEqual([undefined, undefined])
    expect(harness.frames).toEqual([durableFrameOf("keep", "agg", 1)])

    // Once pressure clears, deltas flow again with no further drop or resync.
    underPressure = false
    harness.port1.postMessage(encodeFrame(eventFrame({ event: deltaEvent("d3") })))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.receiver.dropped).toBe(2)
    expect(harness.receiver.resyncs).toBe(2)
    harness.close()
  })
})

describe("egress receiver ordering boundaries", () => {
  test("a batch of JSON messages is processed in port order", async () => {
    const channel = new MessageChannel()
    const frames: EventsFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const first = encodeFrame(deltasFrame("v1"))
    const second = encodeFrame(durableFrame("d2", 2))
    channel.port1.postMessage(first)
    channel.port1.postMessage(second)
    await pollUntil(() => frames.length === 2)
    expect(receiver.lastError).toBeUndefined()
    expect(frames).toEqual([deltasFrame("v1"), durableFrame("d2", 2)])
    receiver.close()
    channel.port1.close()
  })

  test("does not commit a later durable when an earlier message fails", async () => {
    const channel = new MessageChannel()
    const frames: EventsFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const stale = encodeFrame(durableFrame("d1", 1))
    channel.port1.postMessage("{broken")
    channel.port1.postMessage(stale)
    await pollUntil(() => receiver.lastError !== undefined)
    expect(frames).toEqual([])
    receiver.close()
    channel.port1.close()
  })
})
