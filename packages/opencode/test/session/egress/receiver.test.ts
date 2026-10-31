// Hub-side egress receiver over the shared JSON events transport (Q6/Q16/Q21).
//
// Every port message is one shared-envelope JSON string. Event frames are
// delivered to `onFrame` in arrival order; durable `(aggregateID, seq)` pairs
// are checked for monotonicity and fail loud if they regress. The
// `end`/`resync`/`ack` control frames are surfaced distinctly, and backpressure
// drops non-durable deltas (never durables) while emitting a resync.

import { describe, expect, test } from "bun:test"
import { MAX_FRAME_BYTES } from "@/session/egress/frame"
import { encodeEventsFrameBytes as wire } from "@/session/egress/frame-bytes"
import type { ScannedEventsFrame } from "@/session/egress/scan"
import { LazyJson } from "@/util/lazy-json"
import { createEgressReceiver, DurableOrderError } from "@/session/egress/receiver"
import { ProtocolError, type EventsFrame, ackFrame, endFrame, eventFrame, resyncFrame } from "@/session/worker/frame"
import { deltasFrame, durableFrame } from "../../lib/egress-fixtures"
import { pollUntil } from "../../lib/poll"

const deltaEvent = (id: string) => ({ id, type: "message.part.delta", properties: { id } })
const durableEvent = (id: string) => ({ id, type: "message.updated", properties: { id } })

const idsOf = (frames: readonly ScannedEventsFrame[]) =>
  frames.map((frame) => (frame.kind === "event" ? frame.event.id : frame.kind))

/** A durable frame carrying its aggregate identity. */
const durableFrameOf = (id: string, aggregateID: string, seq: number) =>
  eventFrame({ event: durableEvent(id), aggregateID, seq } as never)

interface ReceiverHarness {
  readonly port1: MessagePort
  readonly frames: ScannedEventsFrame[]
  readonly ends: number
  readonly resyncs: (EventsFrame & { readonly kind: "resync" })["payload"][]
  readonly acks: (EventsFrame & { readonly kind: "ack" })["payload"][]
  readonly errors: Error[]
  readonly receiver: ReturnType<typeof createEgressReceiver>
  close(): void
}

function setup(options: { pressure?: () => boolean } = {}): ReceiverHarness {
  const channel = new MessageChannel()
  const frames: ScannedEventsFrame[] = []
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
  test("delivers event frames with properties left as raw LazyJson", async () => {
    const harness = setup()
    harness.port1.postMessage(wire(eventFrame({ event: deltaEvent("a") })))
    await pollUntil(() => harness.frames.length === 1)
    const delivered = harness.frames[0]
    if (delivered?.kind !== "event") throw new Error("expected an event frame")
    expect(delivered.event).toMatchObject({ id: "a", type: "message.part.delta" })
    expect(delivered.event.properties).toBeInstanceOf(LazyJson)
    expect(delivered.event.properties.value).toEqual({ id: "a" })
    expect(harness.receiver.received).toBe(1)
    expect(harness.receiver.pendingBytes).toBe(0)
    harness.close()
  })

  test("surfaces end, resync, and ack distinctly (never swallows them)", async () => {
    const harness = setup()
    harness.port1.postMessage(wire(resyncFrame({ pattern: "message.*" })))
    harness.port1.postMessage(wire(ackFrame({ count: 3 })))
    harness.port1.postMessage(wire(endFrame()))
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
    harness.port1.postMessage(wire(durableFrameOf("d2", "a", 2)))
    harness.port1.postMessage(wire(durableFrameOf("d1", "a", 1)))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/out-of-order durable/)
    expect(harness.receiver.lastDurable("a")).toEqual({ aggregateID: "a", seq: 2 })
    harness.close()
  })

  test("detects a seq gap within one aggregate and fails loud", async () => {
    const harness = setup()
    harness.port1.postMessage(wire(durableFrameOf("a", "agg", 1)))
    harness.port1.postMessage(wire(durableFrameOf("c", "agg", 3)))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(DurableOrderError)
    expect(harness.errors[0]?.message).toMatch(/gap durable/)
    // The gap is not accepted as the new last position.
    expect(harness.receiver.lastDurable("agg")).toEqual({ aggregateID: "agg", seq: 1 })
    harness.close()
  })

  test("each aggregate has its own cursor, so interleaving is never a gap", async () => {
    const harness = setup()
    harness.port1.postMessage(wire(durableFrameOf("a", "a", 9)))
    harness.port1.postMessage(wire(durableFrameOf("b", "b", 5)))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastDurable("a")).toEqual({ aggregateID: "a", seq: 9 })
    expect(harness.receiver.lastDurable("b")).toEqual({ aggregateID: "b", seq: 5 })
    harness.close()
  })

  test("a new aggregate accepts any non-negative seq as its first position", async () => {
    const harness = setup()
    harness.port1.postMessage(wire(durableFrameOf("a", "a", 9)))
    harness.port1.postMessage(wire(durableFrameOf("b", "b", 1)))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.errors).toEqual([])
    expect(harness.receiver.lastDurable("b")).toEqual({ aggregateID: "b", seq: 1 })
    harness.close()
  })

  test("duplicate/stale durables are dropped without a resync or a cursor rewind", async () => {
    const channel = new MessageChannel()
    const frames: ScannedEventsFrame[] = []
    const gaps: Array<[unknown, unknown]> = []
    const receiver = createEgressReceiver({
      port: channel.port2,
      onFrame: (frame) => frames.push(frame),
      onDurableGap: (got, last) => gaps.push([got, last]),
    })
    try {
      channel.port1.postMessage(wire(durableFrameOf("b", "agg", 2)))
      // Stale/duplicate frames arrive after the cursor advanced to 2.
      channel.port1.postMessage(wire(durableFrameOf("a", "agg", 1)))
      channel.port1.postMessage(wire(durableFrameOf("b2", "agg", 2)))
      // A later contiguous frame proves the stale ones did not rewind the cursor.
      channel.port1.postMessage(wire(durableFrameOf("c", "agg", 3)))
      await pollUntil(() => receiver.received === 2)

      expect(idsOf(frames)).toEqual(["b", "c"])
      expect(gaps).toEqual([])
      expect(receiver.resyncs).toBe(0)
      expect(receiver.lastDurable("agg")).toEqual({ aggregateID: "agg", seq: 3 })
      expect(receiver.lastError).toBeUndefined()
    } finally {
      receiver.close()
      channel.port1.close()
    }
  })

  test("backpressure drops non-durable deltas, keeps durables, and emits a resync", async () => {
    const harness = setup({ pressure: () => true })
    harness.port1.postMessage(wire(eventFrame({ event: deltaEvent("dropped") })))
    harness.port1.postMessage(wire(durableFrameOf("kept", "agg", 1)))
    await pollUntil(() => harness.receiver.dropped === 1 && harness.frames.length === 1)
    expect(idsOf(harness.frames)).toEqual(["kept"])
    expect(harness.receiver.dropped).toBe(1)
    expect(harness.resyncs).toEqual([undefined])
    harness.close()
  })

  test("without pressure no delta is dropped", async () => {
    const harness = setup()
    harness.port1.postMessage(wire(eventFrame({ event: deltaEvent("a") })))
    await pollUntil(() => harness.frames.length === 1)
    expect(harness.receiver.dropped).toBe(0)
    expect(harness.resyncs).toEqual([])
    harness.close()
  })

  test("a malformed byte frame fails loud with a typed rejection and stops the receiver", async () => {
    const harness = setup()
    harness.port1.postMessage(new Uint8Array([9, 0, 0, 0, 1]).buffer)
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(ProtocolError)
    expect(harness.receiver.lastError).toBeDefined()
    expect(harness.receiver.pendingBytes).toBe(0)
    harness.close()
  })

  test("a non-ArrayBuffer message (including a string) fails loud instead of being coerced", async () => {
    const harness = setup()
    harness.port1.postMessage(new Uint8Array([1, 2, 3]))
    harness.port1.postMessage("{}")
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.errors[0]).toBeInstanceOf(ProtocolError)
    harness.close()
  })

  test("an over-bound frame is rejected before it is parsed", async () => {
    const harness = setup()
    harness.port1.postMessage(new ArrayBuffer(MAX_FRAME_BYTES + 1))
    await pollUntil(() => harness.errors.length === 1)
    expect(harness.receiver.lastError?.name).toBe("FrameTooLargeError")
    harness.close()
  })
})

describe("egress receiver pressure-resync contract", () => {
  test("every dropped delta yields exactly one resync and durables are never dropped", async () => {
    let underPressure = true
    const harness = setup({ pressure: () => underPressure })

    harness.port1.postMessage(wire(eventFrame({ event: deltaEvent("d1") })))
    harness.port1.postMessage(wire(eventFrame({ event: deltaEvent("d2") })))
    harness.port1.postMessage(wire(durableFrameOf("keep", "agg", 1)))
    await pollUntil(() => harness.receiver.dropped === 2 && harness.frames.length === 1)

    // Under sustained pressure each dropped delta surfaces exactly one overflow
    // resync, while the durable is delivered and never counted as dropped.
    expect(harness.receiver.dropped).toBe(2)
    expect(harness.receiver.resyncs).toBe(2)
    expect(harness.resyncs).toEqual([undefined, undefined])
    expect(idsOf(harness.frames)).toEqual(["keep"])

    // Once pressure clears, deltas flow again with no further drop or resync.
    underPressure = false
    harness.port1.postMessage(wire(eventFrame({ event: deltaEvent("d3") })))
    await pollUntil(() => harness.frames.length === 2)
    expect(harness.receiver.dropped).toBe(2)
    expect(harness.receiver.resyncs).toBe(2)
    harness.close()
  })
})

describe("egress receiver ordering boundaries", () => {
  test("a batch of byte frames is processed in port order", async () => {
    const channel = new MessageChannel()
    const frames: ScannedEventsFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const first = wire(deltasFrame("v1"))
    const second = wire(durableFrame("d2", 2))
    channel.port1.postMessage(first)
    channel.port1.postMessage(second)
    await pollUntil(() => frames.length === 2)
    expect(receiver.lastError).toBeUndefined()
    expect(idsOf(frames)).toEqual(["v1", "d2"])
    receiver.close()
    channel.port1.close()
  })

  test("does not commit a later durable when an earlier message fails", async () => {
    const channel = new MessageChannel()
    const frames: ScannedEventsFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const stale = wire(durableFrame("d1", 1))
    channel.port1.postMessage(new Uint8Array([9, 0, 0, 0, 1]).buffer)
    channel.port1.postMessage(stale)
    await pollUntil(() => receiver.lastError !== undefined)
    expect(frames).toEqual([])
    receiver.close()
    channel.port1.close()
  })
})
