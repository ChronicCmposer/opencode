// Phase 8.6 + 8.8 — pre-encode once, bounded fan-out (plan tasks 8.6, 8.8).
//
// The hub encodes each published frame exactly once and hands the SAME bytes to
// every subscriber (no per-client re-encode). Each subscriber has a bound of 256;
// on overflow the hub drops that subscriber's OLDEST non-durable frame, and when
// the queue is durable-saturated it surfaces `SubscriberOverflowError`. Publishing
// never blocks — it returns synchronously with the ids that overflowed.

import { describe, expect, test } from "bun:test"
import {
  SUBSCRIBER_BOUND,
  SubscriberOverflowError,
  createFanout,
} from "@/session/egress/fanout"
import type { EgressEvent, EgressFrame } from "@/session/egress/frame"
import { encodeFrame } from "@/session/egress/frame"

const delta = (id: string): EgressEvent => ({ id, type: "message.part.updated", data: { id } })
const deltas = (...ids: string[]): EgressFrame => ({ kind: "deltas", v: 1, events: ids.map(delta) })
const durable = (id: string, ordinal: number): EgressFrame => ({
  kind: "durable",
  v: 1,
  meta: { generation: 1, ordinal },
  event: { id, type: "message.updated", data: { id } },
})

describe("egress fan-out", () => {
  test("delivers each frame to every subscriber", () => {
    const fanout = createFanout()
    const a = fanout.subscribe("a")
    const b = fanout.subscribe("b")
    fanout.publish(deltas("1"))
    expect(a.drain()).toEqual([deltas("1")])
    expect(b.drain()).toEqual([deltas("1")])
  })

  test("encodes a frame exactly once regardless of subscriber count", () => {
    const fanout = createFanout()
    const a = fanout.subscribe("a")
    const b = fanout.subscribe("b")
    const c = fanout.subscribe("c")
    fanout.publish(deltas("1"))
    expect(fanout.encodeCount).toBe(1)
    // The very same byte buffer is queued for every subscriber.
    expect(a.queued()[0]).toBe(b.queued()[0])
    expect(b.queued()[0]).toBe(c.queued()[0])
  })

  test("publishBytes forwards already-encoded bytes without encoding", () => {
    const fanout = createFanout()
    const subscriber = fanout.subscribe("s")
    const frame = deltas("1")
    const bytes = encodeFrame(frame)
    fanout.publishBytes(bytes, frame)
    expect(fanout.encodeCount).toBe(0)
    expect(subscriber.queued()[0]).toBe(bytes)
    expect(subscriber.drain()).toEqual([frame])
  })

  test("default subscriber bound is 256", () => {
    expect(SUBSCRIBER_BOUND).toBe(256)
  })

  test("holds up to the bound without dropping", () => {
    const fanout = createFanout({ bound: 3 })
    const sub = fanout.subscribe("s")
    for (const id of ["1", "2", "3"]) fanout.publish(deltas(id))
    expect(sub.depth).toBe(3)
  })

  test("overflow drops the OLDEST non-durable frame", () => {
    const fanout = createFanout({ bound: 3 })
    const sub = fanout.subscribe("s")
    for (const id of ["1", "2", "3", "4"]) fanout.publish(deltas(id))
    expect(sub.depth).toBe(3)
    expect(sub.drain()).toEqual([deltas("2"), deltas("3"), deltas("4")])
  })

  test("durable frames survive; only non-durable frames are dropped", () => {
    const fanout = createFanout({ bound: 3 })
    const sub = fanout.subscribe("s")
    fanout.publish(deltas("1"))
    fanout.publish(durable("d1", 1))
    fanout.publish(deltas("2"))
    fanout.publish(deltas("3"))
    expect(sub.drain()).toEqual([durable("d1", 1), deltas("2"), deltas("3")])
  })

  test("a durable-saturated queue surfaces SubscriberOverflowError without blocking others", () => {
    const fanout = createFanout({ bound: 2 })
    const slow = fanout.subscribe("slow")
    const fast = fanout.subscribe("fast")
    fanout.publish(durable("d1", 1))
    fanout.publish(durable("d2", 2))
    fast.drain() // the fast client kept up and drained its durable queue

    const result = fanout.publish(deltas("3"))
    expect(result.overflowed).toEqual(["slow"])
    expect(slow.overflowed).toBe(true)
    // The other subscriber still received the frame.
    expect(fast.drain()).toEqual([deltas("3")])
  })

  test("publish never throws; it marks the subscriber overflowed", () => {
    const fanout = createFanout({ bound: 1 })
    const sub = fanout.subscribe("s")
    fanout.publish(durable("d1", 1))
    expect(() => fanout.publish(durable("d2", 2))).not.toThrow()
    expect(sub.overflowed).toBe(true)
  })

  test("clear empties a subscriber queue", () => {
    const fanout = createFanout({ bound: 4 })
    const sub = fanout.subscribe("s")
    fanout.publish(deltas("1"))
    sub.clear()
    expect(sub.depth).toBe(0)
    expect(sub.drain()).toEqual([])
  })

  test("unsubscribe stops delivery", () => {
    const fanout = createFanout()
    const sub = fanout.subscribe("s")
    fanout.unsubscribe("s")
    fanout.publish(deltas("1"))
    expect(sub.depth).toBe(0)
    expect(fanout.subscribers).toEqual([])
  })

  test("SubscriberOverflowError names the subscriber", () => {
    const error = new SubscriberOverflowError("client-7")
    expect(error.subscriberId).toBe("client-7")
    expect(error.message).toContain("client-7")
  })
})
