// Phase 8.6 + 8.8 — pre-encode once, bounded fan-out (plan tasks 8.6, 8.8).
//
// The hub encodes each published frame exactly once and hands the SAME bytes to
// every subscriber (no per-client re-encode). Each subscriber has a bound of 256;
// on overflow the hub drops that subscriber's OLDEST non-durable frame, and when
// the queue is durable-saturated it surfaces `SubscriberOverflowError`. Publishing
// never blocks — it returns synchronously with the ids that overflowed.

import { describe, expect, test } from "bun:test"
import { SUBSCRIBER_BOUND, SubscriberOverflowError, createFanout } from "@/session/egress/fanout"
import { encodeFrame } from "@/session/egress/frame"
import { deltasFrame, durableFrame } from "../../lib/egress-fixtures"

describe("egress fan-out", () => {
  test("delivers each frame to every subscriber", () => {
    const fanout = createFanout()
    const a = fanout.subscribe("a")
    const b = fanout.subscribe("b")
    fanout.publish(deltasFrame("1"))
    expect(a.drain()).toEqual([deltasFrame("1")])
    expect(b.drain()).toEqual([deltasFrame("1")])
  })

  test("encodes a frame exactly once regardless of subscriber count", () => {
    const fanout = createFanout()
    const a = fanout.subscribe("a")
    const b = fanout.subscribe("b")
    const c = fanout.subscribe("c")
    fanout.publish(deltasFrame("1"))
    expect(fanout.encodeCount).toBe(1)
    // The very same byte buffer is queued for every subscriber.
    const [aBytes, bBytes, cBytes] = [a.drainBytes(), b.drainBytes(), c.drainBytes()]
    expect(aBytes[0]).toBe(bBytes[0])
    expect(bBytes[0]).toBe(cBytes[0])
  })

  test("publishBytes forwards already-encoded bytes without encoding", () => {
    const fanout = createFanout()
    const subscriber = fanout.subscribe("s")
    const frame = deltasFrame("1")
    const bytes = encodeFrame(frame)
    fanout.publishBytes(bytes, frame)
    expect(fanout.encodeCount).toBe(0)
    expect(subscriber.drainBytes()[0]).toBe(bytes)
    expect(subscriber.drain()).toEqual([])
  })

  test("subscribe memoizes the handle per subscriber id", () => {
    const fanout = createFanout()
    const first = fanout.subscribe("s")
    const second = fanout.subscribe("s")
    expect(second).toBe(first)
    expect(fanout.subscribers[0]).toBe(first)
  })

  test("default subscriber bound is 256", () => {
    expect(SUBSCRIBER_BOUND).toBe(256)
  })

  test("holds up to the bound without dropping", () => {
    const fanout = createFanout({ bound: 3 })
    const sub = fanout.subscribe("s")
    for (const id of ["1", "2", "3"]) fanout.publish(deltasFrame(id))
    expect(sub.depth).toBe(3)
  })

  test("overflow drops the OLDEST non-durable frame", () => {
    const fanout = createFanout({ bound: 3 })
    const sub = fanout.subscribe("s")
    for (const id of ["1", "2", "3", "4"]) fanout.publish(deltasFrame(id))
    expect(sub.depth).toBe(3)
    expect(sub.drain()).toEqual([deltasFrame("2"), deltasFrame("3"), deltasFrame("4")])
  })

  test("durable frames survive; only non-durable frames are dropped", () => {
    const fanout = createFanout({ bound: 3 })
    const sub = fanout.subscribe("s")
    fanout.publish(deltasFrame("1"))
    fanout.publish(durableFrame("d1", 1))
    fanout.publish(deltasFrame("2"))
    fanout.publish(deltasFrame("3"))
    expect(sub.drain()).toEqual([durableFrame("d1", 1), deltasFrame("2"), deltasFrame("3")])
  })

  test("a durable-saturated queue surfaces SubscriberOverflowError without blocking others", () => {
    const fanout = createFanout({ bound: 2 })
    const slow = fanout.subscribe("slow")
    const fast = fanout.subscribe("fast")
    fanout.publish(durableFrame("d1", 1))
    fanout.publish(durableFrame("d2", 2))
    fast.drain() // the fast client kept up and drained its durable queue

    const result = fanout.publish(deltasFrame("3"))
    expect(result.overflowed).toEqual(["slow"])
    expect(slow.overflowed).toBe(true)
    // The other subscriber still received the frame.
    expect(fast.drain()).toEqual([deltasFrame("3")])
  })

  test("publish never throws; it marks the subscriber overflowed", () => {
    const fanout = createFanout({ bound: 1 })
    const sub = fanout.subscribe("s")
    fanout.publish(durableFrame("d1", 1))
    expect(() => fanout.publish(durableFrame("d2", 2))).not.toThrow()
    expect(sub.overflowed).toBe(true)
  })

  test("delivered excludes overflowed subscribers", () => {
    const fanout = createFanout({ bound: 1 })
    fanout.subscribe("slow")
    const fast = fanout.subscribe("fast")
    fanout.publish(durableFrame("d1", 1))
    fast.drain()
    const result = fanout.publish(deltasFrame("2"))
    expect(result.overflowed).toEqual(["slow"])
    expect(result.delivered).toBe(1)
  })

  test("draining clears the overflow flag once the queue is no longer saturated", () => {
    const fanout = createFanout({ bound: 1 })
    const sub = fanout.subscribe("s")
    fanout.publish(durableFrame("d1", 1))
    fanout.publish(durableFrame("d2", 2))
    expect(sub.overflowed).toBe(true)
    sub.drain()
    expect(sub.overflowed).toBe(false)
  })

  test("clear resets the overflow flag", () => {
    const fanout = createFanout({ bound: 1 })
    const sub = fanout.subscribe("s")
    fanout.publish(durableFrame("d1", 1))
    fanout.publish(durableFrame("d2", 2))
    expect(sub.overflowed).toBe(true)
    sub.clear()
    expect(sub.overflowed).toBe(false)
  })

  test("clear empties a subscriber queue", () => {
    const fanout = createFanout({ bound: 4 })
    const sub = fanout.subscribe("s")
    fanout.publish(deltasFrame("1"))
    sub.clear()
    expect(sub.depth).toBe(0)
    expect(sub.drain()).toEqual([])
  })

  test("unsubscribe stops delivery", () => {
    const fanout = createFanout()
    const sub = fanout.subscribe("s")
    fanout.unsubscribe("s")
    fanout.publish(deltasFrame("1"))
    expect(sub.depth).toBe(0)
    expect(fanout.subscribers).toEqual([])
  })

  test("SubscriberOverflowError names the subscriber", () => {
    const error = new SubscriberOverflowError("client-7")
    expect(error.subscriberId).toBe("client-7")
    expect(error.message).toContain("client-7")
  })
})
