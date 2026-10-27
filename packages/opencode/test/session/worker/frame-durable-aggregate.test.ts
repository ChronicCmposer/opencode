// Step 32 — durable frames must carry complete aggregate identity.
//
// The durable wire identity is `(aggregateID, seq)`, independent of the legacy
// sender-global `(generation, ordinal)` pair kept for mesh/control compatibility.
// A frame that specifies only half of the pair is malformed, not "almost durable".

import { describe, expect, test } from "bun:test"
import { ProtocolError, WORKER_FRAME_VERSION, durableAggregateMetaOf, parseEventsFrame } from "@/session/worker/frame"

const event = (extra: Record<string, unknown>) => ({
  v: WORKER_FRAME_VERSION,
  port: "events" as const,
  kind: "event" as const,
  payload: {
    event: { id: "evt_1", type: "test", properties: {} },
    ...extra,
  },
})

describe("durable aggregate identity", () => {
  test("round-trips aggregateID and seq through the events parser", () => {
    const frame = parseEventsFrame(event({ aggregateID: "ses_1", seq: 7 }))
    expect(frame.kind).toBe("event")
    expect(durableAggregateMetaOf(frame)).toEqual({ aggregateID: "ses_1", seq: 7 })
  })

  test("a half-specified aggregate pair is a ProtocolError", () => {
    expect(() => parseEventsFrame(event({ aggregateID: "ses_1" }))).toThrow(ProtocolError)
    expect(() => parseEventsFrame(event({ seq: 1 }))).toThrow(ProtocolError)
  })

  test("durableAggregateMetaOf is undefined for deltas and legacy metadata stays intact", () => {
    const delta = parseEventsFrame(event({}))
    expect(durableAggregateMetaOf(delta)).toBeUndefined()
    const legacy = parseEventsFrame(event({ generation: 2, ordinal: 9 }))
    expect(durableAggregateMetaOf(legacy)).toBeUndefined()
  })
})
