// Shared egress test fixtures.
//
// The egress suites each rebuilt the same tiny event/frame shapes inline. These
// builders are the single source of truth so a wire-shape change is made once.
//
// The channel now carries the shared events envelope: `delta` and `durable` build
// {@link EventV2Event}s — non-durable and durable respectively — with the wire
// `type` overridable for pattern-matching suites. `deltasFrame` / `durableFrame`
// wrap them into the `event` frames the hub transports.

import { type EventV2Event, type EventsFrame, eventFrame } from "@/session/worker/frame"

/** Build a non-durable EventV2 event (default type: `message.part.updated`). */
export const delta = (id: string, type = "message.part.updated"): EventV2Event => ({ id, type, properties: { id } })

/** Build a durable EventV2 event (default type: `message.updated`). */
export const durable = (id: string, type = "message.updated"): EventV2Event => ({ id, type, properties: { id } })

/** Wrap one non-durable delta event into its `event` frame. */
export const deltasFrame = (id: string): EventsFrame => eventFrame({ event: delta(id) })

/** Build a durable `event` frame carrying aggregate `(aggregateID, seq)` identity. */
export const durableFrame = (id: string, seq: number, aggregateID = "agg"): EventsFrame =>
  eventFrame({ event: durable(id), aggregateID, seq } as Parameters<typeof eventFrame>[0])
