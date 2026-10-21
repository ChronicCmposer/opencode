// Shared egress test fixtures (plan finding #183).
//
// The egress suites each rebuilt the same tiny event/frame shapes inline. These
// builders are the single source of truth so a wire-shape change is made once.
//
// `delta` and `durable` build {@link EgressEvent}s — non-durable and durable
// respectively — with the wire `type` overridable for pattern-matching suites.
// `deltasFrame` / `durableFrame` wrap them into the frames the hub transports.

import type { EgressEvent, EgressFrame } from "@/session/egress/frame"

/** Build a non-durable egress event (default type: `message.part.updated`). */
export const delta = (id: string, type = "message.part.updated"): EgressEvent => ({ id, type, data: { id } })

/** Build a durable egress event (default type: `message.updated`). */
export const durable = (id: string, type = "message.updated"): EgressEvent => ({ id, type, data: { id } })

/** Coalesce one delta event per id into a single `deltas` frame. */
export const deltasFrame = (...ids: string[]): EgressFrame => ({ kind: "deltas", v: 1, events: ids.map((id) => delta(id)) })

/** Build a durable frame carrying commit metadata at generation 1. */
export const durableFrame = (id: string, ordinal: number): EgressFrame => ({
  kind: "durable",
  v: 1,
  meta: { generation: 1, ordinal },
  event: durable(id),
})
