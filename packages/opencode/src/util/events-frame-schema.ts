// The events-port body schema: one decode for the `event` / `end` / `resync` /
// `ack` variants.
//
// The frame codec owns the envelope (`v`, `port`, `id`); this module owns the
// family-specific `{ kind, payload }` body. Decoding with Effect Schema rebuilds
// plain objects, so an untrusted own `__proto__` key is dropped rather than
// carried into the decoded value. Callers decode ONCE at the boundary and trust
// the result afterwards.

import { Schema } from "effect"

/** The verbatim EventV2 event `{ id, type, properties }`. */
export const EventV2Schema = Schema.Struct({
  id: Schema.String,
  type: Schema.String,
  properties: Schema.Unknown,
})

/** Writer-stamped durable metadata plus the event itself. */
export const EventsEventPayloadSchema = Schema.Struct({
  event: EventV2Schema,
  generation: Schema.optional(Schema.Int),
  ordinal: Schema.optional(Schema.Int),
})

/** The subscription pattern that overflowed, when scoped. */
export const EventsResyncPayloadSchema = Schema.Struct({
  pattern: Schema.optional(Schema.String),
})

/** A non-negative acknowledgement count. */
export const EventsAckPayloadSchema = Schema.Struct({
  count: Schema.optional(Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)))),
})

/** The four events-port body variants, discriminated by `kind`. */
export const EventsFrameBodySchema = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("event"), payload: EventsEventPayloadSchema }),
  Schema.Struct({ kind: Schema.Literal("end"), payload: Schema.optional(Schema.Undefined) }),
  Schema.Struct({ kind: Schema.Literal("resync"), payload: Schema.optional(EventsResyncPayloadSchema) }),
  Schema.Struct({ kind: Schema.Literal("ack"), payload: Schema.optional(EventsAckPayloadSchema) }),
])

export type EventV2 = typeof EventV2Schema.Type
export type EventsEventPayload = typeof EventsEventPayloadSchema.Type
export type EventsResyncPayload = typeof EventsResyncPayloadSchema.Type
export type EventsAckPayload = typeof EventsAckPayloadSchema.Type
export type EventsFrameBody = typeof EventsFrameBodySchema.Type

/** Decode an untrusted events body, failing loud on any malformed variant. */
export function decodeEventsFrameBody(input: unknown): EventsFrameBody {
  return Schema.decodeUnknownSync(EventsFrameBodySchema)(input)
}

export * as EventsFrameSchema from "./events-frame-schema"
