// Generic worker-side EventV2 → events-port bridge (spec Q6/Q13/Q15).
//
// The worker runs a REAL core `EventV2` (behind `EventV2Bridge`), but publishes
// onto its own in-isolate pubsub — nothing crosses the isolate. This module turns
// those publishes into the verbatim `{ id, type, properties }` event and hands it
// to a sink (the egress channel), so the hub can faithfully republish what the
// worker's session actually produced. The egress transport owns framing: it wraps
// the event in an `eventFrame` (`session/worker/frame.ts`) and posts it.
//
// Three filters keep the wire focused (Q13/Q15):
//
//   * only SESSION-SCOPED events are forwarded — those whose payload carries a
//     `sessionID`, i.e. what the UI renders. Worker-local telemetry (thread ids,
//     metrics, …) never crosses;
//   * the hub-proxied `session.status` is dropped, because the hub already owns
//     that authority and must not receive an echo;
//   * a durable-wakeup republish is dropped, because the hub's mesh delivers every
//     matching durable commit back to the subscribing worker — including the
//     worker's own (pattern `"*"`) — and that payload already crossed to the hub
//     when it was first published. Re-forwarding it would make the hub publish the
//     same event twice.
//
// The forwarded event keeps the EventV2 `{ id, type, properties }` shape exactly
// (`properties` is the EventV2 `data` reference, never a writer-style `data`
// translation). Subscription goes through `EventV2.Interface.listen`, which
// registers synchronously, so a publish immediately after bridge creation is
// never missed. `dispose` unsubscribes and is idempotent.

import { Effect } from "effect"
import type { EventV2 } from "@opencode-ai/core/event"

import type { EventV2Event } from "./frame"

/** The events-port sink: hand one verbatim worker event to the transport. */
export interface WorkerEventSink {
  readonly emit: (event: EventV2Event) => void
  /**
   * Durable events bypass coalescing and carry their committed aggregate
   * `(aggregateID, seq)`. A sink without this seam cannot preserve a durable
   * event's identity, so forwarding one to it fails loud rather than downgrading
   * the event to a plain delta.
   */
  readonly emitDurable?: (event: EventV2Event, meta: { readonly aggregateID: string; readonly seq: number }) => void
}

/** A live worker-event subscription. */
export interface WorkerEventBridge {
  /** Stop forwarding further events; idempotent. */
  readonly dispose: Effect.Effect<void>
}

/** Event types the hub already proxies; the worker must never echo them back. */
export const HUB_PROXIED_EVENT_TYPES: ReadonlySet<string> = new Set(["session.status"])

/**
 * Metadata key marking a payload the durable-wakeup path republished into this
 * isolate (see `republishDurableCommit`). The hub's mesh delivers every matching
 * durable commit back to the subscribing worker — including the worker's own
 * commit (pattern `"*"`) — so the republished payload must not be re-forwarded: it
 * already crossed to the hub when it was first published, and re-forwarding it
 * would make the hub publish the same event twice.
 */
export const DURABLE_WAKEUP_METADATA_KEY = "opencode.worker.durable-wakeup"

/** Is this payload a durable-wakeup republish that must never be re-forwarded? */
export function isDurableWakeup(event: Pick<EventV2.Payload, "metadata">): boolean {
  return event.metadata?.[DURABLE_WAKEUP_METADATA_KEY] === true
}

/** The slice of `EventV2.Interface` the bridge depends on. */
export type EventSubscriber = Pick<EventV2.Interface, "listen">

/** Does the payload identify a session (the discriminator the UI renders on)? */
export function isSessionScopedEvent(event: Pick<EventV2.Payload, "data">): boolean {
  return hasSessionID(event.data)
}

/** Project a trusted EventV2 payload onto the verbatim `{ id, type, properties }` wire shape. */
export function toEventV2Event(event: EventV2.Payload): EventV2Event {
  return { id: event.id, type: event.type, properties: event.data }
}

/**
 * Subscribe to the worker's `EventV2` publishes and forward every session-scoped
 * event (minus hub-proxied types) to `sink` verbatim.
 */
export function createWorkerEventBridge(
  events: EventSubscriber,
  sink: WorkerEventSink,
): Effect.Effect<WorkerEventBridge> {
  return Effect.gen(function* () {
    let disposed = false

    const forward = (event: EventV2.Payload): void => {
      if (disposed) return
      if (!isForwardable(event)) return
      const wire = toEventV2Event(event)
      if (event.durable !== undefined) {
        // A durable event's committed `(aggregateID, seq)` is part of its
        // identity. A sink that cannot carry it must not receive the event as a
        // plain delta: silently downgrading would lose the durable cursor.
        if (sink.emitDurable === undefined) {
          throw new Error(
            `durable worker event "${event.type}" requires a durable sink; this sink has no emitDurable`,
          )
        }
        sink.emitDurable(wire, { aggregateID: event.durable.aggregateID, seq: event.durable.seq })
        return
      }
      sink.emit(wire)
    }

    const unsubscribe = yield* events.listen((event) => Effect.sync(() => forward(event)))

    return {
      dispose: Effect.suspend(() => {
        if (disposed) return Effect.void
        disposed = true
        return unsubscribe
      }),
    }
  })
}

/** Forward session-scoped events only, never a hub-proxied type, never a wakeup republish. */
function isForwardable(event: EventV2.Payload): boolean {
  if (isDurableWakeup(event)) return false
  if (HUB_PROXIED_EVENT_TYPES.has(event.type)) return false
  return isSessionScopedEvent(event)
}

/** A payload identifies a session when its `data` carries a non-empty sessionID. */
function hasSessionID(data: unknown): boolean {
  if (data === null || typeof data !== "object") return false
  const sessionID = (data as { readonly sessionID?: unknown }).sessionID
  return typeof sessionID === "string" && sessionID.length > 0
}

export * as WorkerEventBridgeModule from "./event-bridge"
