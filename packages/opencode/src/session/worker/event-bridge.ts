// Generic worker-side EventV2 → events-port bridge (spec Q6/Q13/Q15).
//
// The worker runs a REAL core `EventV2` (behind `EventV2Bridge`), but publishes
// onto its own in-isolate pubsub — nothing crosses the isolate. This module turns
// those publishes into the verbatim `{ id, type, properties }` event and hands it
// to a sink (the egress channel), so the hub can faithfully republish what the
// worker's session actually produced. The egress transport owns framing: it wraps
// the event in an `eventFrame` (`session/worker/frame.ts`) and posts it.
//
// Two filters keep the wire focused (Q13/Q15):
//
//   * only SESSION-SCOPED events are forwarded — those whose payload carries a
//     `sessionID`, i.e. what the UI renders. Worker-local telemetry (thread ids,
//     metrics, …) never crosses;
//   * the hub-proxied `session.status` is dropped, because the hub already owns
//     that authority and must not receive an echo.
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
  /** Durable events bypass coalescing; omitted sinks fall back to `emit`. */
  readonly emitDurable?: (event: EventV2Event) => void
}

/** A live worker-event subscription. */
export interface WorkerEventBridge {
  /** Stop forwarding further events; idempotent. */
  readonly dispose: Effect.Effect<void>
}

/** Event types the hub already proxies; the worker must never echo them back. */
export const HUB_PROXIED_EVENT_TYPES: ReadonlySet<string> = new Set(["session.status"])

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
      if (event.durable !== undefined && sink.emitDurable !== undefined) sink.emitDurable(wire)
      else sink.emit(wire)
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

/** Forward session-scoped events only, and never a hub-proxied type. */
function isForwardable(event: EventV2.Payload): boolean {
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
