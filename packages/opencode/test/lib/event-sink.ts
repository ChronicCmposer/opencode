// Wave 1 — a fake hub event subscriber for parity tests (spec Q1/Q12).
//
// `createEventSink` subscribes to the REAL `EventV2.Service` (or `GlobalBus`) and
// records every emission as an ordered `{ id, type, properties }`, so a test can
// compare what a session publishes without a browser or SSE endpoint in the loop.
//
// Determinism note: the EventV2 subscription is registered synchronously via
// `EventV2.Service.listen`, so events published immediately after the sink is
// created are never missed. `EventV2Bridge` forwards each event to `GlobalBus`
// with `payload:{ id, type, properties }`, which the `"global-bus"` source reads
// verbatim (a worker/hub contract the parity harness compares against).

import { Effect, Layer, ManagedRuntime } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { GlobalBus, type GlobalEvent } from "@/bus/global"

/** One captured emission, normalized to the browser/SSE event envelope shape. */
export interface SinkEvent {
  readonly id: string
  readonly type: string
  readonly properties: unknown
}

/** Where a sink listens: the canonical event service or the hub's global bus. */
export type EventSinkOrigin = "event-v2" | "global-bus"

/**
 * A runtime (already assembled) or a layer the sink assembles itself. Passing a
 * runtime is preferred when the caller must share one `EventV2` instance with the
 * code under test; a layer is convenient for a one-off sink.
 */
export type EventSinkSource =
  | ManagedRuntime.ManagedRuntime<EventV2.Service, never>
  | Layer.Layer<EventV2.Service, never, never>

export interface EventSinkOptions {
  /** Subscription origin. Defaults to `"event-v2"`. */
  readonly origin?: EventSinkOrigin
}

export interface EventSink extends AsyncDisposable {
  /** The runtime the sink observes; run work here when the sink built its own. */
  readonly runtime: ManagedRuntime.ManagedRuntime<EventV2.Service, never>
  /** A defensive copy of the captured events, in emission order. */
  events(): ReadonlyArray<SinkEvent>
  /** The captured event types, in emission order. */
  types(): ReadonlyArray<string>
  /** Drop all captured events (leaving the subscription active). */
  clear(): void
  /** Resolves once the subscription is active (the sink is always ready on return). */
  readonly ready: Promise<void>
  /** Unsubscribe and, when the sink assembled the runtime, dispose it. */
  dispose(): Promise<void>
}

/** A runtime exposes `runFork`/`runSync`; a layer does not. */
function isRuntime(source: EventSinkSource): source is ManagedRuntime.ManagedRuntime<EventV2.Service, never> {
  return typeof (source as { readonly runFork?: unknown }).runFork === "function"
}

/** Register a listener on the event service, synchronously, and return its removal effect. */
function subscribeEventV2(
  runtime: ManagedRuntime.ManagedRuntime<EventV2.Service, never>,
  push: (event: SinkEvent) => void,
): EventV2.Unsubscribe {
  return runtime.runSync(
    EventV2.Service.use((events) =>
      events.listen((event) =>
        Effect.sync(() =>
          push({ id: event.id, type: event.type, properties: event.data }),
        ),
      ),
    ),
  )
}

/** Register a `GlobalBus` listener and return its removal effect. */
function subscribeGlobalBus(push: (event: SinkEvent) => void): Effect.Effect<void> {
  const handler = (event: GlobalEvent) => {
    const payload = event.payload as { readonly id?: unknown; readonly type?: unknown; readonly properties?: unknown } | undefined
    if (payload === undefined || payload === null || typeof payload !== "object") return
    if (typeof payload.id !== "string" || typeof payload.type !== "string") return
    push({ id: payload.id, type: payload.type, properties: payload.properties })
  }
  GlobalBus.on("event", handler)
  return Effect.sync(() => {
    GlobalBus.off("event", handler)
  })
}

/**
 * Subscribe to a real event source and capture its emissions. The subscription is
 * active before this returns (the returned `ready` promise is already resolved),
 * so no readiness handshake is needed.
 */
export function createEventSink(source: EventSinkSource, options: EventSinkOptions = {}): EventSink {
  const ownsRuntime = !isRuntime(source)
  const runtime = ownsRuntime ? ManagedRuntime.make(source as Layer.Layer<EventV2.Service, never, never>) : source
  const records: SinkEvent[] = []
  const push = (event: SinkEvent): void => {
    records.push(event)
  }

  const unsubscribe =
    (options.origin ?? "event-v2") === "global-bus" ? subscribeGlobalBus(push) : subscribeEventV2(runtime, push)

  let disposed = false

  const dispose = async (): Promise<void> => {
    if (disposed) return
    disposed = true
    await runtime.runPromise(unsubscribe).catch(() => undefined)
    if (ownsRuntime) await runtime.dispose().catch(() => undefined)
  }

  return {
    runtime,
    events: () => records.slice(),
    types: () => records.map((event) => event.type),
    clear: () => {
      records.length = 0
    },
    ready: Promise.resolve(),
    dispose,
    [Symbol.asyncDispose]: dispose,
  }
}
