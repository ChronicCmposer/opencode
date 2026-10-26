// In-process listener port pair (extracted from the duplicated `createPortPair`
// in the cancel/dispatch tests).
//
// A "pair" is two connected endpoints that cross-deliver frames synchronously.
// Posting a frame on one endpoint delivers it, in order, to every subscriber of
// the *other* endpoint. This is the cheapest way to exercise code that talks
// over a `MessagePort` without spawning a worker or a `MessageChannel`.
//
// Design (Law of Atomic Predictability): delivery is synchronous and pure — a
// `post` never mutates the poster, and unsubscribing mid-delivery does not skip
// a peer subscriber because each delivery snapshots its subscriber set.

import type { CancelFrame, CancelPort } from "@/session/worker/cancel"

/** One side of a connected pair. */
export interface ListenerPort<Frame> {
  /** Which side this endpoint is; useful in diagnostics. */
  readonly side: "hub" | "worker"
  /** Send a frame out; it is delivered to the peer's subscribers. */
  post(frame: Frame): void
  /** Subscribe to inbound frames; returns an unsubscribe function. */
  onMessage(handler: (frame: Frame) => void): () => void
  /** Every inbound frame received so far, in delivery order. */
  readonly received: ReadonlyArray<Frame>
  /** True once the whole pair was closed. */
  readonly closed: boolean
  /** Detach this endpoint's subscribers. The peer is unaffected. */
  close(): void
}

/** A connected hub/worker endpoint pair. */
export interface ListenerPair<Frame> {
  readonly hub: ListenerPort<Frame>
  readonly worker: ListenerPort<Frame>
  /** Close both endpoints at once. */
  close(): void
}

type FrameHandler<Frame> = (frame: Frame) => void

/** Optional per-endpoint wire behaviour; each hook receives the endpoint it belongs to. */
export interface EndpointHooks<Frame> {
  readonly post?: (self: ListenerPort<Frame>, frame: Frame) => void
  readonly onMessage?: (self: ListenerPort<Frame>, handler: FrameHandler<Frame>) => () => void
}

interface EndpointBundle<Frame> {
  readonly endpoint: ListenerPort<Frame>
  readonly deliver: FrameHandler<Frame>
}

/**
 * Build one endpoint from its wire hooks up front, so the returned object is
 * never mutated after construction. The default `post` cross-delivers to the
 * peer; `peerDeliver` is a lazy accessor because the peer does not exist yet.
 */
function createEndpoint<Frame>(
  side: "hub" | "worker",
  hooks: EndpointHooks<Frame>,
  peerDeliver: () => FrameHandler<Frame>,
): EndpointBundle<Frame> {
  const handlers = new Set<FrameHandler<Frame>>()
  const received: Frame[] = []
  let closed = false

  const deliver: FrameHandler<Frame> = (frame) => {
    if (closed) return
    received.push(frame)
    // Snapshot so a handler that unsubscribes (or subscribes) does not perturb
    // this delivery.
    for (const handler of [...handlers]) handler(frame)
  }

  const subscribe = (handler: FrameHandler<Frame>): (() => void) => {
    handlers.add(handler)
    return () => {
      handlers.delete(handler)
    }
  }

  const endpoint: ListenerPort<Frame> = {
    side,
    post: (frame) => (hooks.post ? hooks.post(endpoint, frame) : peerDeliver()(frame)),
    onMessage: (handler) => (hooks.onMessage ? hooks.onMessage(endpoint, handler) : subscribe(handler)),
    get received() {
      return received.slice()
    },
    get closed() {
      return closed
    },
    close: () => {
      closed = true
      handlers.clear()
    },
  }

  // The endpoint is fully formed here and never reassigned; freeze it so the
  // wire behaviour cannot be swapped out after construction.
  return { endpoint: Object.freeze(endpoint), deliver }
}

/**
 * Create two connected in-memory listener ports.
 *
 * `post` and `onMessage` optionally define each endpoint's wire behaviour:
 * - `post(port, frame)` sends `frame` out of `port` (default: cross-deliver to the peer);
 * - `onMessage(port, handler)` attaches `handler` at `port` (default: register on it).
 *
 * When a hook is omitted the pair keeps its built-in in-memory behaviour for that
 * operation; the two options are independent.
 */
export function createListenerPair<Frame>(
  post?: (port: ListenerPort<Frame>, frame: Frame) => void,
  onMessage?: (port: ListenerPort<Frame>, handler: FrameHandler<Frame>) => () => void,
): ListenerPair<Frame> {
  // Deliver closures are bound lazily: each endpoint is constructed before its
  // peer's `deliver` exists, so the peer accessors read these bindings.
  let deliverToHub: FrameHandler<Frame> = () => {}
  let deliverToWorker: FrameHandler<Frame> = () => {}
  const hooks: EndpointHooks<Frame> = {
    ...(post === undefined ? {} : { post }),
    ...(onMessage === undefined ? {} : { onMessage }),
  }

  const hub = createEndpoint<Frame>("hub", hooks, () => deliverToWorker)
  const worker = createEndpoint<Frame>("worker", hooks, () => deliverToHub)
  deliverToHub = hub.deliver
  deliverToWorker = worker.deliver

  return {
    hub: hub.endpoint,
    worker: worker.endpoint,
    close: () => {
      hub.endpoint.close()
      worker.endpoint.close()
    },
  }
}

/** The default endpoint type, re-exported for callers that name it explicitly. */
export type { FrameHandler }

/** A cancellation pair: a hub-side {@link CancelPort} wired to the worker endpoint. */
export interface CancelPair {
  readonly hub: CancelPort
  readonly worker: ListenerPort<CancelFrame>
  /** Close both endpoints. */
  close(): void
}

/**
 * Build a typed cancel pair over {@link createListenerPair}: `hub.notify` posts a
 * `CancelFrame` that the worker endpoint receives, and `hub.onMessage` observes
 * frames (e.g. `cancel-ack`) posted back by the worker.
 */
export function createCancelPair(): CancelPair {
  const pair = createListenerPair<CancelFrame>()
  return {
    hub: {
      notify: (frame) => pair.hub.post(frame),
      onMessage: (handler) => pair.hub.onMessage(handler),
    },
    worker: pair.worker,
    close: () => pair.close(),
  }
}
