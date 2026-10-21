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

function createEndpoint<Frame>(side: "hub" | "worker"): {
  endpoint: ListenerPort<Frame>
  deliver(frame: Frame): void
} {
  const handlers = new Set<FrameHandler<Frame>>()
  const received: Frame[] = []
  let closed = false

  const deliver = (frame: Frame): void => {
    if (closed) return
    received.push(frame)
    // Snapshot so a handler that unsubscribes (or subscribes) does not perturb
    // this delivery.
    for (const handler of [...handlers]) handler(frame)
  }

  const endpoint: ListenerPort<Frame> = {
    side,
    post: () => {
      throw new Error(`listener port "${side}" is not connected`)
    },
    onMessage: (handler) => {
      handlers.add(handler)
      return () => handlers.delete(handler)
    },
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

  return { endpoint, deliver }
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
  const hub = createEndpoint<Frame>("hub")
  const worker = createEndpoint<Frame>("worker")

  // `post` always needs a destination, so it is always overridden; `onMessage`
  // is only overridden when a hook is supplied, otherwise the built-in
  // subscriber set is used (overriding it with a self-call would recurse).
  hub.endpoint.post = post ? (frame) => post(hub.endpoint, frame) : (frame) => worker.deliver(frame)
  worker.endpoint.post = post ? (frame) => post(worker.endpoint, frame) : (frame) => hub.deliver(frame)
  if (onMessage) {
    hub.endpoint.onMessage = (handler) => onMessage(hub.endpoint, handler)
    worker.endpoint.onMessage = (handler) => onMessage(worker.endpoint, handler)
  }

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
