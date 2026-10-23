// Phase 9/11 — shared worker port plumbing.
//
// The cancel control channel (`cancel.ts`), the job channel (`job-client.ts`),
// the writer relay (`writer-client.ts`) and the broker relay (`broker-client.ts`)
// all adapt a real `MessagePort` to a framed request/response channel, and all
// need the same unref'd timer, error-message coercion and peer lifecycle lines.
// This module is the one implementation they share, so the port glue is not
// duplicated per protocol.
//
// `createRequestChannel` is deliberately dependency-free: it depends only on the
// `FrameChannel` shape and the `PortScheduler` seam declared here, so importing
// it never drags in a protocol or creates an import cycle. The lifecycle helpers
// additionally use the structured logger, which is a leaf module.

import { workerLogMessage, type WorkerLogger } from "./log"

/** Timer seam shared by dispatch, cancel, and the job registry; unref'd. */
export type PortScheduler = (delayMs: number, run: () => void) => () => void

export const defaultSchedule: PortScheduler = (delayMs, run) => {
  const timer = setTimeout(run, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return () => clearTimeout(timer)
}

/** Coerce an unknown thrown value to a message, for error frames. */
export const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

/** The frame-agnostic channel shape a `MessagePort` is adapted to. */
export interface FrameChannel<Frame> {
  post(frame: Frame): void
  onMessage(handler: (frame: Frame) => void): () => void
}

/**
 * Adapt a real `MessagePort` to `FrameChannel`. Incoming messages are cast to
 * `Frame` at this boundary; `port.start()` is called so a fresh port receives.
 */
export function portFromMessagePort<Frame>(port: MessagePort): FrameChannel<Frame> {
  return {
    post: (frame) => port.postMessage(frame),
    onMessage: (handler) => {
      const listener = (event: MessageEvent) => handler(event.data as Frame)
      port.addEventListener("message", listener)
      port.start()
      return () => port.removeEventListener("message", listener)
    },
  }
}

// ---------------------------------------------------------------------------
// Request/response correlation
// ---------------------------------------------------------------------------

/** One in-flight request: how to settle it and how to cancel its timeout. */
interface PendingRequest<Reply> {
  readonly resolve: (reply: Reply) => void
  readonly reject: (error: unknown) => void
  /** Cancel the unref'd timeout armed for this request, when one was armed. */
  readonly cancelTimeout: (() => void) | undefined
}

export interface RequestChannelOptions<Frame, Reply extends Frame, Id> {
  /** Narrow an inbound frame to a correlated reply; non-replies are ignored. */
  readonly isReply: (frame: Frame) => frame is Reply
  /** Read the correlation id from a reply. */
  readonly idOf: (reply: Reply) => Id
  /** Mint the next correlation id for an outbound request. */
  readonly nextId: () => Id
  /** Default per-request timeout; `undefined` disables timeouts. */
  readonly timeoutMs?: number
  /** Build the rejection used once the channel is disposed. */
  readonly disposedError: () => unknown
  /** Build the rejection used when a request times out. */
  readonly timeoutError: (id: Id, timeoutMs: number) => unknown
  /** Map a `post` failure before rejecting; defaults to the raw thrown error. */
  readonly mapPostError?: (error: unknown) => unknown
  /** Timer seam; defaults to the unref'd {@link defaultSchedule}. */
  readonly schedule?: PortScheduler
}

/**
 * A framed request/response channel: `request` correlates a reply by id,
 * `onFrame` observes every inbound frame (for unsolicited pushes), and `dispose`
 * rejects every in-flight request. Timers are armed through `PortScheduler`, so
 * the default is unref'd and never keeps the process alive.
 */
export interface RequestChannel<Frame, Reply extends Frame, Id> {
  request(build: (id: Id) => Frame, timeoutMs?: number): Promise<Reply>
  onFrame(handler: (frame: Frame) => void): () => void
  dispose(): void
}

/**
 * Build a request/response channel over `channel`. The reply shape and id are
 * supplied by the caller, so each protocol keeps its own frame codec while the
 * pending-map, timeout and dispose plumbing lives here.
 */
export function createRequestChannel<Frame, Reply extends Frame, Id>(
  channel: FrameChannel<Frame>,
  options: RequestChannelOptions<Frame, Reply, Id>,
): RequestChannel<Frame, Reply, Id> {
  const schedule = options.schedule ?? defaultSchedule
  const pending = new Map<Id, PendingRequest<Reply>>()
  const observers = new Set<(frame: Frame) => void>()
  let disposed = false

  const unsubscribe = channel.onMessage((frame) => {
    // Snapshot before delivering: an observer may unsubscribe (or subscribe)
    // while handling a frame, and that must not perturb this delivery.
    for (const observer of Array.from(observers)) observer(frame)
    if (!options.isReply(frame)) return
    const id = options.idOf(frame)
    const waiter = pending.get(id)
    if (waiter === undefined) return
    pending.delete(id)
    waiter.cancelTimeout?.()
    waiter.resolve(frame)
  })

  return {
    request(build, timeoutMs = options.timeoutMs) {
      if (disposed) return Promise.reject(options.disposedError())
      const id = options.nextId()
      return new Promise<Reply>((resolve, reject) => {
        const cancelTimeout =
          timeoutMs === undefined
            ? undefined
            : schedule(timeoutMs, () => {
                pending.delete(id)
                reject(options.timeoutError(id, timeoutMs))
              })
        pending.set(id, { resolve, reject, cancelTimeout })
        try {
          channel.post(build(id))
        } catch (error) {
          pending.delete(id)
          cancelTimeout?.()
          reject(options.mapPostError === undefined ? error : options.mapPostError(error))
        }
      })
    },
    onFrame(handler) {
      if (disposed) return () => {}
      observers.add(handler)
      return () => observers.delete(handler)
    },
    dispose() {
      if (disposed) return
      disposed = true
      unsubscribe()
      for (const waiter of pending.values()) {
        waiter.cancelTimeout?.()
        waiter.reject(options.disposedError())
      }
      pending.clear()
      observers.clear()
    },
  }
}

// ---------------------------------------------------------------------------
// Peer lifecycle logging
// ---------------------------------------------------------------------------

/**
 * The lifecycle log fields for one peer at a given severity. Returned as a
 * function of `level` so a single call site can emit both the INFO `connected`
 * / `closed` lines and the WARN `connect failed` line from one source.
 */
export function peerLifecycle(
  peer: string,
  workerID: number | undefined,
): (level: "info" | "warn") => Record<string, unknown> {
  return (level) => ({ peer, level, ...(workerID === undefined ? {} : { workerID }) })
}

/**
 * Build a port-backed client, emitting the E2 peer lifecycle lines: an INFO
 * `${peer} connected` once `build` succeeds, or a WARN `${peer} connect failed`
 * (with the error `reason`) before rethrowing. `build` is where the port is
 * started, so a start failure surfaces here.
 */
export function connectPeer<T>(
  log: WorkerLogger,
  peer: string,
  workerID: number | undefined,
  build: () => T,
): T {
  const lifecycle = peerLifecycle(peer, workerID)
  try {
    const client = build()
    log(workerLogMessage(`${peer} connected`), lifecycle("info"))
    return client
  } catch (error) {
    log(workerLogMessage(`${peer} connect failed`), { ...lifecycle("warn"), reason: messageOf(error) })
    throw error
  }
}

export * as WorkerPort from "./port"
