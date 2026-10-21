// Phase 9/11 — shared worker port plumbing.
//
// The cancel control channel (`cancel.ts`) and the job channel
// (`job-client.ts`) both adapt a real `MessagePort` to a framed request/response
// channel, and both need the same unref'd timer and error-message coercion. This
// module is the one implementation they share, so the port glue is not
// duplicated per protocol.

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

export * as WorkerPort from "./port"
