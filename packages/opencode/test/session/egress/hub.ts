// Test-only hub-side egress session (relocated from `src/session/egress/hub.ts`,
// plan finding B1). Production serves the worker events port through
// `session/worker/hub.ts`; this hub+SSE stream abstraction was never wired to a
// production route, so it lives under `test/` and backs the hub/worker-egress
// stream tests only.
//
// Ties a worker's dedicated egress port to a JSON fan-out and exposes one async
// JSON stream per SSE client. The hub — not the worker — owns termination: on
// `close` it drops buffered non-durable frames, emits a terminal `end`, and
// closes the egress + control ports. Durable frames are delivered as they arrive
// (immediate flush worker-side), so termination never reorders or loses a
// committed boundary.
//
// Termination is tracked with a `terminated` flag rather than re-decoding each
// drained chunk to look for the `end` marker, and a durable-saturated subscriber
// is surfaced to its stream as a terminal typed error instead of silently
// dropping a committed boundary.

import { endFrame } from "@/session/worker/frame"

import type { ControlLink } from "@/session/egress/control"
import { createControlLink } from "@/session/egress/control"
import { encodeFrame } from "@/session/egress/frame"
import { createEgressReceiver, type EgressReceiver } from "@/session/egress/receiver"
import { createFanout, type Fanout } from "./fanout"

export interface EgressHubOptions {
  readonly egressPort: MessagePort
  /** Dedicated control port, distinct from egress. */
  readonly controlPort?: MessagePort
  /** Per-client backpressure bound; defaults to 256. */
  readonly bound?: number
}

/** Raised on a subscriber stream whose durable queue overflowed — a lost commit. */
export class EgressStreamOverflowError extends Error {
  constructor(readonly subscriberId: string) {
    super(`egress subscriber "${subscriberId}" overflowed: durable queue is full`)
    this.name = "EgressStreamOverflowError"
  }
}

export interface EgressHub {
  readonly fanout: Fanout
  readonly receiver: EgressReceiver
  readonly control: ControlLink | undefined
  /** Non-end frames delivered from the worker. */
  readonly frames: number
  readonly closed: boolean
  /** The async JSON stream an SSE route writes for one client. */
  stream(subscriberId: string): AsyncIterable<string>
  /** Drop non-durable, emit `end`, close ports; idempotent (worker `end` or terminate). */
  close(): void
}

const END_FRAME = endFrame()
/** The terminal frame's wire JSON, handed to subscribers that arrive after termination. */
const END_BYTES = encodeFrame(END_FRAME)

function createNotifier() {
  let waiters: (() => void)[] = []
  return {
    wait: () =>
      new Promise<void>((resolve) => {
        waiters.push(resolve)
      }),
    notify: () => {
      const current = waiters
      waiters = []
      for (const resolve of current) resolve()
    },
  }
}

export function createEgressHub(options: EgressHubOptions): EgressHub {
  const fanout = createFanout({ bound: options.bound })
  const notifier = createNotifier()
  let frames = 0
  let closed = false
  // Set once the terminal `end` marker is queued; the stream returns without
  // re-decoding drained chunks to discover it.
  let terminated = false

  // Declared before `finish` so the closure never observes an uninitialized
  // binding; both are assigned synchronously below, before any callback fires.
  let receiver: EgressReceiver
  let control: ControlLink | undefined

  const finish = (): void => {
    if (closed) return
    closed = true
    fanout.finalize(END_FRAME)
    terminated = true
    receiver.close()
    control?.dispose()
    notifier.notify()
  }

  receiver = createEgressReceiver({
    port: options.egressPort,
    onFrame: (frame, text) => {
      frames++
      fanout.publishBytes(text, frame)
      notifier.notify()
    },
    onEnd: finish,
  })

  control = options.controlPort ? createControlLink(options.controlPort) : undefined

  return {
    fanout,
    receiver,
    control,
    get frames() {
      return frames
    },
    get closed() {
      return closed
    },
    async *stream(subscriberId) {
      const subscriber = fanout.subscribe(subscriberId)
      try {
        // A subscriber that arrives AFTER termination never had `finalize`'s end
        // frame queued for it, so hand it the terminal frame before returning;
        // otherwise a late SSE client would see an empty, silently-truncated stream.
        if (terminated) {
          yield END_BYTES
          return
        }
        while (true) {
          // A durable-saturated overflow means a committed boundary was lost:
          // fail loud rather than emit a silently truncated stream.
          if (subscriber.overflowed) throw new EgressStreamOverflowError(subscriberId)
          const chunks = subscriber.drainBytes()
          if (chunks.length === 0) {
            if (terminated) return
            await notifier.wait()
            continue
          }
          for (const chunk of chunks) yield chunk
          if (terminated) return
        }
      } finally {
        fanout.unsubscribe(subscriberId)
      }
    },
    close: finish,
  }
}
