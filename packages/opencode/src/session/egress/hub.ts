// Phase 8.1 + 8.7 + 8.12 — hub-side egress session (plan tasks 8.1, 8.7, 8.12).
//
// Ties a worker's dedicated egress port to a byte fan-out and exposes one async
// byte stream per SSE client. The hub — not the worker — owns termination: on
// `close` it drops buffered non-durable frames, emits a terminal `end`, and
// closes the egress + control ports (plan decision line 83). Durable frames are
// delivered as they arrive (immediate flush worker-side), so termination never
// reorders or loses a committed boundary.

import type { ControlLink } from "./control"
import { createControlLink } from "./control"
import { decodeFrame, type EgressFrame } from "./frame"
import { createFanout, type Fanout } from "./fanout"
import { createEgressReceiver, type EgressReceiver } from "./receiver"

export interface EgressHubOptions {
  readonly egressPort: MessagePort
  /** Dedicated control port, distinct from egress (task 8.13). */
  readonly controlPort?: MessagePort
  /** Per-client backpressure bound; defaults to 256. */
  readonly bound?: number
}

export interface EgressHub {
  readonly fanout: Fanout
  readonly receiver: EgressReceiver
  readonly control: ControlLink | undefined
  /** Non-end frames delivered from the worker. */
  readonly frames: number
  readonly closed: boolean
  /** The async byte stream an SSE route writes for one client. */
  stream(subscriberId: string): AsyncIterable<Uint8Array>
  /** Drop non-durable, emit `end`, close ports; idempotent (worker `end` or terminate). */
  close(): void
}

const END_FRAME: EgressFrame = { kind: "end", v: 1 }

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

function isEndFrame(bytes: Uint8Array): boolean {
  try {
    return decodeFrame(bytes).kind === "end"
  } catch {
    return false
  }
}

export function createEgressHub(options: EgressHubOptions): EgressHub {
  const fanout = createFanout({ bound: options.bound })
  const notifier = createNotifier()
  let frames = 0
  let closed = false

  const finish = (): void => {
    if (closed) return
    closed = true
    fanout.finalize(END_FRAME)
    receiver.close()
    control?.dispose()
    notifier.notify()
  }

  const receiver = createEgressReceiver({
    port: options.egressPort,
    onFrame: (frame, bytes) => {
      frames++
      fanout.publishBytes(bytes, frame)
      notifier.notify()
    },
    onEnd: finish,
  })

  const control = options.controlPort ? createControlLink(options.controlPort) : undefined

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
        while (true) {
          const chunks = subscriber.drainBytes()
          if (chunks.length === 0) {
            if (closed) return
            await notifier.wait()
            continue
          }
          for (const chunk of chunks) {
            yield chunk
            if (isEndFrame(chunk)) return
          }
        }
      } finally {
        fanout.unsubscribe(subscriberId)
      }
    },
    close: finish,
  }
}

export * as EgressHubModule from "./hub"
