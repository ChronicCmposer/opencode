// Phase 8.1 + 8.5 — hub-side egress receiver (plan tasks 8.1, 8.5).
//
// The hub end of a worker's dedicated egress port. Incoming `Uint8Array` chunks
// are reassembled with the length-prefixed codec (a frame may be split across
// deliveries and a delivery may carry several frames). Complete frames are handed
// to `onFrame` in arrival order — which is commit order for a single worker — and
// durable ordinals are checked for generation-scoped monotonicity, so an
// out-of-order or stale-generation durable frame fails loud instead of silently
// corrupting commit order.

import { decodeFrameChunks, type EgressFrame } from "./frame"
import { compareOrdinals, type WriterOrdinal } from "./ordinal"

export interface EgressReceiverOptions {
  readonly port: MessagePort
  /** Called with each complete frame and the exact wire bytes it arrived as. */
  readonly onFrame: (frame: EgressFrame, bytes: Uint8Array) => void
  readonly onEnd?: () => void
  /** Called once on the first framing/ordering error; the receiver then stops. */
  readonly onError?: (error: Error) => void
}

export interface EgressReceiver {
  /** Number of non-end frames delivered. */
  readonly received: number
  readonly ended: boolean
  /** Bytes buffered awaiting a complete frame. */
  readonly pendingBytes: number
  readonly lastError: Error | undefined
  lastOrdinal(): WriterOrdinal | undefined
  close(): void
}

const EMPTY = new Uint8Array(0)

function asBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data
  if (data instanceof ArrayBuffer) return new Uint8Array(data)
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
  throw new Error(
    `egress receiver expected Uint8Array, ArrayBuffer, or ArrayBufferView, got: ${Object.prototype.toString.call(data)}`,
  )
}

function concat(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.byteLength === 0) return right
  const joined = new Uint8Array(left.byteLength + right.byteLength)
  joined.set(left, 0)
  joined.set(right, left.byteLength)
  return joined
}

export function createEgressReceiver(options: EgressReceiverOptions): EgressReceiver {
  const { port } = options
  let rest: Uint8Array = EMPTY
  let received = 0
  let ended = false
  let lastError: Error | undefined
  let last: WriterOrdinal | undefined

  const fail = (error: Error): void => {
    if (lastError) return
    lastError = error
    options.onError?.(error)
    port.close()
  }

  const handleFrame = (frame: EgressFrame, bytes: Uint8Array): void => {
    if (frame.kind === "end") {
      ended = true
      options.onEnd?.()
      return
    }
    if (frame.kind === "durable") {
      if (last && compareOrdinals(frame.meta, last) <= 0) {
        throw new Error(
          `out-of-order durable egress: got ${frame.meta.generation}/${frame.meta.ordinal}, last ${last.generation}/${last.ordinal}`,
        )
      }
      last = frame.meta
    }
    received++
    options.onFrame(frame, bytes)
  }

  port.onmessage = (event: MessageEvent) => {
    if (lastError || ended) return
    try {
      const chunk = asBytes(event.data)
      const { frames, rest: nextRest } = decodeFrameChunks(concat(rest, chunk))
      // Commit the trailing bytes only once every frame in the batch is handled:
      // a mid-batch failure must not advance `rest` past undelivered frames.
      for (const { frame, bytes } of frames) handleFrame(frame, bytes)
      rest = nextRest
    } catch (error) {
      fail(error instanceof Error ? error : new Error(String(error)))
    }
  }
  port.start()

  return {
    get received() {
      return received
    },
    get ended() {
      return ended
    },
    get pendingBytes() {
      return rest.byteLength
    },
    get lastError() {
      return lastError
    },
    lastOrdinal: () => last,
    close: () => port.close(),
  }
}

export * as EgressReceiverModule from "./receiver"
