// Every durable core blob marker becomes a resolvable position reference.
//
// Core offloads an oversized durable payload to the shared blob store and leaves
// `{ __eventBlob, size }` in the in-memory event. The marker itself is tiny, so
// the frame codec used to forward it raw whenever the *marker* frame fit — even
// though the hub can only resolve `{ __eventBlobPosition, frameBytes }`. The
// window between the inline bound and the frame cap is exactly where that bug
// showed: the marker fits, the body does not, and the event was published
// unresolved. A durable marker must always be rewritten, whatever its declared
// size; a non-durable marker keeps its legacy pass-through.

import { describe, expect, test } from "bun:test"
import { DurableLog } from "@opencode-ai/core/durable-log"
import {
  MAX_FRAME_BYTES,
  MAX_INLINE_EVENT_BYTES,
  decodeFrame,
  encodeFrameChunks,
} from "@/session/egress/frame"
import { eventFrame, type EventV2Event } from "@/session/worker/frame"

function coreMarker(size: number): { readonly __eventBlob: string; readonly size: number } {
  return { __eventBlob: DurableLog.digest(`durable-body-${size}`), size }
}

function durableEvent(size: number): EventV2Event {
  return {
    id: `evt_marker_${size}`,
    type: "session.next.prompted",
    properties: coreMarker(size),
  }
}

function decodedProperties(frame: ReturnType<typeof eventFrame>): unknown {
  const [text] = encodeFrameChunks([frame]).flat()
  const decoded = decodeFrame(text!)
  if (decoded.kind !== "event") throw new Error(`expected an event frame, got ${decoded.kind}`)
  return decoded.payload.event.properties
}

describe("durable core blob marker boundary", () => {
  test("rewrites a marker just above the inline bound", () => {
    const size = MAX_INLINE_EVENT_BYTES + 1
    const position = { aggregateID: "ses_marker_inline", seq: 1 }
    const frame = eventFrame({ event: durableEvent(size), ...position })

    expect(decodedProperties(frame)).toEqual({ __eventBlobPosition: position, frameBytes: size })
  })

  test("rewrites a marker one byte below the frame cap", () => {
    const size = MAX_FRAME_BYTES - 1
    const position = { aggregateID: "ses_marker_frame", seq: 2 }
    const frame = eventFrame({ event: durableEvent(size), ...position })

    expect(decodedProperties(frame)).toEqual({ __eventBlobPosition: position, frameBytes: size })
  })

  test("keeps a non-durable core marker unchanged", () => {
    const marker = coreMarker(MAX_INLINE_EVENT_BYTES + 1)
    const frame = eventFrame({
      event: { id: "evt_marker_delta", type: "session.next.prompted", properties: marker },
    })

    expect(decodedProperties(frame)).toEqual(marker)
  })
})
