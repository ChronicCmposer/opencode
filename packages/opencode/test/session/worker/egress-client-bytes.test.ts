// The worker side of the events port speaks byte frames in both directions.

import { describe, expect, test } from "bun:test"
import { encodeEventsFrameBytes } from "@/session/egress/frame-bytes"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { ackFrame, endFrame, eventFrame, resyncFrame, type EventV2Event } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"
import { readByteFrame } from "../egress/byte-frames"

const delta = (id: string): EventV2Event => ({ id, type: "message.part.delta", properties: { id, text: "hi" } })

describe("worker egress client byte frames", () => {
  test("posts every outbound frame as a transferred ArrayBuffer", async () => {
    const channel = new MessageChannel()
    const received: unknown[] = []
    channel.port2.onmessage = (event: MessageEvent) => received.push(event.data)
    channel.port2.start()
    const client = createWorkerEgressClient(channel.port1, { maxBatch: 1 })
    try {
      client.emit(delta("a"))
      client.emitDurable({ id: "d", type: "message.updated", properties: { id: "d" } }, { aggregateID: "agg", seq: 0 })
      client.end()
      await pollUntil(() => received.length === 5)
      expect(received.every((data) => data instanceof ArrayBuffer)).toBe(true)
      expect(received.map(readByteFrame)).toEqual([
        eventFrame({ event: delta("a") }),
        eventFrame({
          event: { id: "d", type: "message.updated", properties: { id: "d" } },
          aggregateID: "agg",
          seq: 0,
        } as Parameters<typeof eventFrame>[0]),
        ackFrame({ count: 1 }),
        ackFrame({ count: 0 }),
        endFrame(),
      ])
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("decodes a byte resync frame from the hub and heals the named position", async () => {
    const channel = new MessageChannel()
    const client = createWorkerEgressClient(channel.port1, { controlPort: new MessageChannel().port1 })
    const resyncs: Array<{ pattern: string; position: unknown }> = []
    client.subscribeDurable({
      pattern: "*",
      onDurable: () => undefined,
      onResync: (pattern, position) => void resyncs.push({ pattern, position }),
    })
    try {
      const buffer = encodeEventsFrameBytes(resyncFrame({ aggregateID: "agg", seq: 3 } as never))
      channel.port2.postMessage(buffer, [buffer])
      await pollUntil(() => resyncs.length === 1)
      expect(resyncs).toEqual([{ pattern: "*", position: { aggregateID: "agg", seq: 3 } }])
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("ignores string messages and non-resync byte frames", async () => {
    const channel = new MessageChannel()
    const client = createWorkerEgressClient(channel.port1, { controlPort: new MessageChannel().port1 })
    let resyncs = 0
    client.subscribeDurable({ pattern: "*", onDurable: () => undefined, onResync: () => void (resyncs += 1) })
    try {
      channel.port2.postMessage(JSON.stringify(resyncFrame({ pattern: "*" })))
      for (const frame of [ackFrame({ count: 1 }), endFrame()]) {
        const buffer = encodeEventsFrameBytes(frame)
        channel.port2.postMessage(buffer, [buffer])
      }
      const marker = encodeEventsFrameBytes(resyncFrame({ pattern: "*" }))
      channel.port2.postMessage(marker, [marker])
      await pollUntil(() => resyncs >= 1)
      expect(resyncs).toBe(1)
    } finally {
      channel.port1.close()
      channel.port2.close()
    }
  })
})
