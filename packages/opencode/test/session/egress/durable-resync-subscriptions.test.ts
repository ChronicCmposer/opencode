// Every local durable subscription that matches an overflow
// must be told to resync, not just the first one. A gap signalled by the hub on
// the events data port (a missed `(aggregateID, seq)` commit) is likewise
// broadcast to every live subscription with the exact aggregate position, so the
// worker can re-read its database snapshot.

import { describe, expect, test } from "bun:test"
import { encodeFrame, resyncWithPositionFrame } from "@/session/egress/frame"
import { attachMeshWorker, createEgressMesh } from "@/session/egress/mesh"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { pollUntil } from "../../lib/poll"

type Seen = { readonly pattern: string; readonly aggregateID?: string; readonly seq?: number }
type Position = { readonly aggregateID: string; readonly seq: number }

const positionOf = (meta: Position | undefined): Partial<Position> =>
  meta === undefined ? {} : { aggregateID: meta.aggregateID, seq: meta.seq }

describe("worker durable resync fan-out", () => {
  test("a data-port gap resync reaches EVERY live subscription with its position", async () => {
    const data = new MessageChannel()
    const control = new MessageChannel()
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    const seen: Seen[] = []
    try {
      client.subscribeDurable({
        pattern: "session.*",
        onDurable: () => {},
        onResync: (pattern, meta) => seen.push({ pattern, ...positionOf(meta) }),
      })
      client.subscribeDurable({
        pattern: "*",
        onDurable: () => {},
        onResync: (pattern, meta) => seen.push({ pattern, ...positionOf(meta) }),
      })
      await pollUntil(() => mesh.subscriptions().length === 2)

      data.port2.postMessage(encodeFrame(resyncWithPositionFrame({ aggregateID: "ses_title", seq: 3 })))

      await pollUntil(() => seen.length === 2, { message: "gap resync did not reach every subscription" })
      expect(seen).toEqual([
        { pattern: "session.*", aggregateID: "ses_title", seq: 3 },
        { pattern: "*", aggregateID: "ses_title", seq: 3 },
      ])
    } finally {
      client.dispose()
      attachment.dispose()
      data.port1.close()
      data.port2.close()
      control.port1.close()
      control.port2.close()
    }
  })

  test("a mesh overflow resyncs every overlapping pattern with the dropped commit", async () => {
    const data = new MessageChannel()
    const control = new MessageChannel()
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const mesh = createEgressMesh({ bound: 1 })
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    const seen: Seen[] = []
    try {
      client.subscribeDurable({
        pattern: "session.*",
        onDurable: () => {},
        onResync: (pattern, meta) => seen.push({ pattern, ...positionOf(meta) }),
      })
      client.subscribeDurable({
        pattern: "*",
        onDurable: () => {},
        onResync: (pattern, meta) => seen.push({ pattern, ...positionOf(meta) }),
      })
      await pollUntil(() => mesh.subscriptions().length === 2)

      mesh.publish({ meta: { aggregateID: "ses_title", seq: 1 }, event: { id: "e1", type: "session.updated", properties: { sessionID: "ses_title" } } })
      await pollUntil(() => mesh.outstanding("w1") === 1)
      mesh.publish({ meta: { aggregateID: "ses_title", seq: 2 }, event: { id: "e2", type: "session.updated", properties: { sessionID: "ses_title" } } })

      await pollUntil(() => seen.length === 2, { message: "overflow resync did not reach every pattern" })
      expect(seen).toEqual([
        { pattern: "session.*", aggregateID: "ses_title", seq: 2 },
        { pattern: "*", aggregateID: "ses_title", seq: 2 },
      ])
      expect(mesh.resyncPending).toEqual([])
    } finally {
      client.dispose()
      attachment.dispose()
      data.port1.close()
      data.port2.close()
      control.port1.close()
      control.port2.close()
    }
  })

  test("a throwing resync subscriber does not starve the others", async () => {
    const data = new MessageChannel()
    const control = new MessageChannel()
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    const seen: Seen[] = []
    try {
      client.subscribeDurable({
        pattern: "session.*",
        onDurable: () => {},
        onResync: () => {
          throw new Error("subscriber exploded")
        },
      })
      client.subscribeDurable({
        pattern: "*",
        onDurable: () => {},
        onResync: (pattern, meta) => seen.push({ pattern, ...positionOf(meta) }),
      })
      await pollUntil(() => mesh.subscriptions().length === 2)

      data.port2.postMessage(encodeFrame(resyncWithPositionFrame({ aggregateID: "ses_title", seq: 3 })))

      await pollUntil(() => seen.length === 1, { message: "a throwing subscriber starved the rest" })
      expect(seen).toEqual([{ pattern: "*", aggregateID: "ses_title", seq: 3 }])
    } finally {
      client.dispose()
      attachment.dispose()
      data.port1.close()
      data.port2.close()
      control.port1.close()
      control.port2.close()
    }
  })
})
