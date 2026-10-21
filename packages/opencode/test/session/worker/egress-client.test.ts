// W2 — worker-side egress client over the events envelope (spec Q6/Q7/Q21).
//
// `createWorkerEgressClient(port)` turns the worker's EventV2 events into framed
// bytes on its dedicated egress `MessagePort`, reusing `egress/sender.ts` (data
// plane) and `egress/mesh.ts` (control plane). Events cross VERBATIM as
// `{ id, type, properties }`; durable events bypass the coalescing window.

import { describe, expect, test } from "bun:test"
import { type EgressFrame } from "@/session/egress/frame"
import { createEgressReceiver } from "@/session/egress/receiver"
import { attachMeshWorker, createEgressMesh, type DurableCommit } from "@/session/egress/mesh"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { type EventV2Event, eventFrame } from "@/session/worker/frame"

async function pollUntil(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

const text = (id: string, value: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: { id, text: value },
})

const durable = (id: string): EventV2Event => ({ id, type: "message.updated", properties: { id } })

describe("worker egress client framing round-trip", () => {
  test("emit is transported as an event frame with verbatim {id,type,properties}", async () => {
    const channel = new MessageChannel()
    const frames: EgressFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const client = createWorkerEgressClient(channel.port1, { maxBatch: 1 })

    const event = text("e1", "hi")
    client.emit(event)

    await pollUntil(() => frames.length === 1)
    expect(frames).toEqual([eventFrame({ event })])
    expect(receiver.pendingBytes).toBe(0)
    client.dispose()
  })

  test("end flushes buffered deltas then terminates the stream", async () => {
    const channel = new MessageChannel()
    const frames: EgressFrame[] = []
    let ended = false
    createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame), onEnd: () => (ended = true) })
    // Default 16ms window: nothing flushes until `end()`.
    const client = createWorkerEgressClient(channel.port1)

    client.emit(text("e1", "hi"))
    expect(client.buffered).toBe(1)
    client.end()

    await pollUntil(() => ended)
    expect(frames).toEqual([eventFrame({ event: text("e1", "hi") })])
    expect(client.ended).toBe(true)
  })

  test("emitDurable carries writer meta and preserves commit order", async () => {
    const channel = new MessageChannel()
    const frames: EgressFrame[] = []
    createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const client = createWorkerEgressClient(channel.port1)

    client.emitDurable(durable("d1"), { generation: 1, ordinal: 1 })
    client.emitDurable(durable("d2"), { generation: 1, ordinal: 2 })

    await pollUntil(() => frames.length === 2)
    expect(frames).toEqual([
      eventFrame({ event: durable("d1"), generation: 1, ordinal: 1 }),
      eventFrame({ event: durable("d2"), generation: 1, ordinal: 2 }),
    ])
    client.dispose()
  })
})

describe("worker egress client adapts the mesh control link", () => {
  test("a durable mesh wake is delivered to the worker client", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, { controlPort: controlChannel.port1 })

    expect(client.mesh).toBeDefined()
    const commits: DurableCommit[] = []
    client.mesh?.onDurable((commit) => commits.push(commit))
    await client.mesh?.listen("message.updated")

    mesh.publish({ meta: { generation: 1, ordinal: 1 }, event: durable("d1") })

    await pollUntil(() => commits.length === 1)
    expect(commits[0]?.event.id).toBe("d1")
    client.dispose()
    attachment.dispose()
  })

  test("no control port means no mesh link", () => {
    const channel = new MessageChannel()
    const client = createWorkerEgressClient(channel.port1)
    expect(client.mesh).toBeUndefined()
    client.dispose()
  })
})
