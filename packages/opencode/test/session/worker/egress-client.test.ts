// W2 — worker-side egress client over the events envelope (spec Q6/Q7/Q21).
//
// `createWorkerEgressClient(port)` turns the worker's EventV2 events into framed
// bytes on its dedicated egress `MessagePort`, reusing `egress/sender.ts` (data
// plane) and `egress/mesh.ts` (control plane). Events cross VERBATIM as
// `{ id, type, properties }`; durable events bypass the coalescing window.

import { describe, expect, test } from "bun:test"
import { createEgressReceiver } from "@/session/egress/receiver"
import { attachMeshWorker, createEgressMesh, type DurableCommit } from "@/session/egress/mesh"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { workerLogMessage, type WorkerLogger } from "@/session/worker/log"
import { type EventsFrame, type EventV2Event, eventFrame } from "@/session/worker/frame"

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
    const frames: EventsFrame[] = []
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
    const frames: EventsFrame[] = []
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
    const frames: EventsFrame[] = []
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

  test("acknowledgeDelivery frees the hub backlog slot end-to-end", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh({ bound: 1 })
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, { controlPort: controlChannel.port1 })

    const commits: DurableCommit[] = []
    client.mesh?.onDurable((commit) => commits.push(commit))
    await client.mesh?.listen("*")

    mesh.publish({ meta: { generation: 1, ordinal: 1 }, event: durable("d1") })
    await pollUntil(() => commits.length === 1)
    expect(mesh.outstanding("w1")).toBe(1)

    // The worker acknowledges the processed delivery; the slot frees and the
    // next commit is no longer treated as overflow.
    client.mesh?.acknowledgeDelivery()
    await pollUntil(() => mesh.outstanding("w1") === 0)

    mesh.publish({ meta: { generation: 1, ordinal: 2 }, event: durable("d2") })
    await pollUntil(() => commits.length === 2)
    expect(commits.map((commit) => commit.event.id)).toEqual(["d1", "d2"])

    client.dispose()
    attachment.dispose()
  })

  test("a mesh overflow resync reaches onResync and acknowledgeResync clears it", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh({ bound: 1 })
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, { controlPort: controlChannel.port1 })

    const resyncs: string[] = []
    client.mesh?.onResync((pattern) => resyncs.push(pattern))
    await client.mesh?.listen("*")

    // The first delivery occupies the only slot; the second overflows to resync.
    mesh.publish({ meta: { generation: 1, ordinal: 1 }, event: durable("d1") })
    mesh.publish({ meta: { generation: 1, ordinal: 2 }, event: durable("d2") })

    await pollUntil(() => resyncs.length === 1)
    expect(resyncs).toEqual(["*"])
    expect(mesh.resyncPending).toEqual(["w1"])

    client.mesh?.acknowledgeResync("*")
    await pollUntil(() => mesh.resyncPending.length === 0)

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

describe("worker egress client drives durable mesh subscriptions", () => {
  test("a hub-side mesh publish wakes the worker subscriber and acks the delivery", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, { controlPort: controlChannel.port1 })

    const commits: DurableCommit[] = []
    client.subscribeDurable({ pattern: "message.updated", onDurable: (commit) => commits.push(commit) })
    await pollUntil(() =>
      mesh.subscriptions().some((s) => s.workerId === "w1" && s.pattern === "message.updated"),
    )

    mesh.publish({ meta: { generation: 1, ordinal: 1 }, event: durable("d1") })

    await pollUntil(() => commits.length === 1)
    expect(commits[0]?.event.id).toBe("d1")
    // The subscriber's delivery is acknowledged automatically, freeing the slot.
    await pollUntil(() => mesh.outstanding("w1") === 0)

    client.dispose()
    attachment.dispose()
  })

  test("an overflow resync reaches the subscriber and is acknowledged", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh({ bound: 1 })
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, { controlPort: controlChannel.port1 })

    const commits: DurableCommit[] = []
    const resyncs: string[] = []
    client.subscribeDurable({
      pattern: "*",
      onDurable: (commit) => commits.push(commit),
      onResync: (pattern) => resyncs.push(pattern),
    })
    await pollUntil(() => mesh.subscriptions().length === 1)

    // The first delivery occupies the only slot; the second overflows to resync.
    mesh.publish({ meta: { generation: 1, ordinal: 1 }, event: durable("d1") })
    mesh.publish({ meta: { generation: 1, ordinal: 2 }, event: durable("d2") })

    await pollUntil(() => resyncs.length === 1)
    expect(resyncs).toEqual(["*"])
    // The resync is acknowledged automatically, clearing the hub's pending resync.
    await pollUntil(() => mesh.resyncPending.length === 0)

    client.dispose()
    attachment.dispose()
  })

  test("unsubscribing removes the pattern from the hub mesh", async () => {
    const egressChannel = new MessageChannel()
    const controlChannel = new MessageChannel()
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", controlChannel.port2)
    const client = createWorkerEgressClient(egressChannel.port1, { controlPort: controlChannel.port1 })

    const unsubscribe = client.subscribeDurable({ pattern: "message.updated", onDurable: () => {} })
    await pollUntil(() => mesh.subscriptions().length === 1)

    unsubscribe()
    await pollUntil(() => mesh.subscriptions().length === 0)

    client.dispose()
    attachment.dispose()
  })

  test("subscribeDurable without a control port fails loud", () => {
    const channel = new MessageChannel()
    const client = createWorkerEgressClient(channel.port1)

    expect(() => client.subscribeDurable({ pattern: "*", onDurable: () => {} })).toThrow(/no control port/)
    client.dispose()
  })
})

// ---------------------------------------------------------------------------
// E2 — peer lifecycle logging. The data plane is "connected" once the sender is
// adapted; the optional mesh control link is part of the same connect seam, so a
// failed control-port start emits a WARN connect-failed line before rethrowing.
// ---------------------------------------------------------------------------

interface RecordedLog {
  readonly message: string
  readonly fields: Record<string, unknown>
}

function recordingLogger(records: RecordedLog[]): WorkerLogger {
  return (message, fields) => void records.push({ message, fields })
}

describe("worker egress client lifecycle logging (E2)", () => {
  test("emits an INFO connect line with the peer and workerID", () => {
    const channel = new MessageChannel()
    const records: RecordedLog[] = []
    const client = createWorkerEgressClient(channel.port1, {
      maxBatch: 1,
      log: recordingLogger(records),
      workerID: 9,
    })

    expect(records).toEqual([
      { message: workerLogMessage("egress connected"), fields: { peer: "egress", level: "info", workerID: 9 } },
    ])
    client.dispose()
  })

  test("emits an INFO close line on dispose", () => {
    const channel = new MessageChannel()
    const records: RecordedLog[] = []
    const client = createWorkerEgressClient(channel.port1, { log: recordingLogger(records), workerID: 9 })
    client.dispose()

    expect(records.at(-1)).toEqual({
      message: workerLogMessage("egress closed"),
      fields: { peer: "egress", level: "info", workerID: 9 },
    })
  })

  test("omits workerID when the caller did not supply one", () => {
    const channel = new MessageChannel()
    const records: RecordedLog[] = []
    const client = createWorkerEgressClient(channel.port1, { log: recordingLogger(records) })

    expect(records[0]?.fields).toEqual({ peer: "egress", level: "info" })
    client.dispose()
  })

  test("a failed mesh-control connect emits a WARN line with the reason and rethrows", () => {
    const channel = new MessageChannel()
    const records: RecordedLog[] = []
    const brokenControl = {
      addEventListener: () => {},
      removeEventListener: () => {},
      start: () => {
        throw new Error("control closed")
      },
    } as unknown as MessagePort

    expect(() =>
      createWorkerEgressClient(channel.port1, {
        controlPort: brokenControl,
        log: recordingLogger(records),
        workerID: 9,
      }),
    ).toThrow("control closed")
    expect(records).toEqual([
      {
        message: workerLogMessage("egress connect failed"),
        fields: { peer: "egress", level: "warn", reason: "control closed", workerID: 9 },
      },
    ])
  })
})
