// Egress across a real worker isolate.
//
// Spawns the egress fixture with `test/lib/worker-harness.ts`; the worker
// transfers a dedicated egress port and a separate control port. Proves frames
// cross the isolate over the dedicated port, control replies are not starved
// behind a delta flood, and hub-owned terminate flushes durable / drops
// non-durable / ends the stream.

import { describe, expect, test } from "bun:test"
import { createControlLink, type ControlFrame } from "@/session/egress/control"
import { createEgressHub } from "./hub"
import { decodeFrame, type EventsFrame } from "@/session/egress/frame"
import { attachMeshWorker, createEgressMesh } from "@/session/egress/mesh"
import { endFrame } from "@/session/worker/frame"
import { createWorkerHarness } from "../../lib/worker-harness"
import { deltasFrame, durable, durableFrame } from "../../lib/egress-fixtures"
import { pollUntil } from "../../lib/poll"

const WORKER_ENTRY = new URL("../../fixture/egress-worker.ts", import.meta.url)

interface PortsMessage {
  readonly type: "ports"
  readonly egress: MessagePort
  readonly control: MessagePort
  /** Dedicated EventV2 mesh control port (durable wakeups + ack/resync). */
  readonly egressControl: MessagePort
}
interface FloodMessage {
  readonly type: "flood.done"
  readonly count: number
}
interface MeshListenOkMessage {
  readonly type: "mesh.listen.ok"
  readonly pattern?: string
}
interface MeshDurableMessage {
  readonly type: "mesh.durable"
  readonly id: string
}
interface MeshResyncMessage {
  readonly type: "mesh.resync"
  readonly pattern: string
}
type WorkerOut = PortsMessage | FloodMessage | MeshListenOkMessage | MeshDurableMessage | MeshResyncMessage
type WorkerIn = {
  readonly type: string
  readonly id?: string
  readonly ordinal?: number
  readonly count?: number
  readonly pattern?: string
}

// Cross-isolate waits settle more slowly than an in-process `MessageChannel`, so
// budget more time here than the shared helper's default.
const pollWorker = (predicate: () => boolean) => pollUntil(predicate, { timeoutMs: 3000 })

/**
 * Spawn the egress fixture worker and wire its ports to a hub. The returned
 * handle is `await using`-disposable so every test tears down the hub and the
 * worker isolate in one place instead of a hand-rolled `try/finally`.
 */
async function spawnEgressWorker(options: { readonly meshBound?: number } = {}) {
  const harness = createWorkerHarness({ cores: 1 })
  try {
    const worker = harness.spawn<WorkerOut, WorkerIn>(WORKER_ENTRY)
    const ports = await worker.waitFor("ports")
    const hub = createEgressHub({ egressPort: ports.egress, controlPort: ports.control })
    // The hub end of the mesh registry; the worker drives the matching client
    // over the transferred `egressControl` port.
    const mesh = createEgressMesh(options.meshBound === undefined ? {} : { bound: options.meshBound })
    const meshAttachment = attachMeshWorker(mesh, "w1", ports.egressControl)
    return {
      worker,
      hub,
      mesh,
      ports,
      async [Symbol.asyncDispose]() {
        meshAttachment.dispose()
        hub.close()
        await harness.dispose()
      },
    }
  } catch (error) {
    await harness.dispose()
    throw error
  }
}

describe("egress across a worker isolate", () => {
  test("a worker delta reaches the hub over the dedicated egress port", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub, ports } = session
    expect(ports.egress).not.toBe(ports.control)
    const subscriber = hub.fanout.subscribe("sse")
    worker.post({ type: "delta", id: "w1" })
    await pollWorker(() => hub.frames === 1)
    expect(subscriber.drain()).toEqual([deltasFrame("w1")])
  })

  test("durable frames cross in commit order", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const subscriber = hub.fanout.subscribe("sse")
    worker.post({ type: "durable", id: "d1", ordinal: 1 })
    worker.post({ type: "durable", id: "d2", ordinal: 2 })
    await pollWorker(() => hub.frames === 2)
    const frames = subscriber.drain()
    expect(
      frames.map((frame) => (frame.kind === "event" ? [frame.payload.seq ?? -1, frame.payload.event.id] : [-1, ""])),
    ).toEqual([
      [1, "d1"],
      [2, "d2"],
    ])
  })

  test("the control port answers while a delta flood is in flight", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const controlMessages: string[] = []
    hub.control!.onMessage((frame) => controlMessages.push(frame.type))
    // Flood the worker with deltas, then immediately ping on the control port.
    worker.post({ type: "flood", count: 5000 })
    const pong = await hub.control!.request({ type: "ping" })
    expect(pong.type).toBe("pong")

    // Both channels make progress: the flood completes and egress delivers.
    await pollWorker(() => controlMessages.includes("flood.done"))
    await pollWorker(() => hub.frames > 0)
  })

  test("hub terminate drops buffered non-durable and flushes durable", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const subscriber = hub.fanout.subscribe("sse")
    worker.post({ type: "delta", id: "dropped" })
    worker.post({ type: "durable", id: "kept", ordinal: 1 })
    await pollWorker(() => hub.frames === 2)
    hub.close()

    expect(subscriber.drain()).toEqual([durableFrame("kept", 1), endFrame()])
  })

  test("a worker `end` command terminates the stream", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const frames: EventsFrame[] = []
    const consumer = (async () => {
      for await (const bytes of hub.stream("sse")) frames.push(decodeFrame(bytes))
    })()
    await pollWorker(() => hub.fanout.subscribers.length === 1)
    worker.post({ type: "end" })
    await consumer
    expect(frames).toEqual([endFrame()])
  })

  test("the worker transfers a dedicated egressControl port", async () => {
    await using session = await spawnEgressWorker()
    const { ports } = session
    expect(ports.egressControl).toBeInstanceOf(MessagePort)
    expect(ports.egressControl).not.toBe(ports.egress)
    expect(ports.egressControl).not.toBe(ports.control)
  })

  test("mesh listen, durable wake and ack cross the worker boundary", async () => {
    await using session = await spawnEgressWorker()
    const { worker, mesh } = session

    worker.post({ type: "mesh.listen", pattern: "message.*" })
    await worker.waitFor("mesh.listen.ok")
    expect(mesh.subscriptions()).toEqual([{ workerId: "w1", pattern: "message.*" }])

    mesh.publish({ meta: { aggregateID: "agg", seq: 1 }, event: durable("m1") })
    const wake = await worker.waitFor("mesh.durable")
    expect(wake.id).toBe("m1")
    expect(mesh.outstanding("w1")).toBe(1)

    worker.post({ type: "mesh.ack", count: 1 })
    await pollWorker(() => mesh.outstanding("w1") === 0)
  })

  test("mesh overflow resync crosses the worker boundary and is acknowledged", async () => {
    await using session = await spawnEgressWorker({ meshBound: 1 })
    const { worker, mesh } = session

    worker.post({ type: "mesh.listen", pattern: "message.*" })
    await worker.waitFor("mesh.listen.ok")

    // The first commit occupies the only backlog slot; the second overflows.
    mesh.publish({ meta: { aggregateID: "agg", seq: 1 }, event: durable("m1") })
    await worker.waitFor("mesh.durable")
    mesh.publish({ meta: { aggregateID: "agg", seq: 2 }, event: durable("m2") })

    const resync = await worker.waitFor("mesh.resync")
    expect(resync.pattern).toBe("message.*")
    expect(mesh.resyncPending).toEqual(["w1"])

    worker.post({ type: "mesh.resync.ack", pattern: "message.*" })
    await pollWorker(() => mesh.resyncPending.length === 0)
  })
})

describe("control frame variant validation", () => {
  /**
   * Post one candidate frame, then a known-good `resync` sentinel. A malformed
   * KNOWN frame must be rejected (lastError set) and must NOT reach the observer,
   * so the only frame observed is the sentinel.
   */
  async function rejectedFrames(frame: unknown): Promise<void> {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    const seen: ControlFrame[] = []
    link.onMessage((next) => seen.push(next))
    channel.port2.postMessage(frame)
    channel.port2.postMessage({ type: "resync", pattern: "sentinel" })
    await pollWorker(() => link.lastError !== undefined && seen.length === 1)
    expect(link.lastError?.message).toMatch(/invalid control frame/)
    expect(seen).toEqual([{ type: "resync", pattern: "sentinel" }])
    link.dispose()
    channel.port2.close()
  }

  test("rejects a known frame whose body is malformed", async () => {
    const malformed: readonly unknown[] = [
      { type: "ping" }, // no id
      { type: "ping", id: "1" }, // non-numeric id
      { type: "pong", id: 1.5 }, // fractional id
      { type: "listen", id: 1 }, // no pattern
      { type: "listen", id: 1, pattern: 2 }, // non-string pattern
      { type: "unlisten", id: 1 }, // no pattern
      { type: "listen.ok" }, // no id
      { type: "durable", meta: { aggregateID: "agg" }, event: durable("d") }, // missing seq
      { type: "durable", meta: { aggregateID: "agg", seq: 1 }, event: { id: "d", type: "t" } }, // no properties
      { type: "resync" }, // no pattern
      { type: "resync", pattern: 3 }, // non-string pattern
      { type: "durable.ack", count: -1 }, // negative count
      { type: "resync.ack", pattern: 4 }, // non-string pattern
    ]
    for (const frame of malformed) await rejectedFrames(frame)
  }, 30000)

  test("accepts a well-formed frame of every variant and narrows it", async () => {
    const valid: ControlFrame[] = [
      { type: "ping", id: 1 },
      { type: "pong", id: 2 },
      { type: "listen", id: 3, pattern: "message.*" },
      { type: "unlisten", id: 4, pattern: "message.*" },
      { type: "listen.ok", id: 5 },
      { type: "durable", meta: { aggregateID: "agg", seq: 1 }, event: durable("d") },
      { type: "resync", pattern: "message.*" },
      { type: "durable.ack", count: 0 },
      { type: "resync.ack" },
      { type: "resync.ack", pattern: "message.*" },
    ]
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    const seen: ControlFrame[] = []
    link.onMessage((frame) => seen.push(frame))
    for (const frame of valid) channel.port2.postMessage(frame)
    await pollWorker(() => seen.length === valid.length)
    expect(seen).toEqual(valid)
    expect(link.lastError).toBeUndefined()
    link.dispose()
    channel.port2.close()
  })

  test("passes an unknown forward-compatible frame through to the observer", async () => {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    const seen: unknown[] = []
    link.onMessage((frame) => seen.push(frame))
    channel.port2.postMessage({ type: "future.ping", payload: 1 })
    await pollWorker(() => seen.length === 1)
    expect(seen).toEqual([{ type: "future.ping", payload: 1 }])
    expect(link.lastError).toBeUndefined()
    link.dispose()
    channel.port2.close()
  })
})

describe("control link liveness", () => {
  test("a reply resolves the request", async () => {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    channel.port2.onmessage = (event: MessageEvent) => {
      const data: unknown = event.data
      if (typeof data !== "object" || data === null || !("id" in data)) return
      const { id } = data
      if (typeof id !== "number") return
      channel.port2.postMessage({ type: "pong", id })
    }
    channel.port2.start()
    const reply = await link.request({ type: "ping" })
    expect(reply.type).toBe("pong")
    link.dispose()
    channel.port2.close()
  })

  test("dispose rejects pending requests and refuses new ones", async () => {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    const pending = link.request({ type: "ping" })
    link.dispose()
    await expect(pending).rejects.toThrow(/disposed/)
    await expect(link.request({ type: "ping" })).rejects.toThrow(/disposed/)
    channel.port2.close()
  })

  test("an optional timeout rejects an unanswered request", async () => {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1, { timeoutMs: 20 })
    await expect(link.request({ type: "ping" })).rejects.toThrow(/timed out/)
    link.dispose()
    channel.port2.close()
  })

  test("typed ack/resync control frames are delivered to onMessage", async () => {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    const received: ControlFrame[] = []
    link.onMessage((frame) => received.push(frame))
    channel.port2.postMessage({ type: "durable.ack", count: 3 })
    channel.port2.postMessage({ type: "resync.ack", pattern: "message.*" })
    channel.port2.postMessage({ type: "resync.ack" })
    await pollWorker(() => received.length === 3)
    expect(received).toEqual([
      { type: "durable.ack", count: 3 },
      { type: "resync.ack", pattern: "message.*" },
      { type: "resync.ack" },
    ])
    link.dispose()
    channel.port2.close()
  })

  test("hub pushes and worker acks cross the control link in both directions", async () => {
    const channel = new MessageChannel()
    const hub = createControlLink(channel.port2)
    const worker = createControlLink(channel.port1)
    const atWorker: ControlFrame[] = []
    const atHub: ControlFrame[] = []
    worker.onMessage((frame) => atWorker.push(frame))
    hub.onMessage((frame) => atHub.push(frame))

    // Outbound (hub → worker): a resync push and a durable wake.
    hub.notify({ type: "resync", pattern: "message.*" })
    hub.notify({ type: "durable", meta: { aggregateID: "agg", seq: 1 }, event: durable("wake") })
    // Inbound (worker → hub): the matching acknowledgements.
    worker.notify({ type: "durable.ack", count: 2 })
    worker.notify({ type: "resync.ack", pattern: "message.*" })

    await pollWorker(() => atWorker.length === 2 && atHub.length === 2)
    expect(atWorker.map((frame) => frame.type)).toEqual(["resync", "durable"])
    expect(atHub.map((frame) => frame.type)).toEqual(["durable.ack", "resync.ack"])

    hub.dispose()
    worker.dispose()
    channel.port1.close()
    channel.port2.close()
  })
})
