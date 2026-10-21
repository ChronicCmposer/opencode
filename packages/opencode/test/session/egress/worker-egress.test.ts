// Phase 8.1 + 8.13 — egress across a real worker isolate (plan tasks 8.1, 8.13).
//
// Spawns the egress fixture with `test/lib/worker-harness.ts`; the worker
// transfers a dedicated egress port and a separate control port. Proves frames
// cross the isolate over the dedicated port, control replies are not starved
// behind a delta flood, and hub-owned terminate flushes durable / drops
// non-durable / ends the stream.

import { describe, expect, test } from "bun:test"
import {
  createControlLink,
  type ControlFrame,
  type ControlInbound,
  type ControlOutbound,
} from "@/session/egress/control"
import { createEgressHub } from "@/session/egress/hub"
import { decodeFrame, encodeFrame, type EgressFrame } from "@/session/egress/frame"
import { createEgressReceiver } from "@/session/egress/receiver"
import { endFrame } from "@/session/worker/frame"
import { createWorkerHarness } from "../../lib/worker-harness"
import { deltasFrame, durableFrame } from "../../lib/egress-fixtures"

const WORKER_ENTRY = new URL("../../fixture/egress-worker.ts", import.meta.url)

interface PortsMessage {
  readonly type: "ports"
  readonly egress: MessagePort
  readonly control: MessagePort
}
interface FloodMessage {
  readonly type: "flood.done"
  readonly count: number
}
type WorkerOut = PortsMessage | FloodMessage
type WorkerIn = { readonly type: string; readonly id?: string; readonly ordinal?: number; readonly count?: number }

async function pollUntil(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

/**
 * Spawn the egress fixture worker and wire its ports to a hub. The returned
 * handle is `await using`-disposable so every test tears down the hub and the
 * worker isolate in one place instead of a hand-rolled `try/finally`.
 */
async function spawnEgressWorker() {
  const harness = createWorkerHarness({ cores: 1 })
  try {
    const worker = harness.spawn<WorkerOut, WorkerIn>(WORKER_ENTRY)
    const ports = await worker.waitFor("ports")
    const hub = createEgressHub({ egressPort: ports.egress, controlPort: ports.control })
    return {
      worker,
      hub,
      ports,
      async [Symbol.asyncDispose]() {
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
    await pollUntil(() => hub.frames === 1)
    expect(subscriber.drain()).toEqual([deltasFrame("w1")])
  })

  test("durable frames cross in commit order", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const subscriber = hub.fanout.subscribe("sse")
    worker.post({ type: "durable", id: "d1", ordinal: 1 })
    worker.post({ type: "durable", id: "d2", ordinal: 2 })
    await pollUntil(() => hub.frames === 2)
    const frames = subscriber.drain()
    expect(
      frames.map((frame) => (frame.kind === "event" ? [frame.payload.ordinal ?? -1, frame.payload.event.id] : [-1, ""])),
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
    await pollUntil(() => controlMessages.includes("flood.done"))
    await pollUntil(() => hub.frames > 0)
  })

  test("hub terminate drops buffered non-durable and flushes durable", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const subscriber = hub.fanout.subscribe("sse")
    worker.post({ type: "delta", id: "dropped" })
    worker.post({ type: "durable", id: "kept", ordinal: 1 })
    await pollUntil(() => hub.frames === 2)
    hub.close()

    expect(subscriber.drain()).toEqual([durableFrame("kept", 1), endFrame()])
  })

  test("a worker `end` command terminates the stream", async () => {
    await using session = await spawnEgressWorker()
    const { worker, hub } = session
    const frames: EgressFrame[] = []
    const consumer = (async () => {
      for await (const bytes of hub.stream("sse")) frames.push(decodeFrame(bytes))
    })()
    await pollUntil(() => hub.fanout.subscribers.length === 1)
    worker.post({ type: "end" })
    await consumer
    expect(frames).toEqual([endFrame()])
  })
})

describe("egress receiver framing boundaries", () => {
  test("accepts an ArrayBuffer chunk", async () => {
    const channel = new MessageChannel()
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: () => {} })
    const bytes = encodeFrame(endFrame())
    channel.port1.postMessage(bytes.buffer)
    await pollUntil(() => receiver.ended)
    expect(receiver.lastError).toBeUndefined()
    receiver.close()
    channel.port1.close()
  })

  test("accepts a typed-array view over a larger buffer", async () => {
    const channel = new MessageChannel()
    const frames: EgressFrame[] = []
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: (frame) => frames.push(frame) })
    const framed = encodeFrame(deltasFrame("v1"))
    const padded = new Uint8Array(framed.byteLength + 8)
    padded.set(framed, 4)
    const view = padded.subarray(4, 4 + framed.byteLength)
    channel.port1.postMessage(view)
    await pollUntil(() => frames.length === 1)
    expect(receiver.lastError).toBeUndefined()
    expect(frames[0]).toEqual(deltasFrame("v1"))
    receiver.close()
    channel.port1.close()
  })

  test("does not commit trailing bytes when a batch fails mid-way", async () => {
    const channel = new MessageChannel()
    const receiver = createEgressReceiver({ port: channel.port2, onFrame: () => {} })
    const first = encodeFrame(durableFrame("d2", 2))
    const stale = encodeFrame(durableFrame("d1", 1))
    const partial = first.subarray(0, first.byteLength - 2)
    channel.port1.postMessage(new Uint8Array([...first, ...stale, ...partial]))
    await pollUntil(() => receiver.lastError !== undefined)
    expect(receiver.pendingBytes).toBe(0)
    receiver.close()
    channel.port1.close()
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
    await pollUntil(() => received.length === 3)
    expect(received).toEqual([
      { type: "durable.ack", count: 3 },
      { type: "resync.ack", pattern: "message.*" },
      { type: "resync.ack" },
    ])
    link.dispose()
    channel.port2.close()
  })

  test("direction aliases accept their frames (hub pushes / worker acks)", () => {
    const outbound: ControlOutbound = { type: "resync", pattern: "message.*" }
    const inbound: ControlInbound = { type: "durable.ack", count: 2 }
    expect(outbound.type).toBe("resync")
    expect(inbound.type).toBe("durable.ack")
  })
})
