// Phase 8.1 + 8.13 — egress across a real worker isolate (plan tasks 8.1, 8.13).
//
// Spawns the egress fixture with `test/lib/worker-harness.ts`; the worker
// transfers a dedicated egress port and a separate control port. Proves frames
// cross the isolate over the dedicated port, control replies are not starved
// behind a delta flood, and hub-owned terminate flushes durable / drops
// non-durable / ends the stream.

import { describe, expect, test } from "bun:test"
import { createEgressHub } from "@/session/egress/hub"
import { decodeFrame, type EgressEvent, type EgressFrame } from "@/session/egress/frame"
import { createWorkerHarness } from "../../lib/worker-harness"

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

const delta = (id: string): EgressEvent => ({ id, type: "message.part.updated", data: { id } })
const durable = (id: string): EgressEvent => ({ id, type: "message.updated", data: { id } })

async function pollUntil(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

async function spawnEgressWorker() {
  const harness = createWorkerHarness({ cores: 1 })
  const worker = harness.spawn<WorkerOut, WorkerIn>(WORKER_ENTRY)
  const ports = await worker.waitFor("ports")
  const hub = createEgressHub({ egressPort: ports.egress, controlPort: ports.control })
  return { harness, worker, hub, ports }
}

describe("egress across a worker isolate", () => {
  test("a worker delta reaches the hub over the dedicated egress port", async () => {
    const { harness, worker, hub, ports } = await spawnEgressWorker()
    try {
      expect(ports.egress).not.toBe(ports.control)
      const subscriber = hub.fanout.subscribe("sse")
      worker.post({ type: "delta", id: "w1" })
      await pollUntil(() => hub.frames === 1)
      expect(subscriber.drain()).toEqual([{ kind: "deltas", v: 1, events: [delta("w1")] }])
    } finally {
      hub.close()
      await harness.dispose()
    }
  })

  test("durable frames cross in commit order", async () => {
    const { harness, worker, hub } = await spawnEgressWorker()
    try {
      const subscriber = hub.fanout.subscribe("sse")
      worker.post({ type: "durable", id: "d1", ordinal: 1 })
      worker.post({ type: "durable", id: "d2", ordinal: 2 })
      await pollUntil(() => hub.frames === 2)
      const frames = subscriber.drain()
      expect(
        frames.map((frame) => (frame.kind === "durable" ? [frame.meta.ordinal, frame.event.id] : [-1, ""])),
      ).toEqual([
        [1, "d1"],
        [2, "d2"],
      ])
    } finally {
      hub.close()
      await harness.dispose()
    }
  })

  test("the control port answers while a delta flood is in flight", async () => {
    const { harness, worker, hub } = await spawnEgressWorker()
    try {
      const controlMessages: string[] = []
      hub.control!.onMessage((frame) => controlMessages.push(frame.type))
      // Flood the worker with deltas, then immediately ping on the control port.
      worker.post({ type: "flood", count: 5000 })
      const pong = await hub.control!.request({ type: "ping" })
      expect(pong.type).toBe("pong")

      // Both channels make progress: the flood completes and egress delivers.
      await pollUntil(() => controlMessages.includes("flood.done"))
      await pollUntil(() => hub.frames > 0)
    } finally {
      hub.close()
      await harness.dispose()
    }
  })

  test("hub terminate drops buffered non-durable and flushes durable", async () => {
    const { harness, worker, hub } = await spawnEgressWorker()
    try {
      const subscriber = hub.fanout.subscribe("sse")
      worker.post({ type: "delta", id: "dropped" })
      worker.post({ type: "durable", id: "kept", ordinal: 1 })
      await pollUntil(() => hub.frames === 2)
      hub.close()

      expect(subscriber.drain()).toEqual([
        { kind: "durable", v: 1, meta: { generation: 1, ordinal: 1 }, event: durable("kept") },
        { kind: "end", v: 1 },
      ])
    } finally {
      await harness.dispose()
    }
  })

  test("a worker `end` command terminates the stream", async () => {
    const { harness, worker, hub } = await spawnEgressWorker()
    try {
      const frames: EgressFrame[] = []
      const consumer = (async () => {
        for await (const bytes of hub.stream("sse")) frames.push(decodeFrame(bytes))
      })()
      await pollUntil(() => hub.fanout.subscribers.length === 1)
      worker.post({ type: "end" })
      await consumer
      expect(frames).toEqual([{ kind: "end", v: 1 }])
    } finally {
      hub.close()
      await harness.dispose()
    }
  })
})
