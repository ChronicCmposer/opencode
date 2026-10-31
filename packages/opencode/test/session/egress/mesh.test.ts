// EventV2 cross-isolate mesh.
//
// The hub registry keys subscriptions by `(worker, pattern)`; a worker `listen`
// request/reply flows over its control port, and matching durable commits wake
// the worker's `durable()` subscribers. Each worker's backlog is bounded by an
// OUTSTANDING-DELIVERY COUNTER: an un-acknowledged delivery occupies one slot,
// so a worker that keeps acknowledging keeps receiving. An over-full worker drops
// the commit and is told to `resync` (task 8.11).

import { describe, expect, test } from "bun:test"
import { createControlLink, type ControlFrame } from "@/session/egress/control"
import {
  MESH_SUBSCRIPTION_BOUND,
  attachMeshWorker,
  createEgressMesh,
  createMeshWorkerClient,
  type DurableWakeup,
  type MeshSink,
} from "@/session/egress/mesh"
import { pollUntil } from "../../lib/poll"

const wake = (seq: number): DurableWakeup => ({ aggregateID: "agg", seq })

/** A sink that records the durable ids and resync patterns it is pushed. */
function recordingSink(sent: string[], resyncs: string[] = []): MeshSink {
  return {
    sendDurable: (c) => sent.push(`durable:${c.seq}`),
    sendResync: (pattern) => resyncs.push(pattern),
  }
}

describe("event mesh registry", () => {
  test("the default backlog bound is 256", () => {
    expect(MESH_SUBSCRIPTION_BOUND).toBe(256)
  })

  test("delivers a matching commit to a listening worker", () => {
    const mesh = createEgressMesh()
    const sent: string[] = []
    mesh.attach("w1", recordingSink(sent))
    mesh.listen("w1", "p")
    const result = mesh.publish("w1", wake(1))
    expect(result).toEqual({ delivered: ["w1"], dropped: [] })
    expect(sent).toEqual(["durable:1"])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("does not deliver to a worker with no subscription", () => {
    const mesh = createEgressMesh()
    expect(mesh.publish("w1", wake(1))).toEqual({ delivered: [], dropped: [] })
    expect(mesh.outstanding("w1")).toBe(0)
  })

  test("delivers once per worker even with overlapping patterns", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "p")
    mesh.listen("w1", "*")
    expect(mesh.publish("w1", wake(1)).delivered).toEqual(["w1"])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("overflow flags a resync for EVERY overlapping pattern, carrying the dropped position", () => {
    const mesh = createEgressMesh({ bound: 1 })
    const resyncs: Array<{ pattern: string; meta?: { aggregateID: string; seq: number } }> = []
    mesh.attach("w1", {
      sendDurable: () => {},
      sendResync: (pattern, meta) => void resyncs.push({ pattern, ...(meta === undefined ? {} : { meta }) }),
    })
    mesh.listen("w1", "p")
    mesh.listen("w1", "*")
    mesh.publish("w1", wake(1))

    const result = mesh.publish("w1", wake(2))

    expect(result).toEqual({ delivered: [], dropped: ["w1"] })
    expect(resyncs).toEqual([
      { pattern: "p", meta: { aggregateID: "agg", seq: 2 } },
      { pattern: "*", meta: { aggregateID: "agg", seq: 2 } },
    ])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("addresses only the named worker", () => {
    const mesh = createEgressMesh()
    const a: string[] = []
    const b: string[] = []
    mesh.attach("w1", recordingSink(a))
    mesh.attach("w2", recordingSink(b))
    mesh.listen("w1", "p")
    mesh.listen("w2", "p")
    expect(mesh.publish("w1", wake(1)).delivered).toEqual(["w1"])
    expect(a).toEqual(["durable:1"])
    expect(b).toEqual([])
    expect(mesh.outstanding("w2")).toBe(0)
  })

  test("unlisten stops delivery and reclaims the worker entry", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "p")
    mesh.unlisten("w1", "p")
    expect(mesh.subscriptions()).toEqual([])
    // A later publish must not resurrect the reclaimed worker.
    expect(mesh.publish("w1", wake(1))).toEqual({ delivered: [], dropped: [] })
    expect(mesh.subscriptions()).toEqual([])
  })

  test("detach clears the worker's subscriptions and backlog", () => {
    const mesh = createEgressMesh()
    const detach = mesh.attach("w1", recordingSink([]))
    mesh.listen("w1", "p")
    mesh.publish("w1", wake(1))
    expect(mesh.outstanding("w1")).toBe(1)

    detach()
    expect(mesh.subscriptions()).toEqual([])
    expect(mesh.outstanding("w1")).toBe(0)
    expect(mesh.publish("w1", wake(2))).toEqual({ delivered: [], dropped: [] })
  })

  test("overflow drops the commit and flags resync instead of blocking", () => {
    const mesh = createEgressMesh({ bound: 1 })
    const resyncs: string[] = []
    mesh.attach("w1", recordingSink([], resyncs))
    mesh.listen("w1", "p")
    mesh.publish("w1", wake(1))
    const result = mesh.publish("w1", wake(2))
    expect(result).toEqual({ delivered: [], dropped: ["w1"] })
    expect(mesh.resyncPending).toEqual(["w1"])
    expect(resyncs).toEqual(["p"])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("ack lowers the backlog so delivery resumes", () => {
    const mesh = createEgressMesh({ bound: 1 })
    mesh.listen("w1", "p")
    mesh.publish("w1", wake(1))
    mesh.ack("w1")
    expect(mesh.outstanding("w1")).toBe(0)
    expect(mesh.publish("w1", wake(2)).delivered).toEqual(["w1"])
  })

  test("tracks resync per pattern and acknowledges them individually", () => {
    const mesh = createEgressMesh({ bound: 1 })
    mesh.listen("w1", "a.*")
    mesh.listen("w1", "b.*")
    mesh.publish("w1", wake(1)) // occupies the only slot
    mesh.publish("w1", wake(2)) // overflow -> resync b.*
    mesh.publish("w1", wake(3)) // overflow -> resync a.*
    expect(mesh.resyncPending).toEqual(["w1"])
    mesh.acknowledgeResync("w1", "b.*")
    expect(mesh.resyncPending).toEqual(["w1"])
    mesh.acknowledgeResync("w1", "a.*")
    expect(mesh.resyncPending).toEqual([])
  })

  test("the attached sink receives durable and resync pushes", () => {
    const mesh = createEgressMesh()
    const sent: string[] = []
    mesh.attach("w1", recordingSink(sent))
    mesh.listen("w1", "*")
    mesh.publish("w1", wake(1))
    expect(sent).toEqual(["durable:1"])
  })
})

describe("event mesh transport over a control port", () => {
  test("listen request/reply, durable wake, overflow resync, acknowledge", async () => {
    const mesh = createEgressMesh({ bound: 1 })
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    const client = createMeshWorkerClient(channel.port1)

    const woken: number[] = []
    const resyncs: Array<{ pattern: string; meta?: { aggregateID: string; seq: number } }> = []
    client.onDurable((c) => woken.push(c.seq))
    client.onResync((pattern, meta) => resyncs.push({ pattern, ...(meta === undefined ? {} : { meta }) }))

    await client.listen("p")
    expect(mesh.subscriptions()).toEqual([{ workerId: "w1", pattern: "p" }])

    mesh.publish("w1", wake(1))
    await pollUntil(() => woken.length === 1)
    expect(woken).toEqual([1])

    // Backlog bound is 1 and e1 is still un-acknowledged → the second commit overflows.
    mesh.publish("w1", wake(2))
    await pollUntil(() => resyncs.length === 1)
    expect(resyncs).toEqual([{ pattern: "p", meta: { aggregateID: "agg", seq: 2 } }])
    expect(mesh.resyncPending).toEqual(["w1"])

    client.acknowledgeResync("p")
    await pollUntil(() => mesh.resyncPending.length === 0)

    client.dispose()
    attachment.dispose()
  })

  test("a worker keeps receiving after the bound once it acknowledges deliveries", async () => {
    const mesh = createEgressMesh({ bound: 1 })
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    const client = createMeshWorkerClient(channel.port1)

    const woken: number[] = []
    client.onDurable((c) => woken.push(c.seq))
    await client.listen("*")

    // Without a routed ack this wedges at the bound; with it, every commit lands.
    for (let ordinal = 1; ordinal <= 3; ordinal++) {
      mesh.publish("w1", wake(ordinal))
      await pollUntil(() => woken.length === ordinal)
      client.acknowledgeDelivery()
      await pollUntil(() => mesh.outstanding("w1") === 0)
    }

    expect(woken).toEqual([1, 2, 3])

    client.dispose()
    attachment.dispose()
  })

  test("the worker client can be driven end to end (listen -> wake -> ack)", async () => {
    const mesh = createEgressMesh()
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    const client = createMeshWorkerClient(channel.port1)

    const woken: number[] = []
    client.onDurable((c) => woken.push(c.seq))
    await client.listen("p")
    expect(mesh.subscriptions()).toEqual([{ workerId: "w1", pattern: "p" }])

    mesh.publish("w1", wake(1))
    await pollUntil(() => woken.length === 1)
    expect(mesh.outstanding("w1")).toBe(1)

    client.acknowledgeDelivery()
    await pollUntil(() => mesh.outstanding("w1") === 0)

    client.dispose()
    attachment.dispose()
  })
})

describe("mesh durable.ack count validation", () => {
  test("acknowledgeDelivery rejects a non-negative-integer count", () => {
    const channel = new MessageChannel()
    const client = createMeshWorkerClient(channel.port1)
    expect(() => client.acknowledgeDelivery(-1)).toThrow(/count/)
    expect(() => client.acknowledgeDelivery(1.5)).toThrow(/count/)
    client.dispose()
    channel.port2.close()
  })

  test("the control boundary drops a malformed durable.ack and records an error", async () => {
    const channel = new MessageChannel()
    const link = createControlLink(channel.port1)
    const seen: ControlFrame[] = []
    link.onMessage((frame) => seen.push(frame))
    channel.port2.postMessage({ type: "durable.ack", count: -1 })
    channel.port2.postMessage({ type: "durable.ack", count: 2 })
    await pollUntil(() => seen.length === 1)
    expect(seen).toEqual([{ type: "durable.ack", count: 2 }])
    expect(link.lastError?.message).toMatch(/invalid control frame/)
    link.dispose()
    channel.port2.close()
  })

  test("a malformed durable.ack cannot inflate the hub backlog", async () => {
    const mesh = createEgressMesh()
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    mesh.listen("w1", "*")
    mesh.publish("w1", wake(1))
    expect(mesh.outstanding("w1")).toBe(1)

    // A negative count would otherwise INCREASE the backlog (1 - (-5) = 6).
    channel.port1.postMessage({ type: "durable.ack", count: -5 })
    channel.port1.postMessage({ type: "durable.ack", count: 1 })
    await pollUntil(() => mesh.outstanding("w1") === 0)

    attachment.dispose()
  })
})

describe("mesh delivery failure handling", () => {
  const closedSink: MeshSink = {
    sendDurable: () => {
      throw new Error("port closed")
    },
    sendResync: () => {},
  }

  test("a failed durable push does not inflate outstanding or claim a delivery", () => {
    const mesh = createEgressMesh()
    mesh.attach("w1", closedSink)
    mesh.listen("w1", "*")
    const result = mesh.publish("w1", wake(1))
    expect(result).toEqual({ delivered: [], dropped: [] })
    expect(mesh.outstanding("w1")).toBe(0)
  })

  test("a failed resync push still flags the pending resync without throwing", () => {
    const mesh = createEgressMesh({ bound: 1 })
    mesh.attach("w1", {
      sendDurable: () => {},
      sendResync: () => {
        throw new Error("port closed")
      },
    })
    mesh.listen("w1", "*")
    mesh.publish("w1", wake(1))
    expect(() => mesh.publish("w1", wake(2))).not.toThrow()
    expect(mesh.resyncPending).toEqual(["w1"])
  })
})

describe("control link notify guard", () => {
  test("notify surfaces a closed-port failure and records it as the last error", () => {
    const throwingPort = {
      onmessage: null,
      start: () => {},
      close: () => {},
      postMessage: () => {
        throw new Error("port closed")
      },
      addEventListener: () => {},
      removeEventListener: () => {},
    } as unknown as MessagePort
    const link = createControlLink(throwingPort)
    expect(() => link.notify({ type: "resync", pattern: "*" })).toThrow(/port closed/)
    expect(link.lastError?.message).toMatch(/port closed/)
    link.dispose()
  })
})
