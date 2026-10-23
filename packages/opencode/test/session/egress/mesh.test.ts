// Phase 8.9 + 8.10 + 8.11 — EventV2 cross-isolate mesh (plan tasks 8.9–8.11).
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
  matchesMeshPattern,
  type DurableCommit,
  type MeshSink,
} from "@/session/egress/mesh"
import { durable } from "../../lib/egress-fixtures"

const commit = (id: string, type: string, ordinal: number): DurableCommit => ({
  meta: { generation: 1, ordinal },
  event: durable(id, type),
})

/** A sink that records the durable ids and resync patterns it is pushed. */
function recordingSink(sent: string[], resyncs: string[] = []): MeshSink {
  return {
    sendDurable: (c) => sent.push(`durable:${c.event.id}`),
    sendResync: (pattern) => resyncs.push(pattern),
  }
}

async function pollUntil(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`pollUntil timed out after ${timeoutMs}ms`)
}

describe("event mesh registry", () => {
  test("matches exact, trailing-wildcard and catch-all patterns", () => {
    expect(matchesMeshPattern("*", "anything")).toBe(true)
    expect(matchesMeshPattern("session.*", "session.message")).toBe(true)
    expect(matchesMeshPattern("session.*", "message.session")).toBe(false)
    expect(matchesMeshPattern("session.message", "session.message")).toBe(true)
    expect(matchesMeshPattern("session.message", "session.other")).toBe(false)
  })

  test("the default backlog bound is 256", () => {
    expect(MESH_SUBSCRIPTION_BOUND).toBe(256)
  })

  test("delivers a matching commit to a listening worker", () => {
    const mesh = createEgressMesh()
    const sent: string[] = []
    mesh.attach("w1", recordingSink(sent))
    mesh.listen("w1", "session.*")
    const result = mesh.publish(commit("e1", "session.message", 1))
    expect(result).toEqual({ delivered: ["w1"], dropped: [] })
    expect(sent).toEqual(["durable:e1"])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("does not deliver to a non-matching subscription", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    expect(mesh.publish(commit("e1", "message.updated", 1))).toEqual({ delivered: [], dropped: [] })
    expect(mesh.outstanding("w1")).toBe(0)
  })

  test("delivers once per worker even with overlapping patterns", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    mesh.listen("w1", "*")
    expect(mesh.publish(commit("e1", "session.message", 1)).delivered).toEqual(["w1"])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("fans out to every subscribed worker", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    mesh.listen("w2", "session.*")
    expect([...mesh.publish(commit("e1", "session.message", 1)).delivered].sort()).toEqual(["w1", "w2"])
  })

  test("unlisten stops delivery and reclaims the worker entry", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    mesh.unlisten("w1", "session.*")
    expect(mesh.subscriptions()).toEqual([])
    // A later publish must not resurrect the reclaimed worker.
    expect(mesh.publish(commit("e1", "session.message", 1))).toEqual({ delivered: [], dropped: [] })
    expect(mesh.subscriptions()).toEqual([])
  })

  test("detach clears the worker's subscriptions and backlog", () => {
    const mesh = createEgressMesh()
    const detach = mesh.attach("w1", recordingSink([]))
    mesh.listen("w1", "session.*")
    mesh.publish(commit("e1", "session.message", 1))
    expect(mesh.outstanding("w1")).toBe(1)

    detach()
    expect(mesh.subscriptions()).toEqual([])
    expect(mesh.outstanding("w1")).toBe(0)
    expect(mesh.publish(commit("e2", "session.message", 2))).toEqual({ delivered: [], dropped: [] })
  })

  test("overflow drops the commit and flags resync instead of blocking", () => {
    const mesh = createEgressMesh({ bound: 1 })
    const resyncs: string[] = []
    mesh.attach("w1", recordingSink([], resyncs))
    mesh.listen("w1", "session.*")
    mesh.publish(commit("e1", "session.message", 1))
    const result = mesh.publish(commit("e2", "session.message", 2))
    expect(result).toEqual({ delivered: [], dropped: ["w1"] })
    expect(mesh.resyncPending).toEqual(["w1"])
    expect(resyncs).toEqual(["session.*"])
    expect(mesh.outstanding("w1")).toBe(1)
  })

  test("ack lowers the backlog so delivery resumes", () => {
    const mesh = createEgressMesh({ bound: 1 })
    mesh.listen("w1", "session.*")
    mesh.publish(commit("e1", "session.message", 1))
    mesh.ack("w1")
    expect(mesh.outstanding("w1")).toBe(0)
    expect(mesh.publish(commit("e2", "session.message", 2)).delivered).toEqual(["w1"])
  })

  test("tracks resync per pattern and acknowledges them individually", () => {
    const mesh = createEgressMesh({ bound: 1 })
    mesh.listen("w1", "a.*")
    mesh.listen("w1", "b.*")
    mesh.publish(commit("a1", "a.event", 1)) // occupies the only slot
    mesh.publish(commit("b1", "b.event", 2)) // overflow -> resync b.*
    mesh.publish(commit("a2", "a.event", 3)) // overflow -> resync a.*
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
    mesh.publish(commit("e1", "session.message", 1))
    expect(sent).toEqual(["durable:e1"])
  })
})

describe("event mesh transport over a control port", () => {
  test("listen request/reply, durable wake, overflow resync, acknowledge", async () => {
    const mesh = createEgressMesh({ bound: 1 })
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    const client = createMeshWorkerClient(channel.port1)

    const woken: string[] = []
    const resyncs: string[] = []
    client.onDurable((c) => woken.push(c.event.id))
    client.onResync((pattern) => resyncs.push(pattern))

    await client.listen("session.*")
    expect(mesh.subscriptions()).toEqual([{ workerId: "w1", pattern: "session.*" }])

    mesh.publish(commit("e1", "session.message", 1))
    await pollUntil(() => woken.length === 1)
    expect(woken).toEqual(["e1"])

    // Backlog bound is 1 and e1 is still un-acknowledged → the second commit overflows.
    mesh.publish(commit("e2", "session.message", 2))
    await pollUntil(() => resyncs.length === 1)
    expect(resyncs).toEqual(["session.*"])
    expect(mesh.resyncPending).toEqual(["w1"])

    client.acknowledgeResync("session.*")
    await pollUntil(() => mesh.resyncPending.length === 0)

    client.dispose()
    attachment.dispose()
  })

  test("a worker keeps receiving after the bound once it acknowledges deliveries", async () => {
    const mesh = createEgressMesh({ bound: 1 })
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    const client = createMeshWorkerClient(channel.port1)

    const woken: string[] = []
    client.onDurable((c) => woken.push(c.event.id))
    await client.listen("*")

    // Without a routed ack this wedges at the bound; with it, every commit lands.
    for (let ordinal = 1; ordinal <= 3; ordinal++) {
      mesh.publish(commit(`e${ordinal}`, "session.message", ordinal))
      await pollUntil(() => woken.length === ordinal)
      client.acknowledgeDelivery()
      await pollUntil(() => mesh.outstanding("w1") === 0)
    }

    expect(woken).toEqual(["e1", "e2", "e3"])

    client.dispose()
    attachment.dispose()
  })

  test("the worker client can be driven end to end (listen -> wake -> ack)", async () => {
    const mesh = createEgressMesh()
    const channel = new MessageChannel()
    const attachment = attachMeshWorker(mesh, "w1", channel.port2)
    const client = createMeshWorkerClient(channel.port1)

    const woken: string[] = []
    client.onDurable((c) => woken.push(c.event.id))
    await client.listen("session.*")
    expect(mesh.subscriptions()).toEqual([{ workerId: "w1", pattern: "session.*" }])

    mesh.publish(commit("e1", "session.message", 1))
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
    mesh.publish(commit("e1", "session.message", 1))
    expect(mesh.outstanding("w1")).toBe(1)

    // A negative count would otherwise INCREASE the backlog (1 - (-5) = 6).
    channel.port1.postMessage({ type: "durable.ack", count: -5 })
    channel.port1.postMessage({ type: "durable.ack", count: 1 })
    await pollUntil(() => mesh.outstanding("w1") === 0)

    attachment.dispose()
  })
})
