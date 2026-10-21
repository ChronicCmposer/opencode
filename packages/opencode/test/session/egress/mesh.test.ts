// Phase 8.9 + 8.10 + 8.11 — EventV2 cross-isolate mesh (plan tasks 8.9–8.11).
//
// The hub registry keys subscriptions by `(worker, pattern)`; a worker `listen`
// request/reply flows over its control port, and matching durable commits wake
// the worker's `durable()` subscribers. Each worker's backlog is bounded, and an
// overflow drops the commit then instructs a `resync`.

import { describe, expect, test } from "bun:test"
import {
  MESH_SUBSCRIPTION_BOUND,
  attachMeshWorker,
  createEgressMesh,
  createMeshWorkerClient,
  matchesMeshPattern,
  type DurableCommit,
} from "@/session/egress/mesh"
import type { EgressEvent } from "@/session/egress/frame"

const commit = (id: string, type: string, ordinal: number): DurableCommit => ({
  meta: { generation: 1, ordinal },
  event: { id, type, data: { id } } as EgressEvent,
})

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
    mesh.listen("w1", "session.*")
    const result = mesh.publish(commit("e1", "session.message", 1))
    expect(result).toEqual({ delivered: ["w1"], dropped: [] })
    expect(mesh.depth("w1")).toBe(1)
    expect(mesh.drain("w1")).toEqual([commit("e1", "session.message", 1)])
  })

  test("does not deliver to a non-matching subscription", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    expect(mesh.publish(commit("e1", "message.updated", 1))).toEqual({ delivered: [], dropped: [] })
    expect(mesh.depth("w1")).toBe(0)
  })

  test("delivers once per worker even with overlapping patterns", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    mesh.listen("w1", "*")
    expect(mesh.publish(commit("e1", "session.message", 1)).delivered).toEqual(["w1"])
    expect(mesh.depth("w1")).toBe(1)
  })

  test("fans out to every subscribed worker", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    mesh.listen("w2", "session.*")
    expect([...mesh.publish(commit("e1", "session.message", 1)).delivered].sort()).toEqual(["w1", "w2"])
  })

  test("unlisten stops delivery", () => {
    const mesh = createEgressMesh()
    mesh.listen("w1", "session.*")
    mesh.unlisten("w1", "session.*")
    expect(mesh.publish(commit("e1", "session.message", 1))).toEqual({ delivered: [], dropped: [] })
  })

  test("overflow drops the commit and flags resync instead of blocking", () => {
    const mesh = createEgressMesh({ bound: 1 })
    const resyncs: string[] = []
    mesh.attach("w1", { sendDurable: () => {}, sendResync: (pattern) => resyncs.push(pattern) })
    mesh.listen("w1", "session.*")
    mesh.publish(commit("e1", "session.message", 1))
    const result = mesh.publish(commit("e2", "session.message", 2))
    expect(result).toEqual({ delivered: [], dropped: ["w1"] })
    expect(mesh.resyncPending).toEqual(["w1"])
    expect(resyncs).toEqual(["session.*"])
    expect(mesh.depth("w1")).toBe(1)
  })

  test("ack lowers the backlog so delivery resumes", () => {
    const mesh = createEgressMesh({ bound: 1 })
    mesh.listen("w1", "session.*")
    mesh.publish(commit("e1", "session.message", 1))
    mesh.ack("w1")
    expect(mesh.depth("w1")).toBe(0)
    expect(mesh.publish(commit("e2", "session.message", 2)).delivered).toEqual(["w1"])
  })

  test("the attached sink receives durable and resync pushes", () => {
    const mesh = createEgressMesh()
    const sent: string[] = []
    mesh.attach("w1", {
      sendDurable: (c) => sent.push(`durable:${c.event.id}`),
      sendResync: (pattern) => sent.push(`resync:${pattern}`),
    })
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

    // Backlog bound is 1 → the second commit overflows and resyncs.
    mesh.publish(commit("e2", "session.message", 2))
    await pollUntil(() => resyncs.length === 1)
    expect(resyncs).toEqual(["session.*"])
    expect(mesh.resyncPending).toEqual(["w1"])

    client.acknowledgeResync("session.*")
    await pollUntil(() => mesh.resyncPending.length === 0)

    client.dispose()
    attachment.dispose()
  })
})
