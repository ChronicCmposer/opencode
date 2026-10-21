// Phase 9.2/9.3/9.4 — worker thin client over the hub registry (plan tasks 9.2, 9.3, 9.4).
//
// The hub registry (`job-registry.ts`) is one authority; each worker connects a
// `JobPort`. These tests wire several in-process port pairs to one registry and
// prove: a job started on worker A is visible to worker B (9.2); `wait` resolves
// across workers (9.3); and a settled child's result is pushed to the parent
// worker's client only (9.4). No threads.

import { describe, expect, test } from "bun:test"
import {
  createJobClient,
  createResultRouter,
  serveJobRegistry,
  type JobClient,
  type JobFrame,
  type JobPort,
} from "@/session/worker/job-client"
import { createJobRegistry, type JobInfo } from "@/session/worker/job-registry"
import type { BackgroundJobClientHandle } from "@/session/worker/layer"

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

describe("9.2 the thin client is a drop-in for the landed worker handle", () => {
  test("a JobClient structurally satisfies BackgroundJobClientHandle", () => {
    const ports = createPortPair()
    const client = createJobClient(ports.worker)
    // Compile-time proof only: `wait`/`cancel` are the handle's whole surface.
    const handle: BackgroundJobClientHandle = client
    expect(typeof handle.wait).toBe("function")
    expect(typeof handle.cancel).toBe("function")
    client.dispose()
  })
})

/** An in-process hub↔worker port pair; `hub.post` reaches the worker client. */
function createPortPair(): { hub: JobPort; worker: JobPort } {
  const hubListeners = new Set<(frame: JobFrame) => void>()
  const workerListeners = new Set<(frame: JobFrame) => void>()
  const deliver = (listeners: Set<(frame: JobFrame) => void>, frame: JobFrame): void => {
    for (const listener of listeners) listener(frame)
  }
  return {
    hub: {
      post: (frame) => deliver(workerListeners, frame),
      onMessage: (handler) => {
        hubListeners.add(handler)
        return () => hubListeners.delete(handler)
      },
    },
    worker: {
      post: (frame) => deliver(hubListeners, frame),
      onMessage: (handler) => {
        workerListeners.add(handler)
        return () => workerListeners.delete(handler)
      },
    },
  }
}

interface Hub {
  readonly clients: JobClient[]
  readonly ports: Array<{ hub: JobPort; worker: JobPort }>
  dispose(): void
}

/** One registry served on `count` worker ports. */
function createHub(registry: ReturnType<typeof createJobRegistry>, count: number): Hub {
  const ports = Array.from({ length: count }, () => createPortPair())
  const disposes = ports.map((pair) => serveJobRegistry(registry, pair.hub))
  const clients = ports.map((pair) => createJobClient(pair.worker))
  return { clients, ports, dispose: () => disposes.forEach((dispose) => dispose()) }
}

describe("9.2 worker thin client over the hub registry", () => {
  test("a job started on one worker is visible to another", async () => {
    const hub = createHub(createJobRegistry(), 2)
    const [workerA, workerB] = hub.clients

    await workerA.start({ id: "ses_job", type: "task" })
    expect((await workerB.get("ses_job"))?.status).toBe("running")
    expect((await workerB.list()).map((job) => job.id)).toEqual(["ses_job"])

    hub.dispose()
  })

  test("the thin client cancels a job started on another worker", async () => {
    const hub = createHub(createJobRegistry(), 2)
    const [workerA, workerB] = hub.clients

    await workerA.start({ id: "ses_job", type: "task" })
    const waiting = workerA.wait("ses_job")
    await workerB.cancel("ses_job")

    expect((await waiting).info?.status).toBe("cancelled")
    hub.dispose()
  })

  test("promotion on one worker wakes a waiter on another", async () => {
    const hub = createHub(createJobRegistry(), 2)
    const [workerA, workerB] = hub.clients

    await workerA.start({ id: "ses_job", type: "task" })
    const promoted = workerB.waitForPromotion("ses_job")
    await workerA.promote("ses_job")

    expect((await promoted).metadata?.background).toBe(true)
    expect(await workerB.extend("ses_missing")).toBe(false)
    hub.dispose()
  })
})

describe("9.3 background.wait resolves across workers", () => {
  test("worker B's wait settles when worker A completes the job", async () => {
    const hub = createHub(createJobRegistry(), 2)
    const [producer, consumer] = hub.clients

    await producer.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    const waiting = consumer.wait("ses_child")
    let settled = false
    void waiting.then(() => {
      settled = true
    })

    await flush()
    expect(settled).toBe(false)

    await producer.complete("ses_child", "cross-worker output")
    const result = await waiting
    expect(result.timedOut).toBe(false)
    expect(result.info?.output).toBe("cross-worker output")

    hub.dispose()
  })

  test("a wait for an already-finished job resolves immediately", async () => {
    const hub = createHub(createJobRegistry(), 1)
    const client = hub.clients[0]
    await client.start({ id: "ses_job", type: "task" })
    await client.complete("ses_job", "ready")
    const result = await client.wait("ses_job")
    expect(result.info?.output).toBe("ready")
    hub.dispose()
  })
})

describe("9.4 child result routed to the parent worker by parentID", () => {
  test("only the parent worker's client receives the child result", async () => {
    const ports = [createPortPair(), createPortPair(), createPortPair()]
    const registry = createJobRegistry({
      routeResult: createResultRouter((sessionID) => (sessionID === "ses_parent" ? ports[0].hub : undefined)),
    })
    const disposes = ports.map((pair) => serveJobRegistry(registry, pair.hub))
    const [parent, sibling, child] = ports.map((pair) => createJobClient(pair.worker))

    const parentGot: JobInfo[] = []
    const siblingGot: JobInfo[] = []
    parent.onChildResult((info) => parentGot.push(info))
    sibling.onChildResult((info) => siblingGot.push(info))

    await child.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
    await child.complete("ses_child", "done")
    await flush()

    expect(parentGot.map((info) => info.id)).toEqual(["ses_child"])
    expect(parentGot[0]?.output).toBe("done")
    expect(siblingGot).toEqual([])

    disposes.forEach((dispose) => dispose())
  })
})
