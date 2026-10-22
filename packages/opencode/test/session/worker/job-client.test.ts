// Phase 9.2/9.3/9.4 — worker thin client over the hub registry (plan tasks 9.2, 9.3, 9.4).
//
// The hub registry (`job-registry.ts`) is one authority; each worker connects a
// `JobPort`. These tests wire several in-process port pairs to one registry and
// prove: a job started on worker A is visible to worker B (9.2); `wait` resolves
// across workers (9.3); and a settled child's result is pushed to the parent
// worker's client only (9.4). No threads.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { BackgroundJob } from "@/background/job"
import {
  createBackgroundJobClient,
  createJobClient,
  createResultRouter,
  jobPortFromMessagePort,
  serveJobRegistry,
  type JobClient,
  type JobFrame,
  type JobPort,
} from "@/session/worker/job-client"
import { createJobRegistry, type JobInfo } from "@/session/worker/job-registry"
import type { BackgroundJobClientHandle } from "@/session/worker/layer"
import { flush } from "../../lib/async"
import { createListenerPair, type ListenerPair } from "../../lib/port-pair"

describe("9.2 the thin client is a drop-in for the landed worker handle", () => {
  test("a JobClient structurally satisfies BackgroundJobClientHandle", () => {
    const ports = createListenerPair<JobFrame>()
    const client = createJobClient(ports.worker)
    // Compile-time proof only: `wait`/`cancel` are the handle's whole surface.
    const handle: BackgroundJobClientHandle = client
    expect(typeof handle.wait).toBe("function")
    expect(typeof handle.cancel).toBe("function")
    client.dispose()
  })
})

interface Hub {
  readonly clients: JobClient[]
  readonly ports: ListenerPair<JobFrame>[]
  dispose(): void
}

/** One registry served on `count` worker ports. */
function createHub(registry: ReturnType<typeof createJobRegistry>, count: number): Hub {
  const ports = Array.from({ length: count }, () => createListenerPair<JobFrame>())
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

describe("job client lifecycle (dispose / timeout / server faults)", () => {
  test("dispose rejects pending requests and fails later requests fast", async () => {
    const ports = createListenerPair<JobFrame>()
    const client = createJobClient(ports.worker)
    const pending = client.get("ses_none")
    client.dispose()
    await expect(pending).rejects.toThrow("job client disposed")
    await expect(client.get("ses_none")).rejects.toThrow("job client disposed")
  })

  test("a request timeout rejects and drops its pending entry", async () => {
    const ports = createListenerPair<JobFrame>()
    const client = createJobClient(ports.worker, { timeoutMs: 10 })
    await expect(client.get("ses_none")).rejects.toThrow(/timed out after 10ms/)
    client.dispose()
  })

  test("serveJobRegistry replies with an error when the registry rejects", async () => {
    const ports = createListenerPair<JobFrame>()
    const registry = createJobRegistry()
    const failing: typeof registry = {
      ...registry,
      wait: () => Promise.reject(new Error("wait failed")),
      waitForPromotion: () => Promise.reject(new Error("promotion failed")),
    }
    serveJobRegistry(failing, ports.hub)
    const client = createJobClient(ports.worker)

    await expect(client.wait("ses_x")).rejects.toThrow("wait failed")
    await expect(client.waitForPromotion("ses_x")).rejects.toThrow("promotion failed")
    client.dispose()
  })

  test("a reply post failure does not crash the server", () => {
    const handlers = new Set<(frame: JobFrame) => void>()
    const port: JobPort = {
      post: () => {
        throw new Error("port closed")
      },
      onMessage: (handler) => {
        handlers.add(handler)
        return () => handlers.delete(handler)
      },
    }
    serveJobRegistry(createJobRegistry(), port)
    expect(() => {
      for (const handler of handlers) handler({ kind: "request", id: 1, op: "list" })
    }).not.toThrow()
  })
})

describe("9.4 child result routed to the parent worker by parentID", () => {
  test("only the parent worker's client receives the child result", async () => {
    const ports = [createListenerPair<JobFrame>(), createListenerPair<JobFrame>(), createListenerPair<JobFrame>()]
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

describe("21 the port adapter exposes BackgroundJobClientHandle", () => {
  test("createBackgroundJobClient structurally satisfies BackgroundJobClientHandle", () => {
    const channel = new MessageChannel()
    const client = createBackgroundJobClient(channel.port2)
    // Compile-time proof only: `wait`/`cancel` are the handle's whole surface.
    const handle: BackgroundJobClientHandle = client
    expect(typeof handle.wait).toBe("function")
    expect(typeof handle.cancel).toBe("function")
    client.dispose()
  })

  test("wait resolves over a real MessagePort once the job completes", async () => {
    const channel = new MessageChannel()
    const registry = createJobRegistry()
    const disposeServer = serveJobRegistry(registry, jobPortFromMessagePort(channel.port1))
    const handle = createBackgroundJobClient(channel.port2)

    registry.start({ id: "ses_job", type: "task" })
    const waiting = handle.wait("ses_job")
    registry.complete("ses_job", "adapter output")

    const result = await waiting
    expect(result.timedOut).toBe(false)
    expect(result.info?.output).toBe("adapter output")

    handle.dispose()
    disposeServer()
  })

  test("cancel settles a concurrent wait as cancelled", async () => {
    const channel = new MessageChannel()
    const registry = createJobRegistry()
    const disposeServer = serveJobRegistry(registry, jobPortFromMessagePort(channel.port1))
    const handle = createBackgroundJobClient(channel.port2)

    registry.start({ id: "ses_job", type: "task" })
    const waiting = handle.wait("ses_job")
    await handle.cancel("ses_job")

    expect((await waiting).info?.status).toBe("cancelled")

    handle.dispose()
    disposeServer()
  })
})

describe("9.6 clientLayer ships parentID over the wire to the registry", () => {
  test("a parentID handed to the worker BackgroundJob reaches the hub registry", async () => {
    const registry = createJobRegistry()
    const hub = createHub(registry, 1)
    const client = hub.clients[0]

    await Effect.runPromise(
      Effect.gen(function* () {
        const jobs = yield* BackgroundJob.Service
        const info = yield* jobs.start({
          id: "ses_child",
          type: "task",
          parentID: "ses_parent",
          run: Effect.succeed("done"),
        })
        yield* jobs.wait({ id: info.id, timeout: 2_000 })
      }).pipe(Effect.provide(BackgroundJob.clientLayer(client))),
    )

    expect(registry.get("ses_child")?.parentID).toBe("ses_parent")
    expect(registry.childrenOf("ses_parent")).toEqual(["ses_child"])
    expect(registry.parentOf("ses_child")).toBe("ses_parent")
    hub.dispose()
  })
})
