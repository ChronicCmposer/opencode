// Phase 12.1–12.4 — pure hub, admission & HTTP/SSE liveness.
//
// The hub is the composition of the Phase 4 dispatcher (scheduling, admission,
// headroom, crash handling), the Phase 5 executor pool, and the Phase 11
// cooperative cancel path. These tests drive the hub over REAL Bun worker
// isolates and assert the four Phase 12 properties:
//
//   12.1 the hub executes zero sessions (work runs in a worker isolate);
//   12.2 admission admits K + headroom respecting depth; the awaiting parent
//        retains its slot;
//   12.3 the hub loop stays I/O-only while K sessions run;
//   12.4 HTTP ingress + SSE stay functional while K sessions run.

import { describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Layer, ManagedRuntime, Option } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import type { SessionID } from "@/session/schema"
import {
  buildProductionPortBackends,
  createBunExecutorTransport,
  createBunExecutorWorkerPool,
  createHubEventSink,
  createLazyExecutorWorkerPool,
  createProductionWorkerPool,
  createSessionBoundExecutorWorker,
  createSessionHub,
  createSessionHubLayer,
  createWorkerCancellation,
  fallbackInstanceContext,
  lazyOpenWriter,
  makeSessionHubNode,
  productionSessionHubLayer,
  productionSessionHubNode,
  resolveSessionWorkerEntry,
  serveEventsPort,
  SESSION_CANCELLED_TYPE,
  SessionHubService,
  type ProductionPoolFactory,
  type SessionHub,
  type WorkerPortBackends,
  type WorkerTransferredPorts,
} from "@/session/worker/hub"
import { endFrame, eventFrame } from "@/session/worker/frame"
import { encodeFrame } from "@/session/egress/frame"
import { createLiveEventPublisher, EventV2Bridge, type LiveEvent, type LiveEventPublisher } from "@/event-v2-bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { GlobalBus } from "@/bus/global"
import { SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
import { createJobClient, jobPortFromMessagePort, type JobPort } from "@/session/worker/job-client"
import type {
  ExecutorRun,
  ExecutorSessionWorker,
  ExecutorTransport,
  ExecutorWorker,
  ExecutorWorkerPool,
  SessionExecutionResult,
  SessionExecutorInbound,
  SessionExecutorOutbound,
} from "@/session/worker/execute"
import type { CancelFrame, CancelPort, CancelScheduler } from "@/session/worker/cancel"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { createPidRegistry, registerReported } from "@/session/worker/pid-registry"
import { createWorkerPluginHost } from "@/session/worker/plugin-host"
import { sessionWorkerInit, assertStructuredCloneable } from "../../lib/session-worker-init"
import { Session } from "@/session/session"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { sid } from "../../lib/async"
import { pollUntil } from "../../lib/poll"
import { workerSnapshot } from "../../lib/snapshot"
import { createHubProxyClient } from "@/session/worker/layer"
import { createWorkerWriterClient } from "@/session/worker/writer-client"
import { createWorkerBrokerClient } from "@/session/worker/broker-client"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { createEgressReceiver } from "@/session/egress/receiver"
import type { BrokerBackend } from "@/session/broker/broker"
import { serveWriterPort, type HubWriter, type WriterClient } from "@/session/writer/client"
import { sessionWorkerNodes } from "@/session/worker/nodes"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import path from "node:path"
import { tmpdir } from "../../fixture/fixture"
import { healthyEnv } from "../writer/fixtures"

const it = testEffect(TestLLMServer.layer)

/** The dependency-light executor fixture (Phase 5). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)
/** The production worker entry (reduced layer as the work factory). */
const SESSION_WORKER = new URL("../../../src/session/worker/session-worker.ts", import.meta.url)

/** Transaction methods for writer stubs whose tests never exercise them; fail loud if reached. */
const unusedHubWriterTx: Pick<HubWriter, "begin" | "txQuery" | "commit" | "rollback"> = {
  begin: async () => {
    throw new Error("begin was not expected in this test")
  },
  txQuery: async () => {
    throw new Error("txQuery was not expected in this test")
  },
  commit: async () => {
    throw new Error("commit was not expected in this test")
  },
  rollback: async () => {
    throw new Error("rollback was not expected in this test")
  },
}

describe("12.1 the hub executes zero sessions", () => {
  it.live("work runs in the reduced-layer worker, never on the hub", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.text("hub-zero")
      const pool = createBunExecutorWorkerPool({
        k: 1,
        entry: SESSION_WORKER,
        timeoutMs: 15_000,
        workerData: {
          snapshot: workerSnapshot(),
          env: { values: {} },
          model: { baseUrl: llm.url, apiKey: "test-key", model: "test-model" },
        },
      })
      const hub = createSessionHub({ pool })
      try {
        const result = yield* Effect.promise(() => hub.ensureRunning(sid("ses_1")))
        const value = result.value as { text: string; threadId: number }
        expect(value.text).toBe("hub-zero")
        // `threadId` is 0 on the main thread; a positive id proves the session
        // pipeline ran inside the worker isolate, not on the hub.
        expect(value.threadId).toBeGreaterThan(0)
        expect(hub.dispatcher.registry.size).toBe(0)
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )
})

describe("12.2 admission admits K + headroom respecting depth", () => {
  test("K busy queues; a depth-1 child spawns headroom; the parent retains its slot", async () => {
    const pool = createBunExecutorWorkerPool({ k: 1, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      const parent = hub.dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 0 })
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_parent"))?.state === "running", {
        message: "parent never ran",
      })

      const sibling = hub.dispatcher.dispatch({ sessionID: sid("ses_sib"), depth: 0 })
      await pollUntil(() => hub.dispatcher.queuedCount === 1, { message: "sibling never queued" })
      expect(hub.dispatcher.queuedCount).toBe(1)
      expect(hub.dispatcher.headroomSlotCount).toBe(0)

      const child = hub.dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
      await pollUntil(() => hub.dispatcher.headroomSlotCount === 1, { message: "headroom never spawned" })

      // The awaiting parent keeps its foreground slot while the child runs in headroom.
      expect(hub.dispatcher.registry.get(sid("ses_parent"))?.state).toBe("running")
      expect(hub.dispatcher.registry.get(sid("ses_child"))?.state).toBe("running")

      await Promise.all([parent, sibling, child])
      expect(hub.dispatcher.busySlotCount).toBe(0)
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("12.3 the hub loop is I/O-only", () => {
  test("a hub timer keeps firing while K sessions run in workers", async () => {
    const pool = createBunExecutorWorkerPool({ k: 2, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      let ticks = 0
      const timer = setInterval(() => {
        ticks += 1
      }, 5)
      await Promise.all([hub.ensureRunning(sid("ses_a")), hub.ensureRunning(sid("ses_b"))])
      clearInterval(timer)

      // The hub event loop was never blocked: its timer fired while the workers ran.
      expect(ticks).toBeGreaterThan(0)
      expect(hub.dispatcher.busySlotCount).toBe(0)
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("12.4 HTTP ingress + SSE stay functional", () => {
  test("health + SSE respond while K sessions run", async () => {
    const pool = createBunExecutorWorkerPool({ k: 2, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url)
        if (url.pathname === "/health") return new Response("ok")
        if (url.pathname === "/sse") {
          const body = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("data: hello\n\n"))
              controller.close()
            },
          })
          return new Response(body, { headers: { "content-type": "text/event-stream" } })
        }
        return new Response("not found", { status: 404 })
      },
    })
    try {
      const running = Promise.all([hub.ensureRunning(sid("ses_a")), hub.ensureRunning(sid("ses_b"))])
      const base = `http://localhost:${server.port}`

      const health = await fetch(`${base}/health`)
      expect(await health.text()).toBe("ok")

      const sse = await fetch(`${base}/sse`)
      expect(sse.headers.get("content-type")).toContain("text/event-stream")
      const chunk = await sse.body!.getReader().read()
      expect(new TextDecoder().decode(chunk.value)).toContain("data: hello")

      await running
    } finally {
      await server.stop(true)
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// Transport frame buffering: consume-on-match, cap, crash teardown.
// ---------------------------------------------------------------------------

const markerOf = (message: SessionExecutorOutbound): number | undefined =>
  (message as unknown as { readonly marker?: number }).marker

class FakeWorker {
  static last: FakeWorker | undefined
  /** Every constructed fake, so a test can inspect the `workerData` the pool shipped. */
  static readonly instances: FakeWorker[] = []
  onmessage: ((event: { data: unknown }) => void) | null = null
  onerror: ((event: { message: string; error?: unknown }) => void) | null = null
  readonly posted: unknown[] = []
  readonly entry: unknown
  readonly options: { readonly workerData?: unknown } | undefined
  terminateCount = 0

  constructor(entry?: unknown, options?: { readonly workerData?: unknown }) {
    this.entry = entry
    this.options = options
    FakeWorker.last = this
    FakeWorker.instances.push(this)
  }

  postMessage(message: unknown): void {
    this.posted.push(message)
  }

  terminate(): Promise<number> {
    this.terminateCount += 1
    return Promise.resolve(0)
  }

  emit(data: unknown): void {
    this.onmessage?.({ data })
  }

  fail(message: string, error?: unknown): void {
    this.onerror?.(error === undefined ? { message } : { message, error })
  }
}

async function withFakeWorker(
  run: (fake: FakeWorker, transport: ExecutorTransport) => Promise<void>,
  options: { readonly bufferCapacity?: number } = {},
): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    const transport = createBunExecutorTransport({
      entry: "fake://session-worker",
      ...(options.bufferCapacity === undefined ? {} : { bufferCapacity: options.bufferCapacity }),
    })
    const fake = FakeWorker.last
    if (fake === undefined) throw new Error("fake worker was never constructed")
    await run(fake, transport)
  } finally {
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

describe("12.x executor transport buffering", () => {
  test("a buffered frame is consumed by the first matching wait", async () => {
    await withFakeWorker(async (fake, transport) => {
      fake.emit({ marker: 1 })
      const first = await transport.waitFor((message) => markerOf(message) === 1)
      expect(markerOf(first)).toBe(1)
      await expect(transport.waitFor((message) => markerOf(message) === 1, { timeoutMs: 20 })).rejects.toThrow(
        /timed out/,
      )
    })
  })

  test("the buffer is capped, dropping the oldest frames", async () => {
    await withFakeWorker(
      async (fake, transport) => {
        fake.emit({ marker: 1 })
        fake.emit({ marker: 2 })
        fake.emit({ marker: 3 })
        await expect(transport.waitFor((message) => markerOf(message) === 1, { timeoutMs: 20 })).rejects.toThrow(
          /timed out/,
        )
        const latest = await transport.waitFor((message) => markerOf(message) === 3)
        expect(markerOf(latest)).toBe(3)
      },
      { bufferCapacity: 2 },
    )
  })

  test("a worker error terminates the isolate, detaches handlers and fails waiters", async () => {
    await withFakeWorker(async (fake, transport) => {
      const pending = transport.waitFor(() => true)
      fake.fail("boom")

      await expect(pending).rejects.toThrow("session worker error: boom")
      expect(fake.terminateCount).toBe(1)
      expect(fake.onmessage).toBeNull()
      expect(fake.onerror).toBeNull()
      expect(transport.alive).toBe(false)
      expect(() =>
        transport.post({ type: "run", id: 1, sessionID: "ses_1" } as unknown as SessionExecutorInbound),
      ).toThrow(/terminated/)
    })
  })

  test("a malformed workerData fails loud at the isolate boundary", async () => {
    const transport = createBunExecutorTransport({
      entry: SESSION_WORKER,
      timeoutMs: 5_000,
      workerData: { snapshot: null, env: {}, model: { baseUrl: "http://localhost:1" } },
    })
    await expect(transport.waitFor(() => true)).rejects.toThrow(/snapshot is required/)
    await transport.terminate()
  })
})

// ---------------------------------------------------------------------------
// Production wiring: job-port hookup, lazy pool, cancel, layer lifecycle.
// ---------------------------------------------------------------------------

/** An in-memory executor worker that completes instantly and records termination. */
class ImmediateWorker implements ExecutorWorker {
  alive = true
  terminated = false
  readonly results = new Map<SessionID, SessionExecutionResult>()

  constructor(readonly id: number) {}

  async execute(run: ExecutorRun): Promise<void> {
    this.results.set(run.sessionID, { sessionID: run.sessionID, value: null })
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return false
  }
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.terminated = true
    this.alive = false
  }
}

/** A lazy pool whose workers are recorded so a test can observe materialisation. */
const trackedLazyPool = (k: number, created: ImmediateWorker[]) =>
  createLazyExecutorWorkerPool({
    k,
    create: (id) => {
      const worker = new ImmediateWorker(id)
      created.push(worker)
      return worker
    },
  })

/**
 * An executor worker with a cancel port whose run either unwinds (resolves) or
 * dies with the executor machine (rejects) when cancelled. `acknowledge`
 * controls whether it reports a `cancel-ack`.
 */
class ControllableWorker implements ExecutorWorker {
  alive = true
  readonly cancelPort: CancelPort
  private readonly pending = new Map<SessionID, { resolve: () => void; reject: (error: unknown) => void }>()
  private readonly results = new Map<SessionID, SessionExecutionResult>()
  private readonly ackHandlers = new Set<(frame: CancelFrame) => void>()

  constructor(
    readonly id: number,
    private readonly onCancel: "resolve" | "reject" | "ignore",
    private readonly acknowledge = false,
  ) {
    this.cancelPort = {
      notify: (frame) => {
        if (frame.type !== "cancel") return
        if (this.acknowledge) {
          for (const handler of [...this.ackHandlers]) {
            handler({ type: "cancel-ack", sessionID: frame.sessionID, accepted: true })
          }
        }
        if (this.onCancel === "ignore") return
        const sessionID = frame.sessionID as SessionID
        const pending = this.pending.get(sessionID)
        if (pending === undefined) return
        this.pending.delete(sessionID)
        if (this.onCancel === "resolve") {
          this.results.set(sessionID, { sessionID, value: null })
          pending.resolve()
        } else {
          pending.reject(new Error("executor machine interrupted"))
        }
      },
      onMessage: (handler) => {
        this.ackHandlers.add(handler)
        return () => void this.ackHandlers.delete(handler)
      },
    }
  }

  execute(run: ExecutorRun): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.pending.set(run.sessionID, { resolve, reject })
    })
  }

  async cancel(sessionID: SessionID): Promise<void> {
    this.cancelPort.notify({ type: "cancel", sessionID })
  }

  async assertNotBusy(sessionID: SessionID): Promise<boolean> {
    return this.pending.has(sessionID)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }

  async terminate(): Promise<void> {
    this.alive = false
    const pending = [...this.pending.values()]
    this.pending.clear()
    for (const run of pending) run.reject(new Error("terminated"))
  }
}

class ControllablePool implements ExecutorWorkerPool {
  readonly workers: ControllableWorker[]
  readonly spawned: ControllableWorker[] = []

  constructor(
    k: number,
    private readonly onCancel: "resolve" | "reject" | "ignore",
    private readonly acknowledge = false,
  ) {
    this.workers = Array.from({ length: k }, (_, index) => new ControllableWorker(index + 1, onCancel, acknowledge))
  }

  spawn(): ExecutorSessionWorker {
    const worker = new ControllableWorker(100 + this.spawned.length, this.onCancel, this.acknowledge)
    this.spawned.push(worker)
    return worker
  }

  async terminateAll(): Promise<void> {
    await Promise.all([...this.workers, ...this.spawned].map((worker) => worker.terminate()))
  }

  byID(id: number): ControllableWorker | undefined {
    return [...this.workers, ...this.spawned].find((worker) => worker.id === id)
  }

  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    for (const worker of [...this.workers, ...this.spawned]) {
      const result = worker.resultFor(sessionID)
      if (result !== undefined) return result
    }
    return undefined
  }
}

/** A scheduler that records nothing and never fires (the run must settle first). */
const neverSchedule: CancelScheduler = () => () => {}

/** A scheduler that records timers so a test can fire the grace window. */
const recordingSchedule = () => {
  const timers: { delayMs: number; run: () => void; cancelled: boolean }[] = []
  const schedule: CancelScheduler = (delayMs, run) => {
    const timer = { delayMs, run, cancelled: false }
    timers.push(timer)
    return () => {
      timer.cancelled = true
    }
  }
  return { schedule, pending: () => timers.filter((timer) => !timer.cancelled) }
}

describe("11 pool job-port hookup", () => {
  test("the hub hands its job-port sink to a pool factory and a job round-trips", async () => {
    let sink: ((workerID: number, port: MessagePort) => void) | undefined
    const hub = createSessionHub({
      pool: (onJobPort) => {
        sink = onJobPort
        return trackedLazyPool(1, [])
      },
    })

    expect(typeof sink).toBe("function")

    const channel = new MessageChannel()
    const client = createJobClient(jobPortFromMessagePort(channel.port2))
    sink!(1, channel.port1)

    await client.start({ id: "ses_job", type: "task" })
    expect(hub.jobs.get("ses_job")?.status).toBe("running")
    await client.complete("ses_job", "done")
    expect(hub.jobs.get("ses_job")?.output).toBe("done")

    client.dispose()
    await hub.terminateAll()
  })
})

describe("12 lazy pool", () => {
  test("constructing the hub spawns no workers until the first dispatch", async () => {
    const created: ImmediateWorker[] = []
    const hub = createSessionHub({ pool: trackedLazyPool(2, created) })

    expect(created).toHaveLength(0)

    const result = await hub.ensureRunning(sid("ses_lazy"))
    expect(result.sessionID).toBe("ses_lazy")
    expect(created.length).toBeGreaterThan(0)

    await hub.terminateAll()
  })
})

describe("13 cooperative cancel within grace", () => {
  test("cancelWithGrace stops a running session on a real worker", async () => {
    const pool = createBunExecutorWorkerPool({ k: 1, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool, cancellation: (p) => createWorkerCancellation(p, { graceMs: 5_000 }) })
    try {
      const running = hub.dispatcher.dispatch({ sessionID: sid("ses_cancel"), depth: 0 })
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_cancel"))?.state === "running", {
        message: "session never started",
      })

      const outcome = await hub.cancel(sid("ses_cancel"))
      expect(outcome?.mode).toBe("cooperative")
      await expect(running).rejects.toBeInstanceOf(SessionCancelledError)
      expect(hub.dispatcher.registry.size).toBe(0)
    } finally {
      await hub.terminateAll()
    }
  }, 20_000)
})

describe("14 makeSessionHubNode", () => {
  test("provides SessionHubService and terminates the pool on scope close", async () => {
    const created: ImmediateWorker[] = []
    const node = makeSessionHubNode({ pool: trackedLazyPool(1, created) })

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(AppNodeBuilder.build(node))
          const hub = Context.get(context, SessionHubService)
          const result = yield* Effect.promise(() => hub.ensureRunning(sid("ses_prod")))
          expect(result.sessionID).toBe("ses_prod")
        }),
      ),
    )

    // The layer finalizer ran: the pool's worker was materialised and terminated.
    expect(created).toHaveLength(1)
    expect(created.every((worker) => worker.terminated)).toBe(true)
  }, 20_000)
})

describe("15 layer lifecycle / leak", () => {
  test("closing the layer scope terminates every worker and disposes job ports", async () => {
    const created: ImmediateWorker[] = []
    let jobPortDisposed = false
    const fakePort: JobPort = {
      post: () => {},
      onMessage: () => () => {
        jobPortDisposed = true
      },
    }

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(createSessionHubLayer({ pool: trackedLazyPool(2, created) }))
          const hub = Context.get(context, SessionHubService)
          hub.attachJobPort(1, fakePort)
          yield* Effect.promise(() =>
            Promise.all([hub.ensureRunning(sid("ses_life_1")), hub.ensureRunning(sid("ses_life_2"))]),
          )
        }),
      ),
    )

    expect(created).toHaveLength(2)
    expect(created.every((worker) => worker.terminated)).toBe(true)
    expect(jobPortDisposed).toBe(true)
  })
})

describe("35a worker port serving", () => {
  const hubWriter: HubWriter = {
    query: async () => [{ n: 1 }],
    publish: async () => ({ seq: 7, committed: true }),
    begin: async () => {
      throw new Error("begin was not expected in this test")
    },
    txQuery: async () => {
      throw new Error("txQuery was not expected in this test")
    },
    commit: async () => {
      throw new Error("commit was not expected in this test")
    },
    rollback: async () => {
      throw new Error("rollback was not expected in this test")
    },
  }

  test("serves a worker's writer port against the hub writer", async () => {
    const hub = createSessionHub({ pool: trackedLazyPool(1, []), ports: { writer: hubWriter, broker: {} as never } })
    const channel = new MessageChannel()
    const dispose = hub.attachWorkerPorts(1, { writer: channel.port1 })
    const client = createWorkerWriterClient(channel.port2)

    expect(await client.query("all", "SELECT 1", [])).toEqual([{ n: 1 }])
    expect(await client.publish({ id: "e1", type: "t", aggregateID: "a", data: {} })).toEqual({
      seq: 7,
      committed: true,
    })

    client.dispose()
    dispose()
    channel.port1.close()
    channel.port2.close()
    await hub.terminateAll()
  })

  test("serves the hub-proxy authorities over a worker control port", async () => {
    const hub = createSessionHub({
      pool: trackedLazyPool(1, []),
      ports: { writer: hubWriter, broker: {} as never, hubProxy: { ping: () => "pong" } },
    })
    const channel = new MessageChannel()
    const dispose = hub.attachHubProxyPort(1, channel.port1)
    const client = createHubProxyClient(channel.port2)

    expect(await client.call("ping", [])).toBe("pong")
    const missing = await client.call("missing", []).then(
      (value) => ({ ok: true as const, value }),
      (error) => ({ ok: false as const, message: error instanceof Error ? error.message : String(error) }),
    )
    expect(missing).toEqual({ ok: false, message: 'unknown hub-proxy method "missing"' })

    client.dispose()
    dispose()
    channel.port1.close()
    channel.port2.close()
    await hub.terminateAll()
  })

  test("returns idempotent disposers and is a no-op without backends", () => {
    const hub = createSessionHub({ pool: trackedLazyPool(1, []) })
    const writerChannel = new MessageChannel()
    const proxyChannel = new MessageChannel()
    const disposePorts = hub.attachWorkerPorts(1, { writer: writerChannel.port1 })
    const disposeProxy = hub.attachHubProxyPort(1, proxyChannel.port1)
    expect(typeof disposePorts).toBe("function")
    expect(typeof disposeProxy).toBe("function")
    disposePorts()
    disposeProxy()
    writerChannel.port1.close()
    writerChannel.port2.close()
    proxyChannel.port1.close()
    proxyChannel.port2.close()
  })
})

describe("42 crash/respawn end-to-end", () => {
  test("killing a worker mid-dispatch respawns it and completes the next session", async () => {
    const pool = createBunExecutorWorkerPool({ k: 1, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      const first = hub.dispatcher.dispatch({ sessionID: sid("ses_crash"), depth: 0 })
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_crash"))?.state === "running", {
        message: "session never started",
      })

      const workerID = hub.dispatcher.registry.get(sid("ses_crash"))!.workerID!
      await pool.byID(workerID)!.terminate()
      await expect(first).rejects.toBeInstanceOf(WorkerCrashedError)

      const second = hub.dispatcher.dispatch({ sessionID: sid("ses_respawn"), depth: 0 })
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_respawn"))?.state === "running", {
        message: "respawned worker never ran the session",
        timeoutMs: 10_000,
      })
      await second
      expect(pool.resultFor(sid("ses_respawn"))?.sessionID).toBe(sid("ses_respawn"))
    } finally {
      await hub.terminateAll()
    }
  }, 25_000)
})

// ---------------------------------------------------------------------------
// 36 production port backends: capabilities + writer/broker/egress round-trip.
// ---------------------------------------------------------------------------

const productionInstanceCtx = {
  directory: "/tmp/opencode-worker-ports",
  worktree: "/tmp/opencode-worker-ports",
  project: {} as never,
}

const providerInfo = {
  id: "test",
  name: "Test",
  source: "api",
  env: [],
  options: { baseURL: "https://api.example.test/v1", headers: { "x-provider": "1" } },
  models: {},
} as unknown as Provider.Info

const providerModel = {
  id: "model",
  providerID: "test",
  api: { id: "model-api", url: "https://fallback.example.test/v1", npm: "@ai-sdk/openai-compatible" },
  name: "Model",
  capabilities: {},
  cost: {},
  limit: { context: 1_000, output: 1_000 },
  status: "active",
  options: {},
  headers: { "x-model": "1" },
  release_date: "2024-01-01",
} as unknown as Provider.Model

/** Replace the production node's declared service deps with dependency-free mocks. */
const productionReplacements: LayerNode.Replacements = [
  [Config.node, Layer.mock(Config.Service, { get: () => Effect.succeed({ model: "test/model" } as ConfigV1.Info) })],
  [
    Auth.node,
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
      all: () => Effect.succeed({ test: new Auth.Api({ type: "api", key: "sk-test" }) }),
    }),
  ],
  [ModelsDev.node, Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) })],
  [
    Provider.node,
    Layer.mock(Provider.Service, {
      defaultModel: () => Effect.succeed({ providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("model") }),
      getProvider: () => Effect.succeed(providerInfo),
      getModel: () => Effect.succeed(providerModel),
    }),
  ],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalSessionWorkers: true })],
  // Hub-proxy authorities (plan decision 64). `SessionStatus.get` is left
  // implemented — and reads the ambient `InstanceRef` — so the production
  // hub-proxy round-trip proves the handler runs under the dispatch context.
  [
    SessionStatus.node,
    Layer.mock(SessionStatus.Service, {
      get: () =>
        Effect.gen(function* () {
          const ctx = yield* InstanceRef
          if (!ctx) return yield* Effect.die(new Error("InstanceRef not provided to the hub-proxy handler"))
          return { type: "idle" } as SessionStatus.Info
        }),
      list: () => Effect.succeed(new Map()),
      set: () => Effect.void,
    }),
  ],
  [Permission.node, Layer.mock(Permission.Service, {})],
  [Question.node, Layer.mock(Question.Service, {})],
  // The hub republishes worker/originated events through the bridge's live-only
  // seam; the mock answers `publishLive` (the only method the hub calls).
  [EventV2Bridge.node, Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void })],
]

const buildProductionNode = (options: Parameters<typeof productionSessionHubNode>[0]) =>
  AppNodeBuilder.build(productionSessionHubNode(options), productionReplacements)

const provideProductionInstance = <A, E>(effect: Effect.Effect<A, E>) =>
  effect.pipe(Effect.provideService(InstanceRef, productionInstanceCtx))

interface CapturedPorts {
  capabilities?: unknown
  created: number[]
  ports?: WorkerTransferredPorts
  hubProxy?: MessagePort
}

/** A production pool factory whose (in-memory) worker optionally transfers its ports. */
const portPoolFactory =
  (captured: CapturedPorts, emitPorts: boolean): ProductionPoolFactory =>
  (resolveInit, sinks) =>
    createLazyExecutorWorkerPool({
      k: 1,
      create: async (id) => {
        captured.capabilities = (await resolveInit()).capabilities
        captured.created.push(id)
        if (emitPorts) {
          const writer = new MessageChannel()
          const egress = new MessageChannel()
          const broker = new MessageChannel()
          sinks.onWorkerPorts(id, { writer: writer.port2, egress: egress.port2, broker: broker.port2 })
          captured.ports = { writer: writer.port1, egress: egress.port1, broker: broker.port1 }
        }
        return new ImmediateWorker(id)
      },
    })

/** A production pool factory whose worker transfers only its hub-proxy control port. */
const hubProxyPoolFactory =
  (captured: CapturedPorts): ProductionPoolFactory =>
  (resolveInit, sinks) =>
    createLazyExecutorWorkerPool({
      k: 1,
      create: async (id) => {
        captured.capabilities = (await resolveInit()).capabilities
        captured.created.push(id)
        const channel = new MessageChannel()
        sinks.onHubProxyPort(id, channel.port2)
        captured.hubProxy = channel.port1
        return new ImmediateWorker(id)
      },
    })

const fakeBroker = (): BrokerBackend => ({
  acquire: async () => ({ instanceId: "test", servers: [] }),
  call: async () => ({ ok: true }),
  cancel: async () => {},
  oauth: async () => ({ callbackUrl: "http://localhost/oauth", instanceId: "test", owner: "broker" }),
  release: async () => {},
  ping: async () => ({ threadId: 0, directories: 0 }),
  teardown: async () => {},
})

describe("36 production port backends", () => {
  test("productionSessionHubNode is exported and provides SessionHubService", () => {
    expect(typeof productionSessionHubNode).toBe("function")
    expect(typeof makeSessionHubNode).toBe("function")
    expect(productionSessionHubNode().service).toBe(SessionHubService)
  })

  test("the production node advertises sessionWorkerPorts and round-trips writer/broker/egress", async () => {
    const captured: CapturedPorts = { created: [] }
    const writerCalls: unknown[] = []
    const egressFrames: unknown[] = []
    const backends: WorkerPortBackends = {
      writer: {
        query: async (method, sql, params) => {
          writerCalls.push([method, sql, params])
          return [{ n: 1 }]
        },
        publish: async () => ({ seq: 7, committed: true }),
        ...unusedHubWriterTx,
      },
      broker: fakeBroker(),
      egress: (_workerID, port) => {
        const receiver = createEgressReceiver({ port, onFrame: (frame) => egressFrames.push(frame) })
        return () => receiver.close()
      },
    }

    // The round-trip runs inside the hub's scope: closing it disposes the port
    // servers, so a query afterwards would hang.
    await Effect.runPromise(
      provideProductionInstance(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              buildProductionNode({
                count: 1,
                entry: EXECUTOR_FIXTURE,
                workKind: "instant",
                poolFactory: portPoolFactory(captured, true),
                ports: backends,
              }),
            )
            const hub = Context.get(context, SessionHubService)
            yield* Effect.promise(() => hub.ensureRunning(sid("ses_ports")))

            // The dispatched worker's init advertised the ports capability.
            expect(captured.capabilities).toEqual({ sessionWorkerPorts: true })
            expect(captured.created).toEqual([1])
            const ports = captured.ports
            expect(ports?.writer).toBeDefined()
            expect(ports?.broker).toBeDefined()
            expect(ports?.egress).toBeDefined()

            yield* Effect.promise(async () => {
              // Writer port round-trip against the hub authority.
              const writer = createWorkerWriterClient(ports!.writer!)
              expect(await writer.query("all", "SELECT 1", [])).toEqual([{ n: 1 }])
              expect(await writer.publish({ id: "e1", type: "t", aggregateID: "a", data: {} })).toEqual({
                seq: 7,
                committed: true,
              })
              expect(writerCalls).toEqual([["all", "SELECT 1", []]])
              writer.dispose()

              // Broker port round-trip against the hub authority.
              const broker = createWorkerBrokerClient(ports!.broker!)
              expect(await broker.call("dir", "srv", "method")).toEqual({ ok: true })
              broker.dispose()

              // Egress port round-trip: the emitted delta reaches the hub receiver.
              const egress = createWorkerEgressClient(ports!.egress!, { windowMs: 0 })
              egress.emit({ id: "x1", type: "session.text", properties: { text: "hi" } })
              egress.end()
              await pollUntil(() => egressFrames.length > 0, { message: "egress frame never drained" })
              expect(egressFrames.length).toBeGreaterThan(0)
              egress.dispose()

              for (const port of [ports!.writer!, ports!.broker!, ports!.egress!]) port.close()
            })
          }),
        ),
      ),
    )
  })

  test("the default production node wires real backends: sessionWorkerPorts without spawning at build", async () => {
    const captured: CapturedPorts = { created: [] }
    await Effect.runPromise(
      provideProductionInstance(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              buildProductionNode({
                count: 1,
                entry: EXECUTOR_FIXTURE,
                workKind: "instant",
                poolFactory: portPoolFactory(captured, false),
              }),
            )
            // Building the node (default real backends) spawns no worker/isolate.
            expect(captured.created).toEqual([])
            const hub = Context.get(context, SessionHubService)
            yield* Effect.promise(() => hub.ensureRunning(sid("ses_default_ports")))
          }),
        ),
      ),
    )

    expect(captured.capabilities).toEqual({ sessionWorkerPorts: true })
    expect(captured.created).toEqual([1])
  })

  test("the production hub-proxy authorities answer a worker call (resolves, not hangs)", async () => {
    const captured: CapturedPorts = { created: [] }
    await Effect.runPromise(
      provideProductionInstance(
        Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              buildProductionNode({
                count: 1,
                entry: EXECUTOR_FIXTURE,
                workKind: "instant",
                poolFactory: hubProxyPoolFactory(captured),
                // Injected authorities, so building the port backends spawns no
                // real writer/broker isolate; the hub-proxy handlers still come
                // from the layer's real service deps.
                backends: {
                  writer: {
                    query: async () => [],
                    publish: async () => ({ seq: null, committed: false }),
                    ...unusedHubWriterTx,
                  },
                  broker: fakeBroker(),
                  databasePath: ":memory:",
                },
              }),
            )
            const hub = Context.get(context, SessionHubService)
            yield* Effect.promise(() => hub.ensureRunning(sid("ses_hubproxy")))
            yield* Effect.promise(async () => {
              const client = createHubProxyClient(captured.hubProxy!)
              expect(await client.call("get", [sid("ses_hubproxy")])).toEqual({ type: "idle" })
              client.dispose()
            })
          }),
        ),
      ),
    )
  })

  test("the owned production writer opens + migrates the app database so a worker durable publish succeeds", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "writer.db")
    // No manual seed: `buildProductionPortBackends` ships the production schema
    // over `writer.migrate(...)`, so the owned isolate creates `event`/`event_sequence`.
    const backends = buildProductionPortBackends({
      databasePath: filename,
      broker: fakeBroker(),
      writerOptions: { env: healthyEnv },
    })
    const channel = new MessageChannel()
    const dispose = serveWriterPort(backends.writer, channel.port1)
    const client = createWorkerWriterClient(channel.port2)
    try {
      expect(await client.publish({ id: "e1", type: "t", aggregateID: "a", data: { k: 1 } })).toMatchObject({
        seq: 0,
        committed: true,
      })
      expect(await client.query("all", "SELECT seq, type FROM event WHERE aggregate_id = ?", ["a"])).toEqual([
        { seq: 0, type: "t" },
      ])
    } finally {
      client.dispose()
      dispose()
      channel.port1.close()
      channel.port2.close()
      await backends.dispose()
    }
  })

  test("buildProductionPortBackends defaults to a draining egress and owns only its own isolates", async () => {
    let torndown = 0
    const broker = fakeBroker()
    const backends = buildProductionPortBackends({
      writer: {
        query: async () => [],
        publish: async () => ({ seq: null, committed: false }),
        ...unusedHubWriterTx,
      },
      broker: { ...broker, teardown: async () => void (torndown += 1) },
    })

    expect(backends.egress).toBeDefined()
    const channel = new MessageChannel()
    const dispose = backends.egress!(1, channel.port1)
    // The default egress drains without a real sink; emitting is a no-op.
    channel.port1.postMessage(new Uint8Array(0))
    dispose()
    channel.port1.close()
    channel.port2.close()

    // An injected authority belongs to the caller: dispose must not tear it down.
    await backends.dispose()
    expect(torndown).toBe(0)
  })

  test("flag-off excludes the production hub node (zero workers)", () => {
    const off = sessionWorkerNodes({ hubNode: productionSessionHubNode(), enabled: false })
    expect(off).toEqual([])
    expect(LayerNode.group([...off]).dependencies).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// Q22/Q15/Q16 — the hub events port: worker EventV2 frames republished live-only.
// ---------------------------------------------------------------------------

/** A `LiveEventPublisher` spy that records every republished event. */
const recordingPublisher = () => {
  const published: LiveEvent[] = []
  const publisher: LiveEventPublisher = {
    publish: (event) => void published.push(event),
    publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
  }
  return { publisher, published }
}

describe("Q22 hub event sink", () => {
  test("republishes a worker event verbatim, live-only", () => {
    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({ publisher })

    sink.event({ id: "evt_1", type: "session.error", properties: { message: "boom" } })

    expect(published).toEqual([{ id: "evt_1", type: "session.error", data: { message: "boom" } }])
    expect(sink.ended).toBe(false)
  })

  test("end / resync / ack update flow control and never republish", () => {
    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({ publisher })

    sink.resync({ pattern: "message.*" })
    sink.ack({ count: 4 })
    sink.end()

    expect(published).toEqual([])
    expect(sink.resyncs).toBe(1)
    expect(sink.acks).toBe(1)
    expect(sink.ended).toBe(true)
  })

  test("serveEventsPort decodes posted frames onto the sink", async () => {
    const { publisher, published } = recordingPublisher()
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, createHubEventSink({ publisher }))
    try {
      channel.port2.postMessage(
        encodeFrame(eventFrame({ event: { id: "evt_2", type: "message.part.delta", properties: { delta: "hi" } } })),
      )
      channel.port2.postMessage(encodeFrame(endFrame()))
      await pollUntil(() => published.length === 1, { message: "event frame never decoded" })
      expect(published[0]).toEqual({ id: "evt_2", type: "message.part.delta", data: { delta: "hi" } })
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a worker event reaches hub EventV2 live-only through the port", async () => {
    const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
    const runtime = ManagedRuntime.make(EventV2.layerWith().pipe(Layer.provide(databaseStub)))
    const channel = new MessageChannel()
    try {
      const events = runtime.runSync(EventV2.Service)
      const received: { id: string; type: string; properties: unknown }[] = []
      runtime.runSync(
        events.listen((event) =>
          Effect.sync(() => void received.push({ id: event.id, type: event.type, properties: event.data })),
        ),
      )

      const dispose = serveEventsPort(
        channel.port1,
        createHubEventSink({ publisher: createLiveEventPublisher(events) }),
      )
      channel.port2.postMessage(
        encodeFrame(eventFrame({ event: { id: "evt_3", type: "message.part.delta", properties: { d: 1 } } })),
      )

      await pollUntil(() => received.length === 1, { message: "event never reached hub EventV2" })
      expect(received).toContainEqual({ id: "evt_3", type: "message.part.delta", properties: { d: 1 } })
      dispose()
    } finally {
      channel.port1.close()
      channel.port2.close()
      await runtime.dispose()
    }
  })

  test("a durable worker event republishes live-only (no durable re-commit)", async () => {
    // A `db` that throws on any access: a durable commit on the hub would explode.
    const commitGuard = Layer.succeed(Database.Service, {
      db: new Proxy(
        {},
        {
          get: () => () => {
            throw new Error("durable commit attempted on the hub")
          },
        },
      ) as never,
    })
    const runtime = ManagedRuntime.make(EventV2.layerWith().pipe(Layer.provide(commitGuard)))
    const channel = new MessageChannel()
    try {
      const events = runtime.runSync(EventV2.Service)
      const received: string[] = []
      runtime.runSync(events.listen((event) => Effect.sync(() => void received.push(event.type))))

      const dispose = serveEventsPort(
        channel.port1,
        createHubEventSink({ publisher: createLiveEventPublisher(events) }),
      )
      channel.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: { id: "evt_d", type: "message.part.updated", properties: { part: 1 } },
            generation: 3,
            ordinal: 9,
          }),
        ),
      )

      await pollUntil(() => received.length === 1, { message: "durable event never republished live" })
      expect(received).toEqual(["message.part.updated"])
      dispose()
    } finally {
      channel.port1.close()
      channel.port2.close()
      await runtime.dispose()
    }
  })

  test("a worker event reaches GlobalBus through the hub bridge listener", async () => {
    const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
    const bridgeLayer = AppNodeBuilder.build(LayerNode.group([EventV2.node, EventV2Bridge.node]), [
      [Database.node, databaseStub],
    ])
    const runtime = ManagedRuntime.make(bridgeLayer)
    const channel = new MessageChannel()
    try {
      const events = runtime.runSync(EventV2.Service)
      const seen: unknown[] = []
      const listener = (event: { payload?: { id?: string; type?: string; properties?: unknown } }) => {
        if (event.payload?.type === "session.error") seen.push(event.payload)
      }
      GlobalBus.on("event", listener)
      try {
        const dispose = serveEventsPort(
          channel.port1,
          createHubEventSink({ publisher: createLiveEventPublisher(events) }),
        )
        channel.port2.postMessage(
          encodeFrame(eventFrame({ event: { id: "evt_4", type: "session.error", properties: { m: 1 } } })),
        )

        await pollUntil(() => seen.length === 1, { message: "event never reached GlobalBus" })
        expect(seen).toContainEqual({ id: "evt_4", type: "session.error", properties: { m: 1 } })
        dispose()
      } finally {
        GlobalBus.off("event", listener)
      }
    } finally {
      channel.port1.close()
      channel.port2.close()
      await runtime.dispose()
    }
  })

  test("buildProductionPortBackends serves the events port with a live sink", async () => {
    const { publisher, published } = recordingPublisher()
    const backends = buildProductionPortBackends({
      writer: {
        query: async () => [],
        publish: async () => ({ seq: null, committed: false }),
        ...unusedHubWriterTx,
      },
      broker: fakeBroker(),
      events: publisher,
    })
    const channel = new MessageChannel()
    const dispose = backends.egress!(1, channel.port1)
    try {
      channel.port2.postMessage(
        encodeFrame(eventFrame({ event: { id: "evt_5", type: "message.part.delta", properties: { delta: "yo" } } })),
      )
      await pollUntil(() => published.length === 1, { message: "production sink never republished" })
      expect(published[0]).toEqual({ id: "evt_5", type: "message.part.delta", data: { delta: "yo" } })
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
      await backends.dispose()
    }
  })
})

// ---------------------------------------------------------------------------
// Q9/Q23 — the hub publishes `session.cancelled` after `cancelWithGrace` returns.
// ---------------------------------------------------------------------------

describe("Q9/Q23 session.cancelled", () => {
  test("the hub publishes session.cancelled with the CancelOutcome fields", async () => {
    const { publisher, published } = recordingPublisher()
    const pool = createBunExecutorWorkerPool({ k: 1, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({
      pool,
      events: publisher,
      cancellation: (p) => createWorkerCancellation(p, { graceMs: 5_000 }),
    })
    try {
      const running = hub.dispatcher.dispatch({ sessionID: sid("ses_cancelled"), depth: 0 })
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_cancelled"))?.state === "running", {
        message: "session never started",
      })

      const outcome = await hub.cancel(sid("ses_cancelled"))
      expect(outcome?.mode).toBe("cooperative")
      await expect(running).rejects.toBeInstanceOf(SessionCancelledError)

      const cancelled = published.find((event) => event.type === SESSION_CANCELLED_TYPE)
      expect(cancelled).toBeDefined()
      // The published payload is the serialisable slice of the CancelOutcome.
      expect(cancelled?.data).toEqual({
        sessionID: outcome!.sessionID,
        mode: outcome!.mode,
        durationMs: outcome!.durationMs,
        acknowledged: outcome!.acknowledged,
      })
      // The hub publishes ONLY the cancel outcome; the canonical cancel-idle
      // emitter is `run-state.ts`, so the hub must never emit a second idle.
      expect(published.map((event) => event.type)).toEqual([SESSION_CANCELLED_TYPE])
    } finally {
      await hub.terminateAll()
    }
  }, 20_000)

  test("a completion that wins the cancel race publishes no session.cancelled", async () => {
    const { publisher, published } = recordingPublisher()
    // The run resolves on cancel without acking: completion wins the race.
    const pool = new ControllablePool(1, "resolve")
    const hub = createSessionHub({
      pool,
      events: publisher,
      cancellation: {
        portFor: (worker) => (worker as ControllableWorker).cancelPort,
        graceMs: 500,
        schedule: neverSchedule,
      },
    })
    try {
      const running = hub.dispatcher.dispatch({ sessionID: sid("ses_completed"), depth: 0 })
      const settled = running.then(
        () => undefined,
        (error: unknown) => error,
      )
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_completed"))?.state === "running", {
        message: "session never started",
      })

      const outcome = await hub.cancel(sid("ses_completed"))
      expect(outcome).toMatchObject({ mode: "completed", winner: "completion" })
      expect(published.filter((event) => event.type === SESSION_CANCELLED_TYPE)).toHaveLength(0)
      expect(await settled).toBeInstanceOf(SessionCancelledError)
    } finally {
      await hub.terminateAll()
    }
  })

  test("a grace-expiry terminate publishes the terminated outcome", async () => {
    const { publisher, published } = recordingPublisher()
    const scheduler = recordingSchedule()
    // The run ignores the cancel: the grace window expires and the hub terminates.
    const pool = new ControllablePool(1, "ignore", true)
    const hub = createSessionHub({
      pool,
      events: publisher,
      cancellation: {
        portFor: (worker) => (worker as ControllableWorker).cancelPort,
        graceMs: 500,
        schedule: scheduler.schedule,
      },
    })
    try {
      const running = hub.dispatcher.dispatch({ sessionID: sid("ses_terminated"), depth: 0 })
      const settled = running.then(
        () => undefined,
        (error: unknown) => error,
      )
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_terminated"))?.state === "running", {
        message: "session never started",
      })

      const cancelling = hub.cancel(sid("ses_terminated"))
      await pollUntil(() => scheduler.pending().some((timer) => timer.delayMs === 500), {
        message: "grace timer never scheduled",
      })
      scheduler
        .pending()
        .find((timer) => timer.delayMs === 500)
        ?.run()
      const outcome = await cancelling

      expect(outcome).toMatchObject({ mode: "terminated", winner: "cancel", acknowledged: true })
      const cancelled = published.find((event) => event.type === SESSION_CANCELLED_TYPE)
      expect(cancelled?.data).toEqual({
        sessionID: outcome!.sessionID,
        mode: "terminated",
        durationMs: outcome!.durationMs,
        acknowledged: true,
      })
      expect(await settled).toBeInstanceOf(SessionCancelledError)
    } finally {
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// W2 — production cancellation authorities (Phase 5.3/6.2/6.4).
// ---------------------------------------------------------------------------

const settleOf = (promise: Promise<unknown>): Promise<unknown> =>
  promise.then(
    () => undefined,
    (error: unknown) => error,
  )

describe("W2 cancellation authorities", () => {
  test("childrenOf defaults to the job registry and onCancelled marks jobs cancelled", async () => {
    const pool = new ControllablePool(1, "resolve")
    // No explicit childrenOf/onCancelled: the hub must default to its job registry.
    const hub = createSessionHub({ pool, cancellation: {} })
    try {
      hub.jobs.start({ id: "ses_parent", type: "task" })
      hub.jobs.start({ id: "ses_child", type: "task", parentID: "ses_parent" })
      hub.jobs.linkChild("ses_parent", "ses_child")

      const parent = settleOf(hub.dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 0 }))
      const child = settleOf(hub.dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 }))
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_child"))?.state === "running", {
        message: "child never started",
      })

      await hub.cancel(sid("ses_parent"))

      // Children-first cascade reached the child, and both jobs are terminal.
      expect(await child).toBeInstanceOf(SessionCancelledError)
      expect(await parent).toBeInstanceOf(SessionCancelledError)
      expect(hub.jobs.get("ses_child")?.status).toBe("cancelled")
      expect(hub.jobs.get("ses_parent")?.status).toBe("cancelled")
    } finally {
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// W2 — the dispatching session's model is threaded into the worker init.
// ---------------------------------------------------------------------------

describe("W2 session model threading", () => {
  it.live("buildSessionWorkerInit receives the dispatching session's model", () =>
    Effect.gen(function* () {
      const captured: SessionWorkerInit[] = []
      const poolFactory: ProductionPoolFactory = (resolveInit) =>
        createLazyExecutorWorkerPool({
          k: 1,
          create: async (id) => {
            captured.push(await resolveInit())
            return new ImmediateWorker(id)
          },
        })

      const deps = Layer.mergeAll(
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
        }),
        Layer.mock(Auth.Service, {
          all: () => Effect.succeed({}),
          get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
        }),
        Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
        Layer.mock(Provider.Service, {
          defaultModel: () =>
            Effect.succeed({ providerID: ProviderV2.ID.make("default"), modelID: ModelV2.ID.make("default") }),
          getProvider: () => Effect.succeed(providerInfo),
          getModel: () => Effect.succeed(providerModel),
        }),
        RuntimeFlags.layer({ experimentalSessionWorkers: true }),
        Layer.mock(SessionStatus.Service, {
          get: () => Effect.succeed({ type: "idle" } as SessionStatus.Info),
          list: () => Effect.succeed(new Map()),
          set: () => Effect.void,
        }),
        Layer.mock(Permission.Service, {}),
        Layer.mock(Question.Service, {}),
        Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void }),
      )

      const layer = productionSessionHubLayer({ count: 1, poolFactory }).pipe(Layer.provide(deps))
      const sessionID = sid("ses_model")

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          const sessionLayer = Layer.succeed(
            Session.Service,
            Session.Service.of({
              get: () =>
                Effect.succeed({
                  model: { id: "session-model", providerID: "session-provider" },
                } as unknown as Session.Info),
              // The hub reads the last user message for the real plugin input;
              // this model-only test has none.
              findMessage: () => Effect.succeed(Option.none()),
            } as unknown as Session.Interface),
          )
          yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, productionInstanceCtx),
          )
        }),
      )

      expect(captured).toHaveLength(1)
      // The session's model wins over the config default ("config/default").
      expect(String(captured[0].model.providerID)).toBe("session-provider")
      expect(String(captured[0].model.modelID)).toBe("session-model")
    }),
  )
})

// ---------------------------------------------------------------------------
// W4 (P1/P2) — per-session worker model: a worker rebinds when a different
// session (which may carry a different model) is dispatched to it.
// ---------------------------------------------------------------------------

describe("W4 per-session worker model", () => {
  test("one worker rebinds for a second session and resolves that session's model", async () => {
    const resolvedSessions: string[] = []
    const pool = createProductionWorkerPool({
      init: (sessionID) => {
        resolvedSessions.push(String(sessionID))
        return sessionWorkerInit({ model: { modelID: `model-${sessionID}` } })
      },
      count: 1,
      entry: new URL("../../fixture/session-execute-worker.ts", import.meta.url),
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const [worker] = pool.workers
      await worker.execute({ sessionID: sid("ses_first"), depth: 0 })
      expect(pool.resultFor(sid("ses_first"))?.sessionID).toBe(sid("ses_first"))

      // A second session on the same worker slot rebinds: the isolate is rebuilt
      // with THIS session's init, so the model no longer mismatches.
      await worker.execute({ sessionID: sid("ses_second"), depth: 0 })
      expect(resolvedSessions).toEqual(["ses_first", "ses_second"])
      expect(pool.resultFor(sid("ses_second"))?.sessionID).toBe(sid("ses_second"))
    } finally {
      await pool.terminateAll()
    }
  }, 30_000)
})

// ---------------------------------------------------------------------------
// W4 (P3) — the hub ships the resolved plugin effect into the worker init.
// ---------------------------------------------------------------------------

describe("W4 plugin effect shipping", () => {
  it.live("ships the resolved plugin effect in the worker init", () =>
    Effect.gen(function* () {
      const captured: SessionWorkerInit[] = []
      const poolFactory: ProductionPoolFactory = (resolveInit) =>
        createLazyExecutorWorkerPool({
          k: 1,
          create: async (id) => {
            captured.push(await resolveInit())
            return new ImmediateWorker(id)
          },
        })

      const deps = Layer.mergeAll(
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
        }),
        Layer.mock(Auth.Service, {
          all: () => Effect.succeed({}),
          get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
        }),
        Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
        Layer.mock(Provider.Service, {
          defaultModel: () =>
            Effect.succeed({ providerID: ProviderV2.ID.make("default"), modelID: ModelV2.ID.make("default") }),
          getProvider: () => Effect.succeed(providerInfo),
          getModel: () => Effect.succeed(providerModel),
        }),
        RuntimeFlags.layer({ experimentalSessionWorkers: true }),
        Layer.mock(SessionStatus.Service, {
          get: () => Effect.succeed({ type: "idle" } as SessionStatus.Info),
          list: () => Effect.succeed(new Map()),
          set: () => Effect.void,
        }),
        Layer.mock(Permission.Service, {}),
        Layer.mock(Question.Service, {}),
        Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void }),
      )

      const layer = productionSessionHubLayer({
        count: 1,
        poolFactory,
        plugins: async () => [
          {
            name: "sampling",
            hooks: {
              "chat.params": (_input, output) => {
                output.temperature = 0.25
              },
            },
          },
        ],
      }).pipe(Layer.provide(deps))

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          yield* Effect.promise(() => hub.ensureRunning(sid("ses_plugin"))).pipe(
            Effect.provideService(InstanceRef, productionInstanceCtx),
          )
        }),
      )

      expect(captured).toHaveLength(1)
      const shipped = captured[0].plugins
      expect(shipped).toHaveLength(1)
      // The effect must survive the isolate boundary, then apply on the worker.
      expect(assertStructuredCloneable(shipped)).toEqual(shipped)
      const host = createWorkerPluginHost(shipped!)
      const params = yield* Effect.promise(() =>
        host.chatParams({}, { temperature: 1, topP: 1, topK: 1, maxOutputTokens: 100, options: {} }),
      )
      expect(params.temperature).toBe(0.25)
    }),
  )
})

// ---------------------------------------------------------------------------
// W4 (P5 prerequisite) — a worker pid-report on the control port fills the
// hub mirror so reap actually has targets.
// ---------------------------------------------------------------------------

describe("W4 pid-report receive", () => {
  test("a pid-report frame populates the registry and reap uses it", async () => {
    const killed: number[] = []
    const registry = createPidRegistry({ reaper: { killGroup: (pgid) => killed.push(pgid) } })
    const hub = createSessionHub({
      pool: new ControllablePool(1, "resolve"),
      onPidReport: (workerID, report) => registerReported(registry, report, workerID),
    })
    try {
      const channel = new MessageChannel()
      hub.attachHubProxyPort(7, channel.port1)
      channel.port2.postMessage({
        type: "pid-report",
        workerID: 7,
        entries: [{ pid: 4242, pgid: 4242, label: "shell", workerID: 7 }],
      })

      await pollUntil(() => registry.has(4242), { message: "pid report never registered" })
      expect(registry.reap(7)).toEqual([4242])
      expect(killed).toEqual([4242])
    } finally {
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// Fixes A/C/D — define-aware worker entry, migration after open, error cause.
// ---------------------------------------------------------------------------

describe("A define-aware session worker entry", () => {
  test("prefers the runtime env override", () => {
    expect(resolveSessionWorkerEntry({ env: { OPENCODE_SESSION_WORKER_PATH: "/opt/worker.js" } })).toBe(
      "/opt/worker.js",
    )
  })

  test("falls back to the build-time define", () => {
    expect(resolveSessionWorkerEntry({ env: {}, definedPath: "/$bunfs/root/session-worker.ts" })).toBe(
      "/$bunfs/root/session-worker.ts",
    )
  })

  test("falls back to the source isolate when neither is set", () => {
    const entry = resolveSessionWorkerEntry({ env: {} })
    expect(entry).toBeInstanceOf(URL)
    expect(String(entry)).toContain("session-worker.ts")
  })

  test("an empty override is ignored, not treated as a path", () => {
    const entry = resolveSessionWorkerEntry({ env: { OPENCODE_SESSION_WORKER_PATH: "" } })
    expect(entry).toBeInstanceOf(URL)
  })
})

describe("C writer migration runs after open", () => {
  /** A `WriterClient` whose admin calls are recorded; tx methods fail loud if reached. */
  const spyWriter = (calls: string[], onMigrate?: () => void): WriterClient =>
    ({
      open: async (filename: string) => {
        calls.push(`open:${filename}`)
        return { opened: true, filename }
      },
      migrate: async (statements: ReadonlyArray<string>) => {
        calls.push(`migrate:${statements.join(",")}`)
        onMigrate?.()
      },
      query: async () => [],
      publish: async () => ({ seq: 0, committed: true }),
      ...unusedHubWriterTx,
    }) as unknown as WriterClient

  test("opens then migrates exactly once, before the first request", async () => {
    const calls: string[] = []
    const hubWriter = lazyOpenWriter(spyWriter(calls), "app.db", ["CREATE TABLE t(x)"])
    await hubWriter.query("all", "SELECT 1", [])
    await hubWriter.publish({ id: "e", type: "t", aggregateID: "a", data: {} }, { durable: true })
    expect(calls).toEqual(["open:app.db", "migrate:CREATE TABLE t(x)"])
  })

  test("a failed migrate resets the memo so the next request retries", async () => {
    const calls: string[] = []
    let attempts = 0
    const writer = spyWriter(calls, () => {
      attempts += 1
      if (attempts === 1) throw new Error("migrate boom")
    })
    const hubWriter = lazyOpenWriter(writer, "app.db", ["S"])
    await expect(hubWriter.query("all", "SELECT 1", [])).rejects.toThrow("migrate boom")
    await hubWriter.query("all", "SELECT 1", [])
    expect(attempts).toBe(2)
  })
})

describe("D worker isolate errors surface the underlying message + cause", () => {
  test("a worker error carries the message and the originating error as cause", async () => {
    await withFakeWorker(async (fake, transport) => {
      const pending = transport.waitFor(() => true)
      const cause = new Error("Cannot find module './session-worker.ts'")
      fake.fail("boom", cause)

      const error = await pending.then(
        () => {
          throw new Error("expected the waiter to reject")
        },
        (reason: unknown) => reason as Error,
      )
      expect(error.message).toBe("session worker error: boom: Cannot find module './session-worker.ts'")
      expect(error.cause).toBe(cause)
      expect(transport.alive).toBe(false)
    })
  })
})

// ---------------------------------------------------------------------------
// Wave-2 consolidation #1: a dispatch with no ambient `InstanceRef` must still
// ship a REAL project identity to the worker, never the directory string (which
// would leak into the worker snapshot as a project id).
// ---------------------------------------------------------------------------

describe("fallbackInstanceContext (Wave-2 consolidation)", () => {
  test("yields a real project identity, never the directory as a project id", () => {
    const ctx = fallbackInstanceContext()
    expect(ctx.project).toBeDefined()
    expect(typeof ctx.project.id).toBe("string")
    expect(String(ctx.project.id).length).toBeGreaterThan(0)
    expect(String(ctx.project.id)).not.toBe(ctx.directory)
    expect(ctx.worktree).toBe(ctx.directory)
  })
})

// ---------------------------------------------------------------------------
// Fix 1 — the worker reports the REAL pool slot id: the production pool stamps
// `workerData.workerID` on every spawn (initial and per-session rebind), and it
// is the same slot id across a rebind.
// ---------------------------------------------------------------------------

describe("Fix 1 workerID threading", () => {
  test("the production pool ships the pool slot id as workerData.workerID", async () => {
    const original = (globalThis as { Worker?: unknown }).Worker
    ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
    const started = FakeWorker.instances.length
    const pool = createProductionWorkerPool({
      init: () => sessionWorkerInit(),
      count: 2,
      entry: "fake://workerid",
      workKind: "instant",
      timeoutMs: 5_000,
    })
    try {
      // Fire both runs but never await: the fake isolate never replies, and the
      // only thing under test is the `workerData` the pool hands to `new Worker`.
      // Terminating the pool below rejects these pending runs (all caught).
      for (const [index, worker] of pool.workers.entries()) {
        void worker.execute({ sessionID: sid(`ses_wid_${index}`), depth: 0 }).catch(() => undefined)
      }
      await pollUntil(() => FakeWorker.instances.length - started === 2, { message: "workers never spawned" })

      const workerIDs = FakeWorker.instances
        .slice(started)
        .map((worker) => (worker.options?.workerData as { workerID?: number } | undefined)?.workerID)
      // Slot ids 1 and 2 — never the isolate threadId.
      expect(workerIDs).toEqual([1, 2])
    } finally {
      ;(globalThis as { Worker?: unknown }).Worker = original
      await pool.terminateAll()
    }
  })

  test("a worker rebinds to a second session under the SAME pool slot id", async () => {
    const spawned: Array<{ readonly id: number; readonly session: string }> = []
    const worker = createSessionBoundExecutorWorker(3, async (id, sessionID) => {
      spawned.push({ id, session: String(sessionID) })
      return new ImmediateWorker(id)
    })

    await worker.execute({ sessionID: sid("ses_a"), depth: 0 })
    await worker.execute({ sessionID: sid("ses_b"), depth: 0 })

    expect(spawned).toEqual([
      { id: 3, session: "ses_a" },
      { id: 3, session: "ses_b" },
    ])
    await worker.terminate()
  })
})

// ---------------------------------------------------------------------------
// Fix 2 — the hub resolves a plugin effect against the REAL per-request input
// (the session's agent + the last user message), not an empty stub.
// ---------------------------------------------------------------------------

describe("Fix 2 real plugin hook input", () => {
  it.live("ships a plugin effect resolved against the session's real agent + last user message", () =>
    Effect.gen(function* () {
      const captured: SessionWorkerInit[] = []
      const poolFactory: ProductionPoolFactory = (resolveInit) =>
        createLazyExecutorWorkerPool({
          k: 1,
          create: async (id) => {
            captured.push(await resolveInit())
            return new ImmediateWorker(id)
          },
        })

      const deps = Layer.mergeAll(
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
        }),
        Layer.mock(Auth.Service, {
          all: () => Effect.succeed({}),
          get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
        }),
        Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
        Layer.mock(Provider.Service, {
          defaultModel: () =>
            Effect.succeed({ providerID: ProviderV2.ID.make("default"), modelID: ModelV2.ID.make("default") }),
          getProvider: () => Effect.succeed(providerInfo),
          getModel: () => Effect.succeed(providerModel),
        }),
        RuntimeFlags.layer({ experimentalSessionWorkers: true }),
        Layer.mock(SessionStatus.Service, {
          get: () => Effect.succeed({ type: "idle" } as SessionStatus.Info),
          list: () => Effect.succeed(new Map()),
          set: () => Effect.void,
        }),
        Layer.mock(Permission.Service, {}),
        Layer.mock(Question.Service, {}),
        Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void }),
      )

      const layer = productionSessionHubLayer({
        count: 1,
        poolFactory,
        plugins: async () => [
          {
            name: "request",
            hooks: {
              "chat.headers": (input, output) => {
                const real = input as { agent: string; message: { id: string } }
                output.headers["x-agent"] = real.agent
                output.headers["x-message"] = real.message.id
              },
            },
          },
        ],
      }).pipe(Layer.provide(deps))

      const sessionID = sid("ses_plugin_input")
      const lastUserMessage = {
        id: "msg_real",
        sessionID,
        role: "user",
        time: { created: 0 },
        agent: "title",
        model: { providerID: "p", modelID: "m" },
      }

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          const sessionLayer = Layer.succeed(
            Session.Service,
            Session.Service.of({
              get: () =>
                Effect.succeed({
                  agent: "build",
                  model: { id: "m", providerID: "p" },
                } as unknown as Session.Info),
              findMessage: () => Effect.succeed(Option.some({ info: lastUserMessage, parts: [] } as never)),
            } as unknown as Session.Interface),
          )
          yield* Effect.promise(() => hub.ensureRunning(sessionID)).pipe(
            Effect.provide(sessionLayer),
            Effect.provideService(InstanceRef, productionInstanceCtx),
          )
        }),
      )

      expect(captured).toHaveLength(1)
      // The hook saw the last user message's agent + id, exactly as flag-off.
      expect(captured[0].plugins?.[0].resolved?.["chat.headers"]).toEqual({
        headers: { "x-agent": "title", "x-message": "msg_real" },
      })
    }),
  )
})
