// Pure hub, admission & HTTP/SSE liveness.
//
// The hub is the composition of the per-session dispatcher (one lazily
// materialized worker per session, crash handling), the executor pool, and the
// cooperative cancel path. These tests drive the hub over REAL Bun worker
// isolates and assert the four hub properties:
//
//   12.1 the hub executes zero sessions (work runs in a worker isolate);
//   12.2 each session is admitted on its own worker (no queue/headroom split);
//   12.3 the hub loop stays I/O-only while K sessions run;
//   12.4 HTTP ingress + SSE stay functional while K sessions run.

import { beforeEach, describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Fiber, Layer, Logger, ManagedRuntime, Option } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import type { SessionID } from "@/session/schema"
import {
  buildProductionPortBackends,
  createBunExecutorTransport,
  createBunExecutorWorkerPool,
  createEgressDeliveryBarrier,
  createHubEventSink,
  createProductionSessionHub,
  createProductionWorkerPool,
  createSessionBoundExecutorWorker,
  createSessionHub,
  createSessionHubLayer,
  hubProxyAuthoritiesFor,
  createWorkerCancellation,
  describeSessionWorkerEntry,
  DEFAULT_RUN_IDLE_TIMEOUT_MS,
  fallbackInstanceContext,
  makeSessionHubNode,
  productionSessionHubLayer,
  productionSessionHubNode,
  resolveRunIdleTimeoutMs,
  resolveSessionWorkerEntry,
  runIdleTimeoutWarning,
  serveEventsPort,
  spawnBunExecutorWorker,
  sweepDeadLeasesAtStartup,
  SESSION_CANCELLED_TYPE,
  SessionHubService,
  type ProductionPoolFactory,
  type SessionHub,
  type WorkerPortBackends,
  type WorkerTransferredPorts,
} from "@/session/worker/hub"
import { createLazyExecutorWorker, createLazyExecutorWorkerPool } from "../../lib/lazy-worker-pool"
import {
  ackFrame,
  blobPositionReferenceFrame,
  endFrame,
  eventFrame,
  MAX_WORKER_FRAME_BYTES,
  resyncFrame,
} from "@/session/worker/frame"
import { resolveSessionWorkerIdleTimeoutMs, SESSION_WORKER_IDLE_TIMEOUT_ENV } from "@/session/worker/pool"
import { createIdleIsolateCap } from "@/session/worker/worker-pools"
import { encodeFrame } from "@/session/egress/frame"
import { createMeshWorkerClient, type DurableCommit } from "@/session/egress/mesh"
import { createLiveEventPublisher, EventV2Bridge, type LiveEvent, type LiveEventPublisher } from "@/event-v2-bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { GlobalBus } from "@/bus/global"
import { SessionCancelledError, WorkerCrashedError } from "@/session/worker/dispatch"
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
import { SessionWorkerReplyTimeoutError } from "@/session/worker/execute"
import type { CancelFrame, CancelPort, CancelScheduler } from "@/session/worker/cancel"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { createPidRegistry, registerReported } from "@/session/worker/pid-registry"
import { workerLoggerFromBridge, workerLogMessage, type WorkerLogger } from "@/session/worker/log"
import { createWorkerPluginHost } from "@/session/worker/plugin-host"
import { sessionWorkerInit, assertStructuredCloneable } from "../../lib/session-worker-init"
import { Session } from "@/session/session"
import type { ShellInput } from "@/session/prompt"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { settleOf, sid } from "../../lib/async"
import { lineFor, recordingLogger } from "../../lib/recorders"
import { pollUntil } from "../../lib/poll"
import { createHubProxyClient } from "@/session/worker/hub-proxy-wire"
import { createFakeExecutorPool } from "../../lib/fake-worker-pool"
import type { HubProxyMethod } from "@/session/worker/hub-proxy-wire"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { createEgressReceiver } from "@/session/egress/receiver"
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
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { randomUUID } from "node:crypto"
import { openConnection } from "@opencode-ai/core/database/connection"
import { tmpdir } from "../../fixture/fixture"

const it = testEffect(TestLLMServer.layer)

/** The dependency-light executor fixture. */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)
/** The production worker entry (reduced layer as the work factory). */
const SESSION_WORKER = new URL("../../../src/session/worker/session-worker.ts", import.meta.url)


describe("12.1 the hub executes zero sessions", () => {
  it.live("work runs in the reduced-layer worker, never on the hub", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.text("hub-zero")
      const pool = createBunExecutorWorkerPool({
        entry: SESSION_WORKER,
        timeoutMs: 15_000,
        // `sessionWorkerInit` defaults the model identity (`providerID`/`modelID`/
        // `apiId` = `test-model`); only the transport is overridden to the mock
        // server. The entry's `modelConfig` reads the descriptor directly (D8
        // dropped the legacy `{model}` fallback).
        workerData: sessionWorkerInit({
          model: { baseUrl: llm.url, apiKey: "test-key" },
        }),
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

describe("12.2 per-session admission", () => {
  test("parent, sibling and depth-1 child run concurrently on distinct workers", async () => {
    const pool = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      const parent = hub.dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 0 })
      const sibling = hub.dispatcher.dispatch({ sessionID: sid("ses_sib"), depth: 0 })
      const child = hub.dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })

      await pollUntil(() => hub.dispatcher.busySlotCount === 3, { message: "sessions never ran" })
      expect(hub.dispatcher.queuedCount).toBe(0)
      // One worker per session: three sessions, three isolates.
      expect(pool.workers).toHaveLength(3)
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
    // Use the long-running work kind so a saturated scheduler still gives the
    // main-thread timer a real window; the 200ms "slow" kind could finish before
    // the timer got a turn under host load.
    const pool = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "verySlow", timeoutMs: 15_000 })
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
    const pool = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
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

  /** Clear the shared statics so an assertion never reads a previous test's worker. */
  static reset(): void {
    FakeWorker.last = undefined
    FakeWorker.instances.length = 0
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

// Every test starts from a clean FakeWorker fleet, so `last`/`instances` can never
// leak one test's workers into the next.
beforeEach(() => FakeWorker.reset())

/**
 * Swap `globalThis.Worker` for {@link FakeWorker} for the duration of `run`. A
 * test that builds its own pool (rather than a transport) uses this instead of
 * hand-rolling the swap, so the restore is always paired with the install.
 */
async function withGlobalFakeWorker(run: () => Promise<void>): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    await run()
  } finally {
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

async function withFakeWorker(
  run: (fake: FakeWorker, transport: ExecutorTransport) => Promise<void>,
  options: { readonly bufferCapacity?: number; readonly log?: WorkerLogger; readonly workerID?: number } = {},
): Promise<void> {
  await withGlobalFakeWorker(async () => {
    const transport = createBunExecutorTransport({
      entry: "fake://session-worker",
      ...(options.bufferCapacity === undefined ? {} : { bufferCapacity: options.bufferCapacity }),
      ...(options.log === undefined ? {} : { log: options.log }),
      ...(options.workerID === undefined ? {} : { workerID: options.workerID }),
    })
    const fake = FakeWorker.last
    if (fake === undefined) throw new Error("fake worker was never constructed")
    await run(fake, transport)
  })
}

/** The shared log recorder; `lineFor` fails loud when a line is missing. */
const recordingLog = recordingLogger

describe("12.x executor transport buffering", () => {
  test("a buffered frame is consumed by the first matching wait", async () => {
    await withFakeWorker(async (fake, transport) => {
      fake.emit({ marker: 1 })
      const first = await transport.waitFor((message) => markerOf(message) === 1)
      expect(markerOf(first)).toBe(1)
      const rejection = transport.waitFor((message) => markerOf(message) === 1, { timeoutMs: 20 })
      await expect(rejection).rejects.toThrow(/timed out/)
      await expect(rejection).rejects.toBeInstanceOf(SessionWorkerReplyTimeoutError)
    })
  })

  test("timeoutMs 0 means no deadline: a frame emitted after the call still resolves", async () => {
    await withFakeWorker(async (fake, transport) => {
      const pending = transport.waitFor((message) => markerOf(message) === 7, { timeoutMs: 0 })
      fake.emit({ marker: 7 })
      expect(markerOf(await pending)).toBe(7)
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

  test("a frame matching an outstanding waiter resolves even when the cap would drop it", async () => {
    await withFakeWorker(
      async (fake, transport) => {
        // The waiter is registered first; an unmatched frame fills the (zero)
        // buffer, then the awaited frame arrives. It must be matched before the
        // cap can evict it, rather than being discarded and starving the waiter.
        const pending = transport.waitFor((message) => markerOf(message) === 9, { timeoutMs: 0 })
        fake.emit({ marker: 8 })
        fake.emit({ marker: 9 })
        expect(markerOf(await pending)).toBe(9)
      },
      { bufferCapacity: 0 },
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
      // A cold worker import graph can outlast a tight budget on a loaded host;
      // the assertion is about the failure surfacing, not its latency.
      timeoutMs: 15_000,
      workerData: { snapshot: null, env: {}, model: { baseUrl: "http://localhost:1" } },
    })
    await expect(transport.waitFor(() => true)).rejects.toThrow(/snapshot is required/)
    await transport.terminate()
  })

  test("a worker boot/import failure is logged as an error with its workerID", async () => {
    const { log, lines } = recordingLog()
    await withFakeWorker(
      async (fake, transport) => {
        const pending = transport.waitFor(() => true)
        fake.fail("Cannot find module './session-worker.ts'", new Error("Cannot find module './session-worker.ts'"))

        await expect(pending).rejects.toThrow("session worker error")
        const line = lineFor(lines, "worker boot failed")
        expect(line.fields.workerID).toBe(11)
        expect(line.fields.level).toBe("ERROR")
        expect(String(line.fields.message)).toContain("Cannot find module")
      },
      { log, workerID: 11 },
    )
  })
})

// ---------------------------------------------------------------------------
// Production wiring: lazy pool, cancel, layer lifecycle.
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
const trackedLazyPool = (created: ImmediateWorker[]) =>
  createLazyExecutorWorkerPool({
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

describe("12 lazy pool", () => {
  test("constructing the hub spawns no workers until the first dispatch", async () => {
    const created: ImmediateWorker[] = []
    const hub = createSessionHub({ pool: trackedLazyPool(created) })

    expect(created).toHaveLength(0)

    const result = await hub.ensureRunning(sid("ses_lazy"))
    expect(result.sessionID).toBe("ses_lazy")
    expect(created.length).toBeGreaterThan(0)

    await hub.terminateAll()
  })
})

// ---------------------------------------------------------------------------
// startShell payload forwarding (FROZEN CONTRACT). The hub must carry the
// serialized `ShellInput` through `dispatch.startShell` to the worker's
// `ExecutorRun`, and keep the plain startShell path when the shell is absent.
// The executor run is the observable boundary: `dispatch.startShell` builds it
// from the payload, so asserting on it pins the forwarding end-to-end.
// ---------------------------------------------------------------------------

/** An in-memory executor pool that records every `ExecutorRun` a worker receives. */
const recordingExecutorPool = (runs: ExecutorRun[]): ExecutorWorkerPool => {
  const results = new Map<SessionID, SessionExecutionResult>()
  const worker: ExecutorWorker = {
    id: 1,
    alive: true,
    async execute(run) {
      runs.push(run)
      results.set(run.sessionID, { sessionID: run.sessionID, value: null })
    },
    async cancel() {},
    async assertNotBusy() {
      return false
    },
    resultFor: (sessionID) => results.get(sessionID),
    async terminate() {},
  }
  return {
    workers: [worker],
    spawn: () => worker,
    async terminateAll() {},
    byID: (id) => (id === worker.id ? worker : undefined),
    resultFor: (sessionID) => results.get(sessionID),
  }
}

describe("hub startShell payload forwarding", () => {
  test("forwards the ShellInput payload and the startShell mode to the worker", async () => {
    const runs: ExecutorRun[] = []
    const hub = createSessionHub({ pool: recordingExecutorPool(runs) })
    const sessionID = sid("ses_shell_fwd")
    const shell: ShellInput = { sessionID, agent: "build", command: "echo hi" }
    try {
      const result = await hub.startShell(sessionID, shell)
      expect(result.sessionID).toBe(sessionID)
      expect(runs).toHaveLength(1)
      // Exact shape: the payload rides the run, no extra field leaks.
      expect(runs[0]).toEqual({ sessionID, depth: 0, mode: "startShell", shell })
    } finally {
      await hub.terminateAll()
    }
  })

  test("an absent shell keeps the plain startShell path (no shell on the run)", async () => {
    const runs: ExecutorRun[] = []
    const hub = createSessionHub({ pool: recordingExecutorPool(runs) })
    const sessionID = sid("ses_shell_plain")
    try {
      await hub.startShell(sessionID)
      expect(runs).toHaveLength(1)
      expect(runs[0]).toEqual({ sessionID, depth: 0, mode: "startShell" })
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("13 cooperative cancel within grace", () => {
  test("cancelWithGrace stops a running session on a real worker", async () => {
    const pool = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
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
  })
})

// ---------------------------------------------------------------------------
// Production depth plumbing: a depth ≥ 1 child must materialize its own
// per-session worker. Depth rides the run metadata; scheduling has no split.
// ---------------------------------------------------------------------------

describe("production depth plumbing", () => {
  test("a depth ≥ 1 child admitted through the hub materializes its own worker", async () => {
    const childID = sid("ses_w11_depth_child")
    const created: ImmediateWorker[] = []
    const hub = createSessionHub({ pool: trackedLazyPool(created) })
    try {
      const result = await hub.ensureRunning(childID)

      expect(result.sessionID).toBe(childID)
      // Depth rides the run metadata; scheduling has no headroom split.
      expect(created).toHaveLength(1)
    } finally {
      await hub.terminateAll()
    }
  })

  test("a depth 0 root also materializes its own worker", async () => {
    const sessionID = sid("ses_w11_root")
    const created: ImmediateWorker[] = []
    const hub = createSessionHub({ pool: trackedLazyPool(created) })
    try {
      const result = await hub.ensureRunning(sessionID)

      expect(result.sessionID).toBe(sessionID)
      expect(created).toHaveLength(1)
    } finally {
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// Hub-side egress mesh. The worker's `egressControl` port is wired; the
// hub must create ONE mesh, attach each worker's control port to it, and publish
// durable commits on the commit path so `events.durable()` subscribers wake.
// ---------------------------------------------------------------------------

describe("production egress mesh", () => {
  test("the production egress backend attaches a mesh and publishes durable commits under load", async () => {
    const { publisher, published } = recordingPublisher()
    const backends = buildProductionPortBackends({
      events: publisher,
      // A tiny bound makes the overflow (resync) path deterministic without
      // driving hundreds of frames.
      egressMeshBound: 2,
    })
    const data = new MessageChannel()
    const control = new MessageChannel()
    const dispose = backends.egress!(1, data.port1, control.port1)
    const mesh = createMeshWorkerClient(control.port2)
    const commits: DurableCommit[] = []
    const resyncs: string[] = []
    mesh.onDurable((commit) => void commits.push(commit))
    mesh.onResync((pattern) => void resyncs.push(pattern))
    try {
      await mesh.listen("*")

      // Three durable commits against a bound of two: the first two deliver to
      // the attached worker, the third overflows and is answered with a resync.
      for (let ordinal = 1; ordinal <= 3; ordinal++) {
        data.port2.postMessage(
          encodeFrame(
            eventFrame({
              event: { id: `evt_${ordinal}`, type: "session.message", properties: { ordinal } },
              aggregateID: "agg",
              seq: ordinal,
            }),
          ),
        )
      }

      await pollUntil(() => resyncs.length === 1, { message: "mesh never overflowed to a resync" })
      expect(commits.map((commit) => commit.meta.seq)).toEqual([1, 2])
      expect(resyncs).toEqual(["*"])
      // The hub still republishes every durable event live-only (the mesh is an
      // additive wakeup channel, not a replacement for the event surface).
      expect(published).toHaveLength(3)

      // An ack frees a backlog slot, so the next durable commit lands again.
      mesh.acknowledgeDelivery()
      data.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: { id: "evt_4", type: "session.message", properties: { ordinal: 4 } },
            aggregateID: "agg",
            seq: 4,
          }),
        ),
      )
      await pollUntil(() => commits.length === 3, { message: "ack never freed a delivery slot" })
      expect(commits.map((commit) => commit.meta.seq)).toEqual([1, 2, 4])
    } finally {
      mesh.dispose()
      dispose()
      data.port1.close()
      data.port2.close()
      control.port1.close()
      control.port2.close()
      await backends.dispose()
    }
  })

  test("the hub event sink's ack/resync counters become non-zero under load", async () => {
    const { publisher } = recordingPublisher()
    const sink = createHubEventSink({ publisher })
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      // Load: many deltas, then the two flow-control frames the worker sends back.
      for (let i = 0; i < 50; i++) {
        channel.port2.postMessage(
          encodeFrame(eventFrame({ event: { id: `delta_${i}`, type: "message.part.delta", properties: { i } } })),
        )
      }
      channel.port2.postMessage(encodeFrame(resyncFrame({ pattern: "message.*" })))
      channel.port2.postMessage(encodeFrame(ackFrame({ count: 50 })))

      await pollUntil(() => sink.acks > 0 && sink.resyncs > 0, {
        message: "sink flow-control counters never advanced",
      })
      expect(sink.resyncs).toBe(1)
      expect(sink.acks).toBe(1)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})

describe("egress ack correlation", () => {
  test("an ack for a resynced frame does not credit a later run", async () => {
    const sessionID = "ses_egress_drift" as SessionID
    const barrier = createEgressDeliveryBarrier({ timeoutMs: 1_000 })

    // Run 1: one durable observed, but the sender's batch ack includes a
    // second frame that resynced instead of reaching the live sink.
    barrier.observeDurable(sessionID)
    barrier.observeAck(sessionID, 2)
    await barrier.wait(sessionID)

    // Run 2: the stale extra ack must not be carried forward as credit.
    barrier.observeDurable(sessionID)
    let resolved = false
    const wait = barrier.wait(sessionID).then(() => {
      resolved = true
    })
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(resolved).toBe(false)

    barrier.observeAck(sessionID, 1)
    await wait
    expect(resolved).toBe(true)
  })

  test("waits for observed durables when the same batch also carried a resynced reference", async () => {
    const sessionID = "ses_egress_reference" as SessionID
    const { publisher } = recordingPublisher()
    const barrier = createEgressDeliveryBarrier({ timeoutMs: 1_000 })
    const backends = buildProductionPortBackends({
      events: publisher,
      egressDelivery: barrier,
      sessionOfWorker: () => sessionID,
    })
    const channel = new MessageChannel()
    const dispose = backends.egress!(1, channel.port1)
    try {
      // A durable body that cannot resolve by position is surfaced as a resync:
      // the sender still counts it in its batch ack.
      const reference = blobPositionReferenceFrame(
        eventFrame({
          event: { id: "evt_ref", type: "session.updated", properties: { sessionID } },
          aggregateID: sessionID,
          seq: 1,
        }),
        MAX_WORKER_FRAME_BYTES + 1,
      )
      if (reference === undefined) throw new Error("oversized durable did not produce a reference")
      channel.port2.postMessage(encodeFrame(reference))
      channel.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: { id: "evt_ok", type: "session.updated", properties: { sessionID } },
            aggregateID: sessionID,
            seq: 2,
          }),
        ),
      )
      await new Promise((resolve) => setTimeout(resolve, 20))

      let resolved = false
      const wait = barrier.wait(sessionID).then(() => {
        resolved = true
      })
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(resolved).toBe(false)

      // The ack covers both hand-offs; the resynced one is settled without
      // crediting a future batch.
      channel.port2.postMessage(encodeFrame(ackFrame({ count: 2 })))
      await wait
      expect(resolved).toBe(true)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})

describe("14 makeSessionHubNode", () => {
  test("provides SessionHubService and terminates the pool on scope close", async () => {
    const created: ImmediateWorker[] = []
    const node = makeSessionHubNode({ pool: trackedLazyPool(created) })

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
  test("closing the layer scope terminates every per-session worker", async () => {
    const created: ImmediateWorker[] = []

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(createSessionHubLayer({ pool: trackedLazyPool(created) }))
          const hub = Context.get(context, SessionHubService)
          yield* Effect.promise(() =>
            Promise.all([hub.ensureRunning(sid("ses_life_1")), hub.ensureRunning(sid("ses_life_2"))]),
          )
        }),
      ),
    )

    expect(created).toHaveLength(2)
    expect(created.every((worker) => worker.terminated)).toBe(true)
  })
})

describe("35a worker port serving", () => {


  test("serves the hub-proxy authorities over a worker control port", async () => {
    const hub = createSessionHub({
      pool: trackedLazyPool([]),
      ports: { hubProxy: { "sessionStatus.get": () => "pong" } },
    })
    const channel = new MessageChannel()
    const dispose = hub.attachHubProxyPort(1, channel.port1)
    const client = createHubProxyClient(channel.port2)

    expect(await client.call("sessionStatus.get", [])).toBe("pong")
    const missing = await client.call("missing" as HubProxyMethod, []).then(
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
    const hub = createSessionHub({ pool: trackedLazyPool([]) })
    const proxyChannel = new MessageChannel()
    const disposePorts = hub.attachWorkerPorts(1, {})
    const disposeProxy = hub.attachHubProxyPort(1, proxyChannel.port1)
    expect(typeof disposePorts).toBe("function")
    expect(typeof disposeProxy).toBe("function")
    disposePorts()
    disposeProxy()
    proxyChannel.port1.close()
    proxyChannel.port2.close()
  })
})

describe("42 crash/respawn end-to-end", () => {
  test("killing a worker mid-dispatch respawns it and completes the next session", async () => {
    const pool = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      const first = hub.dispatcher.dispatch({ sessionID: sid("ses_crash"), depth: 0 })
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_crash"))?.state === "running", {
        message: "session never started",
      })

      const workerID = hub.dispatcher.registry.get(sid("ses_crash"))!.workerID!
      // Attach the rejection handler before terminating: the orderly close takes
      // a bounded tick, so an unobserved rejection would be reported as unhandled.
      const crashed = first.then(
        () => {
          throw new Error("expected the killed run to reject")
        },
        (error: unknown) => {
          expect(error).toBeInstanceOf(WorkerCrashedError)
        },
      )
      await pool.byID(workerID)!.terminate()
      await crashed

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
// 36 production port backends: capabilities + broker/egress round-trip.
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
  [
    Config.node,
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({ model: "test/model" } as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
  ],
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
  // Hub-proxy authorities. `SessionStatus.get` is left
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
      create: async (id) => {
        captured.capabilities = (await resolveInit()).capabilities
        captured.created.push(id)
        if (emitPorts) {
          const egress = new MessageChannel()
          sinks.onWorkerPorts(id, { egress: egress.port2 })
          captured.ports = { egress: egress.port1 }
        }
        return new ImmediateWorker(id)
      },
    })

/** A production pool factory whose worker transfers only its hub-proxy control port. */
const hubProxyPoolFactory =
  (captured: CapturedPorts): ProductionPoolFactory =>
  (resolveInit, sinks) =>
    createLazyExecutorWorkerPool({
      create: async (id) => {
        captured.capabilities = (await resolveInit()).capabilities
        captured.created.push(id)
        const channel = new MessageChannel()
        sinks.onHubProxyPort(id, channel.port2)
        captured.hubProxy = channel.port1
        return new ImmediateWorker(id)
      },
    })

describe("36 production port backends", () => {
  test("productionSessionHubNode is exported and provides SessionHubService", () => {
    expect(typeof productionSessionHubNode).toBe("function")
    expect(typeof makeSessionHubNode).toBe("function")
    expect(productionSessionHubNode().service).toBe(SessionHubService)
  })

  test("the production node advertises sessionWorkerPorts and round-trips egress", async () => {
    const captured: CapturedPorts = { created: [] }
    const egressFrames: unknown[] = []
    const backends: WorkerPortBackends = {
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
            expect(ports?.egress).toBeDefined()

            yield* Effect.promise(async () => {
              // Egress port round-trip: the emitted delta reaches the hub receiver.
              const egress = createWorkerEgressClient(ports!.egress!, { windowMs: 0 })
              egress.emit({ id: "x1", type: "session.text", properties: { text: "hi" } })
              egress.end()
              await pollUntil(() => egressFrames.length > 0, { message: "egress frame never drained" })
              expect(egressFrames.length).toBeGreaterThan(0)
              egress.dispose()

              ports!.egress!.close()
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
                // Building the port backends does no eager work; the
                // hub-proxy handlers come from the layer's real service deps.
              }),
            )
            const hub = Context.get(context, SessionHubService)
            yield* Effect.promise(() => hub.ensureRunning(sid("ses_hubproxy")))
            yield* Effect.promise(async () => {
              const client = createHubProxyClient(captured.hubProxy!)
              expect(await client.call("sessionStatus.get", [sid("ses_hubproxy")])).toEqual({ type: "idle" })
              client.dispose()
            })
          }),
        ),
      ),
    )
  })

  test("buildProductionPortBackends defaults to a draining egress", async () => {
    const backends = buildProductionPortBackends()

    expect(backends.egress).toBeDefined()
    const channel = new MessageChannel()
    const dispose = backends.egress!(1, channel.port1)
    // The default egress drains without a real sink; emitting is a no-op.
    channel.port1.postMessage(new Uint8Array(0))
    dispose()
    channel.port1.close()
    channel.port2.close()

    await backends.dispose()
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

  test("a durable worker event keeps its committed aggregate identity on republish", () => {
    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({ publisher })

    sink.event(
      { id: "evt_9", type: "session.next.prompted", properties: { sessionID: "ses_a" } },
      { durable: true, aggregateID: "ses_a", seq: 7 },
    )

    const republished = published[0] as unknown as {
      readonly durable?: { readonly aggregateID: string; readonly seq: number; readonly version: number }
    }
    expect(republished.durable).toEqual({ aggregateID: "ses_a", seq: 7, version: 1 })
    // A non-durable event still carries no durable identity.
    sink.event({ id: "evt_10", type: "session.error", properties: { message: "boom" } })
    expect((published[1] as unknown as { durable?: unknown }).durable).toBeUndefined()
  })

  test("a durable frame's aggregate wins over the payload's sessionID for cache invalidation", () => {
    const invalidated: string[] = []
    const { publisher } = recordingPublisher()
    const sink = createHubEventSink({ publisher, cache: { invalidate: (id) => void invalidated.push(id) } })

    sink.event(
      { id: "evt_11", type: "session.next.prompted", properties: { sessionID: "ses_payload" } },
      { durable: true, aggregateID: "ses_committed", seq: 3 },
    )

    expect(invalidated).toEqual(["ses_committed"])
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

  test("a position reference without a resolver resyncs instead of publishing raw data", async () => {
    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({ publisher })
    const reference = {
      __eventBlobPosition: { aggregateID: "ses_big", seq: 1 },
      frameBytes: MAX_WORKER_FRAME_BYTES + 1,
    }

    // A direct call must never hand the raw reference to the live publisher.
    sink.event({ id: "evt_big", type: "session.next.prompted", properties: reference })
    expect(sink.resyncs).toBe(1)
    expect(published).toEqual([])

    // The same guard applies through the events-port pipeline.
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      channel.port2.postMessage(
        encodeFrame(
          eventFrame({
            event: { id: "evt_big", type: "session.next.prompted", properties: reference },
            aggregateID: "ses_big",
            seq: 1,
          }),
        ),
      )
      await pollUntil(() => sink.resyncs === 2, { message: "the events port published a raw reference" })
      expect(published).toEqual([])
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
    // EventV2 probes the schema once at layer construction through the Effect
    // client (`db.$client.unsafe`). Emulate that production surface with empty
    // probe results; every real database access still explodes, so a durable
    // commit on the hub fails loudly.
    const probeOnlyClient = { unsafe: () => Effect.succeed([] as Record<string, unknown>[]) }
    const commitGuard = Layer.succeed(Database.Service, {
      db: new Proxy(
        {},
        {
          get: (_, property) =>
            property === "$client"
              ? probeOnlyClient
              : () => {
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
            aggregateID: "agg",
            seq: 9,
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
    const pool = createBunExecutorWorkerPool({ entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
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
      // The run finished on its own, so the dispatch promise resolves (no error).
      expect(await settled).toBeUndefined()
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
// Production cancellation authorities.
// ---------------------------------------------------------------------------

/** The shared settlement primitive, adapted to this suite's rejection-or-undefined shape. */
const rejectionOf = async (promise: Promise<unknown>): Promise<unknown> => {
  const settled = await settleOf(promise)
  return settled.ok ? undefined : settled.error
}

describe("cancellation authorities", () => {
  test("an explicit childrenOf drives the children-first cascade", async () => {
    const parentID = sid("ses_parent")
    const childID = sid("ses_child")
    const cancelled: string[] = []
    const pool = new ControllablePool(1, "resolve")
    const hub = createSessionHub({
      pool,
      cancellation: {
        childrenOf: (id) => (id === parentID ? [childID] : []),
        onCancelled: (event) => cancelled.push(event.sessionID),
      },
    })
    try {
      const parent = rejectionOf(hub.dispatcher.dispatch({ sessionID: parentID, depth: 0 }))
      const child = rejectionOf(hub.dispatcher.dispatch({ sessionID: childID, depth: 1 }))
      await pollUntil(() => hub.dispatcher.registry.get(childID)?.state === "running", {
        message: "child never started",
      })

      await hub.cancel(parentID)

      // Children-first cascade reached the child; both sessions settled.
      expect(await child).toBeInstanceOf(SessionCancelledError)
      expect(await parent).toBeInstanceOf(SessionCancelledError)
      expect(cancelled).toEqual([childID, parentID])
    } finally {
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// The dispatching session's model is threaded into the worker init.
// ---------------------------------------------------------------------------

describe("session model threading", () => {
  it.live("buildSessionWorkerInit receives the dispatching session's model", () =>
    Effect.gen(function* () {
      const captured: SessionWorkerInit[] = []
      const poolFactory: ProductionPoolFactory = (resolveInit) =>
        createLazyExecutorWorkerPool({
          create: async (id) => {
            captured.push(await resolveInit())
            return new ImmediateWorker(id)
          },
        })

      const deps = Layer.mergeAll(
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
          directories: () => Effect.succeed(["/cfg/opencode"]),
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
// Per-session worker model: a worker rebinds when a different
// session (which may carry a different model) is dispatched to it.
// ---------------------------------------------------------------------------

describe("per-session worker model", () => {
  test("one worker rebinds for a second session and resolves that session's model", async () => {
    const resolvedSessions: string[] = []
    const pool = createProductionWorkerPool({
      init: (sessionID) => {
        resolvedSessions.push(String(sessionID))
        return sessionWorkerInit({ model: { modelID: `model-${sessionID}` } })
      },
      entry: new URL("../../fixture/session-execute-worker.ts", import.meta.url),
      workKind: "instant",
      timeoutMs: 15_000,
    })
    try {
      const worker = pool.spawn()
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
// Fix 1 (per-session working directory) — the hub must resolve each session's
// worker init under that session's OWN instance, never the first dispatch's.
// The bug latched `capturedInstance ??= captureInstanceRef()` once per process,
// so a second project inherited the first project's directory. `snapshot.directory`
// is the observable seam (`buildWorkerSnapshot` reads the ambient `InstanceRef`).
// ---------------------------------------------------------------------------

describe("Fix 1 per-session working directory", () => {
  /** An ambient InstanceRef whose only meaningful field for the snapshot is `directory`. */
  const ambient = (directory: string) => ({ directory, worktree: directory, project: {} as never })

  /**
   * A pool whose single worker records the init resolved for the session it is
   * asked to run. `resolveInit(run.sessionID)` is the production seam the real
   * pool drives, so the recording pins the per-session resolution end-to-end.
   */
  const recordingInitPool =
    (captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }>): ProductionPoolFactory =>
    (resolveInit) => {
      const results = new Map<SessionID, SessionExecutionResult>()
      return createLazyExecutorWorkerPool({
        create: (id) => ({
          id,
          alive: true,
          execute: async (run) => {
            captured.push({ sessionID: run.sessionID, init: await resolveInit(run.sessionID) })
            results.set(run.sessionID, { sessionID: run.sessionID, value: null })
          },
          cancel: async () => {},
          assertNotBusy: async () => false,
          resultFor: (sessionID) => results.get(sessionID),
          terminate: async () => {},
        }),
      })
    }

  const deps = Layer.mergeAll(
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
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

  /** A `Session` stub whose `get` returns a per-session persisted directory. */
  const sessionLayer = (directoryBySession: Map<SessionID, string>) =>
    Layer.succeed(
      Session.Service,
      Session.Service.of({
        get: (sessionID: SessionID) =>
          Effect.succeed({
            id: sessionID,
            directory: directoryBySession.get(sessionID) ?? "/persisted/unknown",
            projectID: "proj-test",
            model: { id: "m", providerID: "p" },
          } as unknown as Session.Info),
        findMessage: () => Effect.succeed(Option.none()),
      } as unknown as Session.Interface),
    )

  it.live("resolves each session under its own ambient InstanceRef, never the first", () =>
    Effect.gen(function* () {
      const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []
      const layer = productionSessionHubLayer({ count: 1, poolFactory: recordingInitPool(captured) }).pipe(
        Layer.provide(deps),
      )
      const sessionA = sid("ses_fixdir_a")
      const sessionB = sid("ses_fixdir_b")
      const sessions = sessionLayer(
        new Map([
          [sessionA, "/persisted/a"],
          [sessionB, "/persisted/b"],
        ]),
      )

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          yield* Effect.promise(() => hub.ensureRunning(sessionA)).pipe(
            Effect.provide(sessions),
            Effect.provideService(InstanceRef, ambient("/ambient/a")),
          )
          yield* Effect.promise(() => hub.ensureRunning(sessionB)).pipe(
            Effect.provide(sessions),
            Effect.provideService(InstanceRef, ambient("/ambient/b")),
          )
        }),
      )

      // The second session must NOT inherit the first dispatch's directory.
      expect(captured.map((entry) => entry.init.snapshot.directory)).toEqual(["/ambient/a", "/ambient/b"])
    }),
  )

  it.live("falls back to the session's own persisted directory when no ambient InstanceRef exists", () =>
    Effect.gen(function* () {
      const captured: Array<{ sessionID: SessionID; init: SessionWorkerInit }> = []
      const layer = productionSessionHubLayer({ count: 1, poolFactory: recordingInitPool(captured) }).pipe(
        Layer.provide(deps),
      )
      const sessionA = sid("ses_fixdir_c")
      const sessionB = sid("ses_fixdir_d")
      const sessions = sessionLayer(
        new Map([
          [sessionA, "/persisted/c"],
          [sessionB, "/persisted/d"],
        ]),
      )

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer)
          const hub = Context.get(context, SessionHubService)
          yield* Effect.promise(() => hub.ensureRunning(sessionA)).pipe(Effect.provide(sessions))
          yield* Effect.promise(() => hub.ensureRunning(sessionB)).pipe(Effect.provide(sessions))
        }),
      )

      // No ambient instance: each session resolves from its OWN persisted directory.
      expect(captured.map((entry) => entry.init.snapshot.directory)).toEqual(["/persisted/c", "/persisted/d"])
    }),
  )
})

// ---------------------------------------------------------------------------
// The hub ships the resolved plugin effect into the worker init.
// ---------------------------------------------------------------------------

describe("plugin effect shipping", () => {
  it.live("ships the resolved plugin effect in the worker init", () =>
    Effect.gen(function* () {
      const captured: SessionWorkerInit[] = []
      const poolFactory: ProductionPoolFactory = (resolveInit) =>
        createLazyExecutorWorkerPool({
          create: async (id) => {
            captured.push(await resolveInit())
            return new ImmediateWorker(id)
          },
        })

      const deps = Layer.mergeAll(
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
          directories: () => Effect.succeed(["/cfg/opencode"]),
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
        // This fake pool never transfers a hub-proxy port, so the worker has no
        // per-request bridge and the init must ship the resolved boot diff.
        ports: {},
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
// A worker pid-report on the control port fills the
// hub mirror so reap actually has targets.
// ---------------------------------------------------------------------------

describe("pid-report receive", () => {
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
// Fixes A/D — define-aware worker entry and error cause.
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

describe("describeSessionWorkerEntry reports the resolution source", () => {
  test("an env override yields source \"env\" and the same entry", () => {
    const described = describeSessionWorkerEntry({ env: { OPENCODE_SESSION_WORKER_PATH: "/opt/worker.js" } })
    expect(described).toEqual({ entry: "/opt/worker.js", source: "env" })
    expect(described.entry).toBe(resolveSessionWorkerEntry({ env: { OPENCODE_SESSION_WORKER_PATH: "/opt/worker.js" } }))
  })

  test("a build-time define yields source \"definedPath\"", () => {
    const described = describeSessionWorkerEntry({ env: {}, definedPath: "/$bunfs/root/session-worker.ts" })
    expect(described).toEqual({ entry: "/$bunfs/root/session-worker.ts", source: "definedPath" })
  })

  test("neither set yields source \"default\" and the source-relative isolate", () => {
    const described = describeSessionWorkerEntry({ env: {} })
    expect(described.source).toBe("default")
    expect(described.entry).toBeInstanceOf(URL)
    expect(String(described.entry)).toContain("session-worker.ts")
  })

  test("an empty env override falls through to the default source", () => {
    const described = describeSessionWorkerEntry({ env: { OPENCODE_SESSION_WORKER_PATH: "" } })
    expect(described.source).toBe("default")
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
// A dispatch with no ambient `InstanceRef` must still
// ship a REAL project identity to the worker, never the directory string (which
// would leak into the worker snapshot as a project id).
// ---------------------------------------------------------------------------

describe("fallbackInstanceContext", () => {
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
    await withGlobalFakeWorker(async () => {
      const pool = createProductionWorkerPool({
        init: () => sessionWorkerInit(),
        entry: "fake://workerid",
        workKind: "instant",
        timeoutMs: 5_000,
      })
      try {
        // Fire both runs but never await: the fake isolate never replies, and the
        // only thing under test is the `workerData` the pool hands to `new Worker`.
        // Terminating the pool below rejects these pending runs (all caught).
        const workers = Array.from({ length: 2 }, () => pool.spawn())
        for (const [index, worker] of workers.entries()) {
          void worker.execute({ sessionID: sid(`ses_wid_${index}`), depth: 0 }).catch(() => undefined)
        }
        await pollUntil(() => FakeWorker.instances.length === 2, { message: "workers never spawned" })

        const workerIDs = FakeWorker.instances.map(
          (worker) => (worker.options?.workerData as { workerID?: number } | undefined)?.workerID,
        )
        // Slot ids 1 and 2 — never the isolate threadId.
        expect(workerIDs).toEqual([1, 2])
      } finally {
        await pool.terminateAll()
      }
    })
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
          create: async (id) => {
            captured.push(await resolveInit())
            return new ImmediateWorker(id)
          },
        })

      const deps = Layer.mergeAll(
        Layer.mock(Config.Service, {
          get: () => Effect.succeed({ model: "config/default" } as ConfigV1.Info),
          directories: () => Effect.succeed(["/cfg/opencode"]),
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
        // This fake pool never transfers a hub-proxy port, so the worker has no
        // per-request bridge and the init must resolve the boot diff eagerly.
        ports: {},
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

// ---------------------------------------------------------------------------
// `cancelWithReport` reconstructs the subagent cascade's settlement.
// The dispatcher discards a descendant's own outcome, so the hub reports which
// descendants still need the caller's fallback idle. `run-state.ts` consumes it.
// ---------------------------------------------------------------------------

describe("cancelWithReport", () => {
  test("a cooperatively settled child needs no fallback idle; the parent's outcome is returned", async () => {
    const parentID = sid("ses_rep_parent")
    const childID = sid("ses_rep_child")
    // Two per-session workers: the parent and the child each own one.
    const pool = new ControllablePool(2, "resolve", true)
    const hub = createSessionHub({
      pool,
      cancellation: {
        childrenOf: (id) => (id === parentID ? [childID] : []),
        portFor: (worker) => (worker as ControllableWorker).cancelPort,
        graceMs: 500,
        schedule: neverSchedule,
      },
    })
    try {

      const parent = rejectionOf(hub.dispatcher.dispatch({ sessionID: parentID, depth: 0 }))
      const child = rejectionOf(hub.dispatcher.dispatch({ sessionID: childID, depth: 1 }))
      await pollUntil(() => hub.dispatcher.registry.get(childID)?.state === "running", {
        message: "child never started",
      })

      const report = await hub.cancelWithReport(parentID)

      expect(report.outcome).toMatchObject({ sessionID: parentID, mode: "cooperative", winner: "cancel" })
      // The child's worker acked (settled cooperatively), so it published its own
      // idle: the caller must not write a second one.
      expect(report.descendants).toEqual([{ sessionID: childID, needsFallbackIdle: false }])
      expect(await child).toBeInstanceOf(SessionCancelledError)
      expect(await parent).toBeInstanceOf(SessionCancelledError)
    } finally {
      await hub.terminateAll()
    }
  })

  test("a linked child that never ran needs the fallback idle", async () => {
    const parentID = sid("ses_unset_parent")
    const childID = sid("ses_unset_child")
    const pool = new ControllablePool(1, "resolve", true)
    const hub = createSessionHub({
      pool,
      cancellation: {
        childrenOf: (id) => (id === parentID ? [childID] : []),
        portFor: (worker) => (worker as ControllableWorker).cancelPort,
        graceMs: 500,
        schedule: neverSchedule,
      },
    })
    try {

      const parent = rejectionOf(hub.dispatcher.dispatch({ sessionID: parentID, depth: 0 }))
      await pollUntil(() => hub.dispatcher.registry.get(parentID)?.state === "running", {
        message: "parent never started",
      })

      const report = await hub.cancelWithReport(parentID)

      // The child was never admitted, so no worker could publish its idle.
      expect(report.descendants).toEqual([{ sessionID: childID, needsFallbackIdle: true }])
      expect(await parent).toBeInstanceOf(SessionCancelledError)
    } finally {
      await hub.terminateAll()
    }
  })

  test("D3 logs the cancel cascade with descendant and fallback-idle counts", async () => {
    const parentID = sid("ses_cascade_parent")
    const childID = sid("ses_cascade_child")
    const { log, lines } = recordingLog()
    const pool = new ControllablePool(1, "resolve", true)
    const hub = createSessionHub({
      pool,
      log,
      cancellation: {
        childrenOf: (id) => (id === parentID ? [childID] : []),
        portFor: (worker) => (worker as ControllableWorker).cancelPort,
        graceMs: 500,
        schedule: neverSchedule,
      },
    })
    try {

      const parent = rejectionOf(hub.dispatcher.dispatch({ sessionID: parentID, depth: 0 }))
      await pollUntil(() => hub.dispatcher.registry.get(parentID)?.state === "running", {
        message: "parent never started",
      })

      await hub.cancelWithReport(parentID)

      const line = lineFor(lines, "cancel cascade")
      expect(line.fields).toEqual({ level: "INFO", sessionID: parentID, descendants: 1, needsFallbackIdle: 1 })
      expect(await parent).toBeInstanceOf(SessionCancelledError)
    } finally {
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// Hub observability: session-bound worker rebind and hub port connections.
// ---------------------------------------------------------------------------

describe("rebind and ports", () => {
  test("logs a rebound worker's session transition", async () => {
    const { log, lines } = recordingLog()
    const worker = createSessionBoundExecutorWorker(5, async (id) => new ImmediateWorker(id), log)
    try {
      await worker.execute({ sessionID: sid("ses_a"), depth: 0 })
      await worker.execute({ sessionID: sid("ses_b"), depth: 0 })

      const rebound = lineFor(lines, "worker rebound").fields
      expect(rebound).toEqual({
        level: "INFO",
        workerID: 5,
        fromSession: "ses_a",
        toSession: "ses_b",
        sessionID: "ses_b",
      })
      // The rebound-to session rides the hoisted `sessionID` field so the log
      // seam prefixes this line with it, while `fromSession`/`toSession` stay.
      expect(rebound.sessionID).toBe(rebound.toSession)
    } finally {
      await worker.terminate()
    }
  })

  test("renders the rebound line through workerLoggerFromBridge with the bare ULID prefix", async () => {
    const rendered: Array<{ readonly logLevel: string; readonly message: unknown }> = []
    // A bridge whose `fork` runs the logging effect synchronously under a
    // capturing logger, so this test observes what `workerLoggerFromBridge`
    // emits in production: both concurrent edits composed end-to-end.
    const capturingBridge: EffectBridge.Shape = {
      promise: () => Promise.reject(new Error("capturing bridge: promise is unused")),
      fork: <A, E, R>(effect: Effect.Effect<A, E, R>): Fiber.Fiber<A, E> => {
        Effect.runSync(
          (effect as unknown as Effect.Effect<A, E, never>).pipe(
            Effect.provide(
              Logger.layer([
                Logger.make<unknown, void>(({ logLevel, message }) => {
                  rendered.push({ logLevel, message })
                }),
              ]),
            ),
          ),
        )
        return undefined as unknown as Fiber.Fiber<A, E>
      },
      run: <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E> =>
        effect as unknown as Effect.Effect<A, E>,
      bind: <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) => fn,
    }
    const worker = createSessionBoundExecutorWorker(
      7,
      async (id) => new ImmediateWorker(id),
      workerLoggerFromBridge(capturingBridge),
    )
    try {
      await worker.execute({ sessionID: sid("ses_a"), depth: 0 })
      await worker.execute({ sessionID: sid("ses_b"), depth: 0 })

      const rebound = rendered.find(
        (entry) => Array.isArray(entry.message) && entry.message[0] === "session workers[b]: worker rebound",
      )
      expect(rebound).toBeDefined()
      // The prefix shows only the bare ULID; the full branded id is retained as a field.
      expect(rebound?.message).toEqual([
        "session workers[b]: worker rebound",
        { workerID: 7, fromSession: "ses_a", toSession: "ses_b", sessionID: "ses_b" },
      ])
      expect(rebound?.logLevel).toBe("Info")
    } finally {
      await worker.terminate()
    }
  })

  test("does not log a rebind when the same session is reused", async () => {
    const { log, lines } = recordingLog()
    const worker = createSessionBoundExecutorWorker(6, async (id) => new ImmediateWorker(id), log)
    try {
      await worker.execute({ sessionID: sid("ses_same"), depth: 0 })
      await worker.execute({ sessionID: sid("ses_same"), depth: 0 })

      expect(lines.some((entry) => entry.message === workerLogMessage("worker rebound"))).toBe(false)
    } finally {
      await worker.terminate()
    }
  })

  test("logs each connected hub port under its worker id", async () => {
    const { log, lines } = recordingLog()
    const hub = createSessionHub({ pool: trackedLazyPool([]), log })
    const egress = new MessageChannel()
    const hubProxy = new MessageChannel()
    try {
      hub.attachWorkerPorts(4, { egress: egress.port1 })
      hub.attachHubProxyPort(5, hubProxy.port1)

      const connected = lines.filter((entry) => entry.message === workerLogMessage("ports connected"))
      const portsByWorker = new Map(connected.map((entry) => [entry.fields.workerID, entry.fields.ports]))
      expect(portsByWorker.get(4)).toEqual(["egress"])
      expect(portsByWorker.get(5)).toEqual(["hub-proxy"])
    } finally {
      egress.port1.close()
      egress.port2.close()
      hubProxy.port1.close()
      hubProxy.port2.close()
      await hub.terminateAll()
    }
  })
})

// ---------------------------------------------------------------------------
// The production spawn/cancel helpers must forward their
// `log` seam so `worker spawning` and `cancel outcome` actually fire.
// Before the wiring: `spawnBunExecutorWorker` built its executor without `log`
// (spawning dormant), and `createSessionBoundExecutorWorker` / `createLazyExecutorWorker`
// built their cancel ports without `log` (D2 dormant).
// ---------------------------------------------------------------------------

describe("hub wiring — forward log through production paths", () => {
  test("spawnBunExecutorWorker forwards log into createExecutorWorker", async () => {
    await withGlobalFakeWorker(async () => {
      const { log, lines } = recordingLog()
      const worker = spawnBunExecutorWorker(7, { entry: "fake://spawn", workKind: "slow", log })
      try {
        expect(lineFor(lines, "worker spawning").fields).toEqual({ workerID: 7, kind: "slow", reason: "initial" })
      } finally {
        await worker.terminate()
      }
    })
  })

  test("D2 createSessionBoundExecutorWorker forwards log into its cancel port", async () => {
    const { log, lines } = recordingLog()
    const worker = createSessionBoundExecutorWorker(5, async (id) => new ImmediateWorker(id), log)
    try {
      await worker.execute({ sessionID: sid("ses_d2"), depth: 0 })
      await worker.cancelPort!.notify({ type: "cancel", sessionID: sid("ses_d2") })
      await pollUntil(() => lines.some((entry) => entry.message === workerLogMessage("cancel outcome")), {
        message: "cancel outcome never logged",
      })
      expect(lineFor(lines, "cancel outcome").fields).toEqual({
        sessionID: "ses_d2",
        mode: "cooperative",
        accepted: true,
      })
    } finally {
      await worker.terminate()
    }
  })

  test("D2 createLazyExecutorWorker forwards log into its cancel port", async () => {
    const { log, lines } = recordingLog()
    const worker = createLazyExecutorWorker(9, async () => new ImmediateWorker(9), log)
    try {
      await worker.cancelPort!.notify({ type: "cancel", sessionID: sid("ses_lazy_d2") })
      await pollUntil(() => lines.some((entry) => entry.message === workerLogMessage("cancel outcome")), {
        message: "cancel outcome never logged",
      })
      expect(lineFor(lines, "cancel outcome").fields).toEqual({
        sessionID: "ses_lazy_d2",
        mode: "cooperative",
        accepted: true,
      })
    } finally {
      await worker.terminate()
    }
  })
})

// ---------------------------------------------------------------------------
// Liveness watchdog — the transport must let a pong waiter and a run waiter
// coexist, and the production wiring must resolve the env override into the
// worker that spawns.
// ---------------------------------------------------------------------------

describe("liveness watchdog wiring", () => {
  test("a pong waiter and a run waiter coexist on one transport without stealing frames", async () => {
    await withFakeWorker(async (fake, transport) => {
      const run = transport.waitFor((message) => message.type === "done" && message.id === 1, { timeoutMs: 0 })
      const pong = transport.waitFor((message) => message.type === "pong" && message.id === 2, { timeoutMs: 0 })

      // The pong arrives first; it must be consumed by the pong waiter only, so
      // the later done still reaches the run waiter.
      fake.emit({ type: "pong", id: 2 })
      fake.emit({ type: "done", id: 1, result: { sessionID: "ses_1" } })

      expect(await pong).toEqual({ type: "pong", id: 2 })
      expect((await run).type).toBe("done")
    })
  })

  test("resolveSessionWorkerIdleTimeoutMs reads a positive integer and defaults anything else to 0", () => {
    expect(resolveSessionWorkerIdleTimeoutMs({ env: {} })).toBe(0)
    expect(resolveSessionWorkerIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "250" } })).toBe(250)
    // Absent, empty, zero, negative and non-numeric all mean "disabled".
    expect(resolveSessionWorkerIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "0" } })).toBe(0)
    expect(resolveSessionWorkerIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "-5" } })).toBe(0)
    expect(resolveSessionWorkerIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "abc" } })).toBe(0)
    expect(resolveSessionWorkerIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "  " } })).toBe(0)
  })

  test("the default-on watchdog resolves malformed env to 60s, only explicit 0 disables, and it warns", () => {
    expect(resolveRunIdleTimeoutMs({ env: {} })).toBe(DEFAULT_RUN_IDLE_TIMEOUT_MS)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "250" } })).toBe(250)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "0" } })).toBe(0)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "-5" } })).toBe(
      DEFAULT_RUN_IDLE_TIMEOUT_MS,
    )
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "abc" } })).toBe(
      DEFAULT_RUN_IDLE_TIMEOUT_MS,
    )
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "1.5" } })).toBe(
      DEFAULT_RUN_IDLE_TIMEOUT_MS,
    )
    // A malformed override warns with the raw value and the safe fallback;
    // absent/empty/valid/0 stay quiet.
    expect(runIdleTimeoutWarning({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "abc" } })).toContain(
      `invalid ${SESSION_WORKER_IDLE_TIMEOUT_ENV}: abc`,
    )
    expect(runIdleTimeoutWarning({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "0" } })).toBeUndefined()
    expect(runIdleTimeoutWarning({ env: {} })).toBeUndefined()
    expect(runIdleTimeoutWarning({ runIdleTimeoutMs: 0, env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "abc" } })).toBeUndefined()
  })

  test("createBunExecutorWorkerPool resolves the env override instead of passing undefined", async () => {
    await withGlobalFakeWorker(async () => {
      const pool = createBunExecutorWorkerPool({
        entry: "fake://idle-default",
        workKind: "instant",
        env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "25" },
      })
      try {
        const worker = pool.spawn()
        // The fake never replies. Before the fix this factory forwarded
        // `runIdleTimeoutMs: undefined`, disabling the watchdog (0) and hanging.
        await expect(worker.execute({ sessionID: sid("ses_idle_default"), depth: 0 })).rejects.toBeInstanceOf(
          SessionWorkerReplyTimeoutError,
        )
      } finally {
        await pool.terminateAll()
      }
    })
  })

  test("createProductionWorkerPool threads runIdleTimeoutMs into the spawned worker", async () => {
    await withGlobalFakeWorker(async () => {
      const pool = createProductionWorkerPool({
        init: () => sessionWorkerInit(),
        entry: "fake://idle-timeout",
        workKind: "instant",
        runIdleTimeoutMs: 25,
      })
      try {
        const worker = pool.spawn()
        // The fake isolate never replies, so only a watchdog that actually reached
        // the worker can reject this instead of hanging.
        await expect(worker.execute({ sessionID: sid("ses_idle"), depth: 0 })).rejects.toBeInstanceOf(
          SessionWorkerReplyTimeoutError,
        )
      } finally {
        await pool.terminateAll()
      }
    })
  })

  test("createProductionSessionHub resolves the env override and reaches the worker", async () => {
    await withGlobalFakeWorker(async () => {
      const hub = createProductionSessionHub({
        init: () => sessionWorkerInit(),
        count: 1,
        entry: "fake://idle-env",
        workKind: "instant",
        env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "25" },
      })
      try {
        // Resolved from the env override: the dispatch must settle (the watchdog
        // fired) rather than hang forever. The exact rejection is the dispatcher's
        // concern (it may terminate the worker), so only settlement is asserted.
        const outcome = await Promise.race([
          hub.ensureRunning(sid("ses_idle_env")).then(
            () => "resolved" as const,
            (error: unknown) => error,
          ),
          new Promise<"hung">((resolve) => setTimeout(() => resolve("hung"), 3_000)),
        ])
        expect(outcome).not.toBe("hung")
      } finally {
        await hub.terminateAll()
      }
    })
  })
})

describe("lifecycle reports are bound to the reporting worker's session", () => {
  test("a bound worker may report its own session; a foreign session is rejected", async () => {
    const hub = createSessionHub({
      pool: createFakeExecutorPool(1),
      ports: { hubProxy: { "sessionStatus.get": () => "pong" } },
    })
    const channel = new MessageChannel()
    try {
      // Bind worker 1 to one session without running it; the lifecycle guard reads
      // the dispatcher's live key→worker map, exactly as a real attach does.
      hub.dispatcher.registry.getOrCreate(sid("ses_bound"), () => ({ id: 1, alive: true, terminate: async () => {} }) as never)
      const dispose = hub.attachHubProxyPort(1, channel.port1)
      const client = createHubProxyClient(channel.port2)

      await client.call("lifecycle.commit", ["ses_bound", "msg_own"])
      await client.call("lifecycle.toolStarted", ["ses_bound", "call_own", true])
      expect(hub.lifecycle.crashState("ses_bound").lastCommittedMessageID).toBe("msg_own")
      expect(hub.lifecycle.indeterminate("ses_bound")).toEqual([{ callID: "call_own", sideEffecting: true }])

      const foreign = await client.call("lifecycle.commit", ["ses_foreign", "msg_foreign"]).then(
        () => ({ ok: true as const }),
        (error: unknown) => ({ ok: false as const, message: error instanceof Error ? error.message : String(error) }),
      )
      expect(foreign.ok).toBe(false)
      if (!foreign.ok) expect(foreign.message).toMatch(/bound to ses_bound/)
      expect(hub.lifecycle.crashState("ses_foreign").lastCommittedMessageID).toBeNull()

      client.dispose()
      dispose()
    } finally {
      channel.port1.close()
      channel.port2.close()
      await hub.terminateAll()
    }
  })
})

describe("session.clearRevert is a hub authority bound to the worker's session", () => {
  test("the bound session clears; a foreign session is rejected before the authority runs", async () => {
    const cleared: string[] = []
    const authorities = hubProxyAuthoritiesFor({
      run: (effect) => Effect.runPromise(effect),
      boundSessionID: sid("ses_revert_own"),
      subagentDepth: 1,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: {
        get: () => Effect.succeed({ type: "idle" as const }),
        list: () => Effect.succeed(new Map()),
        set: () => Effect.void,
      },
      permission: {
        ask: () => Effect.succeed(undefined as never),
        reply: () => Effect.succeed(undefined as never),
        list: () => Effect.succeed([]),
      },
      question: {
        ask: () => Effect.succeed(undefined as never),
        reply: () => Effect.succeed(undefined as never),
        reject: () => Effect.succeed(undefined as never),
        list: () => Effect.succeed([]),
      },
      clearRevert: async (sessionID) => {
        cleared.push(sessionID)
      },
    })

    await authorities.clearRevert!("ses_revert_own")
    expect(cleared).toEqual(["ses_revert_own"])
    await expect(authorities.clearRevert!("ses_revert_foreign")).rejects.toThrow(/may not clear a session revert/)
    expect(cleared).toEqual(["ses_revert_own"])
  })
})

describe("the worker job mirror authority is session-bound", () => {
  test("an unbound authority map exposes no backgroundJob", () => {
    const backgroundJob = {
      register: async () => {},
      settle: async () => {},
      awaitPromotion: async () => false,
    }
    const deps = {
      run: <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect),
      subagentDepth: 1,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: {
        get: () => Effect.succeed({ type: "idle" as const }),
        list: () => Effect.succeed(new Map()),
        set: () => Effect.void,
      },
      permission: {
        ask: () => Effect.succeed(undefined as never),
        reply: () => Effect.succeed(undefined as never),
        list: () => Effect.succeed([]),
      },
      question: {
        ask: () => Effect.succeed(undefined as never),
        reply: () => Effect.succeed(undefined as never),
        reject: () => Effect.succeed(undefined as never),
        list: () => Effect.succeed([]),
      },
      backgroundJob,
    }

    const unbound = hubProxyAuthoritiesFor({ ...deps, boundSessionID: undefined })
    expect(unbound.backgroundJob).toBeUndefined()

    const bound = hubProxyAuthoritiesFor({ ...deps, boundSessionID: sid("ses_job_bound") })
    expect(bound.backgroundJob).toBe(backgroundJob)
  })
})

// ---------------------------------------------------------------------------
// Idle isolate cap: reuse across turns is useful, but a long-lived process must
// not retain one idle isolate per session ever dispatched. The cap is an LRU of
// idle isolates (~2x available cores by default); active/generating workers are
// never candidates, and the existing per-session idle reap still applies.
// ---------------------------------------------------------------------------

describe("idle isolate cap", () => {
  function scriptedIdleWorker(
    id: number,
    terminated: number[],
    execute?: (run: ExecutorRun) => Promise<void>,
  ): ExecutorWorker {
    let alive = true
    return {
      id,
      get alive() {
        return alive
      },
      execute: async (run) => {
        await execute?.(run)
      },
      cancel: async () => {},
      assertNotBusy: async () => false,
      resultFor: () => undefined,
      terminate: async () => {
        if (!alive) return
        alive = false
        terminated.push(id)
      },
    }
  }

  test("retains only the LRU cap of idle isolates and evicts the oldest", async () => {
    const terminated: number[] = []
    const pool = createLazyExecutorWorkerPool({
      create: (id) => scriptedIdleWorker(id, terminated),
      maxRunning: 16,
      maxIdleWorkers: 2,
    })
    try {
      const workers = Array.from({ length: 5 }, () => pool.spawn())
      for (const [index, worker] of workers.entries()) {
        await worker.execute({ sessionID: sid(`ses_idle_cap_${index}`), depth: 0 })
      }

      expect(pool.idleCapacity).toBe(2)
      expect(pool.idleWorkers).toBe(2)
      expect(terminated).toEqual([1, 2, 3])
    } finally {
      await pool.terminateAll()
    }
  })

  test("never evicts an active or generating worker to make room", async () => {
    const terminated: number[] = []
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const pool = createLazyExecutorWorkerPool({
      create: (id) =>
        scriptedIdleWorker(id, terminated, id === 1 ? async () => await gate : undefined),
      maxRunning: 16,
      maxIdleWorkers: 1,
    })
    try {
      const busy = pool.spawn()
      const second = pool.spawn()
      const third = pool.spawn()
      const running = busy.execute({ sessionID: sid("ses_idle_active"), depth: 0 })
      await second.execute({ sessionID: sid("ses_idle_second"), depth: 0 })
      await third.execute({ sessionID: sid("ses_idle_third"), depth: 0 })

      // The oldest IDLE worker is evicted; the still-running one is untouched.
      expect(terminated).toEqual([2])
      expect(pool.idleWorkers).toBe(1)
      expect(terminated).not.toContain(1)

      release()
      await running
      // Once it becomes idle the LRU applies again, still keeping one isolate.
      expect(terminated).toEqual([2, 3])
      expect(pool.idleWorkers).toBe(1)
      expect(terminated).not.toContain(1)
    } finally {
      release()
      await pool.terminateAll()
    }
  })

  test("keeps an idle worker with active background work above the cap", async () => {
    const terminated: number[] = []
    const evicted: number[] = []
    const background = scriptedIdleWorker(1, terminated)
    background.hasActiveWork = async () => true
    const foreground = scriptedIdleWorker(2, terminated)
    const spawnFor = async (id: number) => (id === 1 ? background : foreground)
    const cap = createIdleIsolateCap({
      capacity: 1,
      onEvict: (entry) => evicted.push(entry.worker.id),
    })
    const first = cap.track(createSessionBoundExecutorWorker(1, spawnFor))
    const second = cap.track(createSessionBoundExecutorWorker(2, spawnFor))

    await first.execute({ sessionID: sid("ses_idle_background_first"), depth: 0 })
    await second.execute({ sessionID: sid("ses_idle_background_second"), depth: 0 })

    // The oldest idle worker still owns a background job. The cap must probe it
    // and keep it (temporarily above capacity) instead of killing that work.
    await pollUntil(() => cap.idle === 2, {
      message: "the worker with active background work was evicted",
      timeoutMs: 250,
    })
    expect(terminated).toEqual([])
    expect(evicted).toEqual([])
  })
})

describe("startup dead-lease sweep", () => {
  const DEAD_PID = 2_000_000_000
  const owner = (id: number, pid: number) => `worker-${id}-${pid}-${randomUUID()}`

  function scratch(): { filename: string; directory: string } {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-hub-lease-sweep-"))
    return { filename: path.join(directory, "opencode.db"), directory }
  }

  test("marks only provably dead generations reclaimable", () => {
    const { filename, directory } = scratch()
    const connection = openConnection({ filename })
    try {
      connection.exec(`
        CREATE TABLE event_sequence (
          aggregate_id text PRIMARY KEY,
          seq integer NOT NULL,
          owner_id text,
          worker_owner_id text,
          worker_heartbeat_at integer,
          worker_expires_at integer
        )
      `)
      const deadOwner = owner(1, DEAD_PID)
      const liveOwner = owner(2, process.pid)
      const expiry = Date.now() + 30_000
      connection.run(
        "INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id, worker_expires_at) VALUES (?, ?, ?, ?)",
        ["s-dead", 1, deadOwner, expiry],
      )
      connection.run(
        "INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id, worker_expires_at) VALUES (?, ?, ?, ?)",
        ["s-live", 2, liveOwner, expiry],
      )

      const logs: Array<{ message: string; fields: Record<string, unknown> }> = []
      const released = sweepDeadLeasesAtStartup(filename, (message, fields) => {
        logs.push({ message, fields })
      })

      expect(released).toBe(1)
      const rows = connection.all<{ aggregate_id: string; worker_expires_at: number }>(
        "SELECT aggregate_id, worker_expires_at FROM event_sequence ORDER BY aggregate_id",
      )
      expect(rows).toEqual([
        { aggregate_id: "s-dead", worker_expires_at: 0 },
        { aggregate_id: "s-live", worker_expires_at: expiry },
      ])
      expect(logs).toHaveLength(1)
      expect(logs[0]?.message).toContain("startup lease sweep")
      expect(logs[0]?.fields.released).toBe(1)
    } finally {
      connection.close()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test("a missing database is logged and never thrown", () => {
    const { filename, directory } = scratch()
    try {
      const logs: string[] = []
      expect(sweepDeadLeasesAtStartup(filename, (message) => logs.push(message))).toBe(0)
      expect(logs).toEqual([expect.stringContaining("startup lease sweep failed")])
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})
