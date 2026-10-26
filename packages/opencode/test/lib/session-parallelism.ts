// Hub-side session-parallelism runner (split from the former mock-sse-harness).
//
// `runParallelSessions` drives N sessions through the REAL
// `createDispatcher`/`createExecutorWorkerPool` (one active session per worker,
// FIFO beyond K), then records wall time, throughput, per-thread CPU, the hub
// event-loop gap and the metrics snapshot.
//
// No real model APIs: the provider is either the hub's `TestLLMServer` (tests) or
// the shared `startMockProvider` Bun.serve fallback (benchmark). Numbers are
// RECORDED, never asserted against a machine-dependent threshold.

import type { SessionID } from "@/session/schema"
import {
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionDispatch,
  type ExecutorTransport,
  type ExecutorWorker,
  type SessionExecutionResult,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
} from "@/session/worker/execute"
import { createDispatcher } from "@/session/worker/dispatch"
import { createLeakDetector, type LeakReport } from "./leak-detector"
import {
  LINUX_CLOCK_TICKS_PER_SECOND,
  createLoopGapProbe,
  readThreadCpuTicks,
  recordLoopGap,
  type LoopGapProbe,
  type LoopGapSample,
  type ThreadCpuTicks,
} from "./loop-gap"
import { createMetrics, type Metrics, type MetricsSnapshot } from "./metrics"
import {
  DEFAULT_BURN_ITERATIONS,
  MOCK_SSE_WORK_KIND,
  MOCK_SSE_WORKER_ENTRY,
  mockSseEnv,
} from "./mock-sse-worker"
import { createWorkerHarness, type HarnessWorker, type WorkerHarness } from "./worker-harness"

/** A worker thread counts as "busy" once it has accrued this much CPU over the run. */
export const BUSY_THREAD_CPU_MS = 5

const DEFAULT_TIMEOUT_MS = 20_000
const LOOP_GAP_INTERVAL_MS = 10

export interface ParallelSessionOptions {
  readonly sessions: number
  readonly poolSize: number
  /** Base URL of the mock provider (tests: `TestLLMServer.url`). */
  readonly baseUrl: string
  readonly burnIterations?: number
  readonly model?: string
  /** API key handed to the worker; defaults to the worker-side test key. */
  readonly apiKey?: string
  /** Injected core count for deterministic pool sizing. */
  readonly cores?: number
  readonly timeoutMs?: number
  readonly entry?: string | URL
}

export interface ThreadCpuSample {
  readonly threadId: number
  readonly cpuMs: number
  readonly busy: boolean
}

export interface ParallelSessionReport {
  readonly cores: number
  readonly sessions: number
  readonly poolSize: number
  readonly wallMs: number
  readonly throughputPerSec: number
  /** Worker threads that accrued CPU during the run. */
  readonly busyThreads: number
  readonly threadCpu: ReadonlyArray<ThreadCpuSample>
  readonly hubLoopGap: LoopGapSample
  readonly metrics: MetricsSnapshot
  readonly results: ReadonlyArray<SessionExecutionResult>
  readonly leak: LeakReport
}

type WorkerOutbound = SessionExecutorOutbound | { readonly type: "ports"; readonly egress: MessagePort }
type WorkerInbound = SessionExecutorInbound

/** A spawned worker plus the single promise that resolves its bootstrap handshake. */
interface SpawnedWorker {
  readonly worker: ExecutorWorker
  /** Resolves once the worker reported `ready` AND transferred its egress port. */
  readonly spawn: Promise<{ readonly threadId: number; readonly egress: MessagePort | undefined }>
}

/**
 * The executor outbound frame types, derived from `SessionExecutorOutbound` so
 * the runtime set cannot drift from the type union. `satisfies` rejects any
 * member that is not a valid outbound type at compile time.
 */
const EXECUTOR_FRAME_TYPES = [
  "ready",
  "pong",
  "busy",
  "cancelled",
  "done",
  "error",
] as const satisfies ReadonlyArray<SessionExecutorOutbound["type"]>

const EXECUTOR_FRAME_TYPE_SET: ReadonlySet<string> = new Set(EXECUTOR_FRAME_TYPES)

function isExecutorOutbound(message: WorkerOutbound): message is SessionExecutorOutbound {
  return EXECUTOR_FRAME_TYPE_SET.has(message.type)
}

/**
 * Sessions completed per second for a run. A non-positive wall time yields `0`
 * instead of `Infinity`/`NaN`, so an instantaneous run still records a finite
 * throughput rather than poisoning the metrics snapshot.
 */
export function throughputPerSec(sessions: number, wallMs: number): number {
  if (!(wallMs > 0)) return 0
  return sessions / (wallMs / 1000)
}

function requirePositiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`invalid ${label}: ${value} (expected a positive integer)`)
  }
  return value
}

function executorTransport(handle: HarnessWorker<WorkerOutbound, WorkerInbound>): ExecutorTransport {
  return {
    post: (message) => handle.post(message),
    waitFor: async (match, options) => {
      const message = await handle.waitFor((candidate) => isExecutorOutbound(candidate) && match(candidate), options)
      if (!isExecutorOutbound(message)) throw new Error("expected an executor frame from the session worker")
      return message
    },
    get alive() {
      return handle.alive
    },
    terminate: (code) => handle.terminate(code),
  }
}

function spawnSessionWorker(
  harness: WorkerHarness,
  id: number,
  entry: string | URL,
  env: Record<string, string>,
  timeoutMs: number,
): SpawnedWorker {
  const handle = harness.spawn<WorkerOutbound, WorkerInbound>(entry, { env })
  const spawn = Promise.all([
    handle.waitFor((message) => message.type === "ready", { timeoutMs }),
    handle.waitFor((message) => message.type === "ports", { timeoutMs }),
  ]).then(([ready, ports]) => ({
    threadId: ready.type === "ready" ? ready.threadId : 0,
    egress: ports.type === "ports" ? ports.egress : undefined,
  }))
  return {
    worker: createExecutorWorker({ id, transport: executorTransport(handle), workKind: MOCK_SSE_WORK_KIND, timeoutMs }),
    spawn,
  }
}

function cpuDelta(threadId: number, before: ThreadCpuTicks | undefined): ThreadCpuSample {
  const after = readThreadCpuTicks(threadId)
  if (before === undefined || after === undefined) return { threadId, cpuMs: 0, busy: false }
  const cpuMs = ((after.totalTicks - before.totalTicks) / LINUX_CLOCK_TICKS_PER_SECOND) * 1000
  return { threadId, cpuMs, busy: cpuMs >= BUSY_THREAD_CPU_MS }
}

function recordRun(metrics: Metrics, run: { sessions: number; poolSize: number; wallMs: number; busyThreads: number }): void {
  metrics.gauge("parallel.sessions", run.sessions)
  metrics.gauge("parallel.pool_size", run.poolSize)
  metrics.gauge("parallel.wall_ms", run.wallMs)
  metrics.gauge("parallel.throughput_per_sec", throughputPerSec(run.sessions, run.wallMs))
  metrics.gauge("parallel.busy_threads", run.busyThreads)
}

/**
 * Drive `sessions` concurrent sessions through a `poolSize`-worker pool, then tear
 * the pool down and report what was recorded. The report is a plain data snapshot:
 * same inputs + same host behaviour ⇒ same shape.
 */
export async function runParallelSessions(options: ParallelSessionOptions): Promise<ParallelSessionReport> {
  const sessions = requirePositiveInteger(options.sessions, "sessions")
  const poolSize = requirePositiveInteger(options.poolSize, "poolSize")
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const burnIterations = options.burnIterations ?? DEFAULT_BURN_ITERATIONS
  const entry = options.entry ?? MOCK_SSE_WORKER_ENTRY
  const env = mockSseEnv(options.baseUrl, burnIterations, options.model, options.apiKey)

  const metrics = createMetrics()
  const harness = createWorkerHarness({ cores: options.cores, timeoutMs })
  const leakDetector = createLeakDetector({ workers: () => harness.workers })
  const spawned: SpawnedWorker[] = []

  const pool = createExecutorWorkerPool({
    spawn: (id) => {
      const worker = spawnSessionWorker(harness, id, entry, env, timeoutMs)
      spawned.push(worker)
      return worker.worker
    },
  })
  // Eagerly materialize the requested number of workers for the measurement.
  for (let index = 0; index < poolSize; index += 1) pool.spawn()
  const dispatcher = createDispatcher({ pool })
  const dispatch = createSessionDispatch({ dispatcher, pool })

  let probe: LoopGapProbe | undefined
  let report: Omit<ParallelSessionReport, "leak"> | undefined
  try {
    const bootstraps = await Promise.all(spawned.map((worker) => worker.spawn))
    const threadIds = bootstraps.map((bootstrap) => bootstrap.threadId)
    const ports = bootstraps.map((bootstrap) => bootstrap.egress)
    for (const [index, port] of ports.entries()) {
      if (port !== undefined) leakDetector.trackPort(port, `worker-${index + 1}-egress`)
    }

    const before = threadIds.map((threadId) => readThreadCpuTicks(threadId))
    probe = createLoopGapProbe({ intervalMs: LOOP_GAP_INTERVAL_MS })
    const startedAt = performance.now()
    const sessionIDs = Array.from({ length: sessions }, (_, index) => `ses_par_${index}` as SessionID)
    const results = await Promise.all(sessionIDs.map((sessionID) => dispatch.ensureRunning(sessionID)))
    const wallMs = performance.now() - startedAt
    const hubLoopGap = probe.stop()

    const threadCpu = threadIds.map((threadId, index) => cpuDelta(threadId, before[index]))
    const busyThreads = threadCpu.filter((sample) => sample.busy).length
    recordRun(metrics, { sessions, poolSize, wallMs, busyThreads })
    recordLoopGap(metrics, hubLoopGap)

    report = {
      cores: harness.cores,
      sessions,
      poolSize,
      wallMs,
      throughputPerSec: throughputPerSec(sessions, wallMs),
      busyThreads,
      threadCpu,
      hubLoopGap,
      metrics: metrics.snapshot(),
      results,
    }
  } finally {
    probe?.stop()
    for (const port of leakDetector.ports) port.close()
    await dispatcher.terminateAll().catch(() => undefined)
    await harness.dispose().catch(() => undefined)
  }

  if (report === undefined) throw new Error("parallel session run produced no report")
  return { ...report, leak: leakDetector.detect() }
}
