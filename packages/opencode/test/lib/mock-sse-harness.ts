// Phase 13.1/13.3/13.5/13.6 — mock-SSE session-parallelism harness.
//
// Two halves:
//
//   * WORKER SIDE — `consumeMockSse` fetches the hub's mock provider and parses
//     the SSE stream in-isolate; `burnCpu` runs a synchronous CPU fold that
//     simulates the per-delta assembly cost. Because the fold is synchronous, a
//     busy session pins exactly one worker thread — the "one hot thread" baseline.
//
//   * HUB SIDE — `runParallelSessions` drives N sessions through the REAL
//     `createDispatcher`/`createExecutorWorkerPool` (one active session per
//     worker, FIFO beyond K), then records wall time, throughput, per-thread CPU,
//     the hub event-loop gap and the metrics snapshot. `measureSessionWorkerBoot`
//     records per-worker boot ms + process RSS at K and K+2.
//
// No real model APIs: the provider is the hub's `TestLLMServer` (tests) or a
// self-contained `Bun.serve` SSE mock (`startMockSseServer`, benchmark). Numbers
// are RECORDED, never asserted against a machine-dependent threshold.

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
  type LoopGapSample,
  type ThreadCpuTicks,
} from "./loop-gap"
import { createMetrics, type Metrics, type MetricsSnapshot } from "./metrics"
import { deriveWorkerCount, readRssBytes } from "./boot-memory"
import { createWorkerHarness, type HarnessWorker, type WorkerHarness } from "./worker-harness"

// --- Shared constants -------------------------------------------------------

/** Work kind the session-worker fixture registers for the SSE workload. */
export const MOCK_SSE_WORK_KIND = "sse"

/** Env overlay keys the hub uses to hand the worker its bootstrap (kind is a string only). */
export const MOCK_SSE_URL_ENV = "OPENCODE_MOCK_SSE_URL"
export const MOCK_SSE_BURN_ENV = "OPENCODE_MOCK_SSE_BURN"
export const MOCK_SSE_MODEL_ENV = "OPENCODE_MOCK_SSE_MODEL"
export const MOCK_SSE_API_KEY_ENV = "OPENCODE_MOCK_SSE_API_KEY"

/** CPU iterations per session; ~40 ms of synchronous work on the reference host. */
export const DEFAULT_BURN_ITERATIONS = 20_000_000

/** A worker thread counts as "busy" once it has accrued this much CPU over the run. */
export const BUSY_THREAD_CPU_MS = 5

const DEFAULT_TIMEOUT_MS = 20_000
/**
 * Boot waits are more tolerant than per-message waits: a cold worker pays the
 * full module-graph load, which can stretch past the per-message budget when the
 * whole test directory runs in parallel (Phase 13.6 flake).
 */
const DEFAULT_BOOT_TIMEOUT_MS = 30_000
/** A worker that misses its per-attempt budget is retried on a fresh worker, bounded here. */
const BOOT_ATTEMPTS = 2
const DEFAULT_EXTRA_WORKERS = 2
const LOOP_GAP_INTERVAL_MS = 10

/** The real session-worker fixture the harness spawns. */
export const MOCK_SSE_WORKER_ENTRY = new URL("../fixture/mock-sse-session-worker.ts", import.meta.url)

// --- Worker-side: SSE consumer + CPU burn -----------------------------------

export interface MockSseBootstrap {
  readonly baseUrl: string
  readonly model: string
  readonly apiKey: string
}

export interface MockSseConfig extends MockSseBootstrap {
  readonly burnIterations: number
}

export interface MockSseMessage {
  readonly role: string
  readonly content: string
}

export interface MockSseChatResult {
  readonly text: string
  readonly chunks: number
  readonly usage?: { readonly input: number; readonly output: number }
}

interface SseAccumulator {
  text: string
  chunks: number
  usage?: { input: number; output: number }
}

function parsePositiveInteger(raw: string | undefined): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined
  const parsed = Number(raw)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined
}

/**
 * Resolve the worker's mock-SSE config from the spawn environment. Fails loudly
 * when the hub forgot to hand over the base URL.
 */
export function readMockSseConfig(env: Record<string, string | undefined>): MockSseConfig {
  const baseUrl = env[MOCK_SSE_URL_ENV]
  if (baseUrl === undefined || baseUrl.trim() === "") {
    throw new Error(`${MOCK_SSE_URL_ENV} is not set (the hub must pass the mock SSE base URL)`)
  }
  return {
    baseUrl,
    model: env[MOCK_SSE_MODEL_ENV] ?? "mock-model",
    apiKey: env[MOCK_SSE_API_KEY_ENV] ?? "test-key",
    burnIterations: parsePositiveInteger(env[MOCK_SSE_BURN_ENV]) ?? DEFAULT_BURN_ITERATIONS,
  }
}

/**
 * A deterministic, synchronous integer fold. It is intentionally opaque to the
 * optimiser (the running state feeds the next multiply) so the CPU cost is real
 * and a busy session genuinely pins its worker thread.
 */
export function burnCpu(iterations: number, seed: number): number {
  let state = seed >>> 0
  for (let index = 0; index < iterations; index++) {
    state = (Math.imul(state ^ (state >>> 15), 2246822519) + 0x9e3779b9) | 0
    state ^= state << 13
  }
  return state >>> 0
}

/** Apply one decoded SSE frame (which may hold several `data:` lines). */
function applySseFrame(accumulator: SseAccumulator, frame: string): void {
  for (const rawLine of frame.split("\n")) {
    const line = rawLine.trim()
    if (!line.startsWith("data:")) continue
    const data = line.slice("data:".length).trim()
    if (data === "" || data === "[DONE]") continue
    let payload: unknown
    try {
      payload = JSON.parse(data)
    } catch {
      continue
    }
    if (payload === null || typeof payload !== "object") continue

    if ("usage" in payload && payload.usage !== null && typeof payload.usage === "object") {
      const usage = payload.usage as Record<string, unknown>
      if (typeof usage.prompt_tokens === "number" && typeof usage.completion_tokens === "number") {
        accumulator.usage = { input: usage.prompt_tokens, output: usage.completion_tokens }
      }
    }

    if (!("choices" in payload) || !Array.isArray(payload.choices)) continue
    const choice = payload.choices[0]
    if (choice === null || typeof choice !== "object" || !("delta" in choice)) continue
    const delta = choice.delta
    if (delta === null || typeof delta !== "object") continue
    if (!("content" in delta) || typeof delta.content !== "string") continue

    accumulator.text += delta.content
    accumulator.chunks += 1
  }
}

/** Consume the mock provider's streamed chat completion, entirely in-isolate. */
export async function consumeMockSse(
  bootstrap: MockSseBootstrap,
  messages: ReadonlyArray<MockSseMessage>,
): Promise<MockSseChatResult> {
  const response = await fetch(`${bootstrap.baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${bootstrap.apiKey}`,
    },
    body: JSON.stringify({ model: bootstrap.model, stream: true, messages }),
  })
  if (!response.ok) throw new Error(`mock SSE provider responded ${response.status}`)
  if (response.body === null) throw new Error("mock SSE provider returned an empty body")

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  const accumulator: SseAccumulator = { text: "", chunks: 0 }
  let buffer = ""

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let boundary = buffer.indexOf("\n\n")
    while (boundary !== -1) {
      applySseFrame(accumulator, buffer.slice(0, boundary))
      buffer = buffer.slice(boundary + 2)
      boundary = buffer.indexOf("\n\n")
    }
  }
  buffer += decoder.decode()
  if (buffer.trim() !== "") applySseFrame(accumulator, buffer)

  return accumulator.usage === undefined
    ? { text: accumulator.text, chunks: accumulator.chunks }
    : { text: accumulator.text, chunks: accumulator.chunks, usage: accumulator.usage }
}

// --- Hub-side: a self-contained mock SSE server (benchmark) -----------------

export interface MockSseServer {
  readonly url: string
  close(): void
}

/**
 * A dependency-free OpenAI-compatible SSE endpoint for the opt-in benchmark, so
 * the script needs no Effect runtime. Serves `POST /v1/chat/completions`.
 */
export function startMockSseServer(options: { readonly chunks?: number } = {}): MockSseServer {
  const chunks = options.chunks ?? 8
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const path = new URL(request.url).pathname
      if (request.method !== "POST" || !path.endsWith("/chat/completions")) {
        return new Response("not found", { status: 404 })
      }
      const encoder = new TextEncoder()
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          const frame = (payload: unknown) => controller.enqueue(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`))
          frame({ choices: [{ delta: { role: "assistant" } }] })
          for (let index = 0; index < chunks; index++) {
            frame({ choices: [{ delta: { content: `token${index} ` } }] })
          }
          frame({ usage: { prompt_tokens: 5, completion_tokens: chunks } })
          controller.enqueue(encoder.encode("data: [DONE]\n\n"))
          controller.close()
        },
      })
      return new Response(body, { headers: { "content-type": "text/event-stream" } })
    },
  })
  return { url: `http://127.0.0.1:${server.port}/v1`, close: () => void server.stop(true) }
}

// --- Hub-side: parallel session run -----------------------------------------

export interface ParallelSessionOptions {
  readonly sessions: number
  readonly poolSize: number
  /** Base URL of the mock provider (tests: `TestLLMServer.url`). */
  readonly baseUrl: string
  readonly burnIterations?: number
  readonly model?: string
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

interface SpawnedWorker {
  readonly worker: ExecutorWorker
  readonly ready: Promise<number>
  readonly ports: Promise<MessagePort | undefined>
}

const EXECUTOR_FRAME_TYPES: ReadonlySet<string> = new Set([
  "ready",
  "pong",
  "busy",
  "cancelled",
  "done",
  "error",
])

function isExecutorOutbound(message: WorkerOutbound): message is SessionExecutorOutbound {
  return EXECUTOR_FRAME_TYPES.has(message.type)
}

function requirePositiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`invalid ${label}: ${value} (expected a positive integer)`)
  }
  return value
}

function mockSseEnv(baseUrl: string, burnIterations: number, model?: string): Record<string, string> {
  return {
    [MOCK_SSE_URL_ENV]: baseUrl,
    [MOCK_SSE_BURN_ENV]: String(burnIterations),
    ...(model === undefined ? {} : { [MOCK_SSE_MODEL_ENV]: model }),
  }
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
  const ready = handle
    .waitFor((message) => message.type === "ready", { timeoutMs })
    .then((message) => (message.type === "ready" ? message.threadId : 0))
  const ports = handle
    .waitFor((message) => message.type === "ports", { timeoutMs })
    .then((message) => (message.type === "ports" ? message.egress : undefined))
  return {
    worker: createExecutorWorker({ id, transport: executorTransport(handle), workKind: MOCK_SSE_WORK_KIND, timeoutMs }),
    ready,
    ports,
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
  metrics.gauge("parallel.throughput_per_sec", run.sessions / (run.wallMs / 1000))
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
  const env = mockSseEnv(options.baseUrl, burnIterations, options.model)

  const metrics = createMetrics()
  const harness = createWorkerHarness({ cores: options.cores, timeoutMs })
  const leakDetector = createLeakDetector({ workers: () => harness.workers })
  const spawned: SpawnedWorker[] = []

  const pool = createExecutorWorkerPool({
    k: poolSize,
    spawn: (id) => {
      const worker = spawnSessionWorker(harness, id, entry, env, timeoutMs)
      spawned.push(worker)
      return worker.worker
    },
  })
  const dispatcher = createDispatcher({ pool })
  const dispatch = createSessionDispatch({ dispatcher, pool })

  let report: Omit<ParallelSessionReport, "leak"> | undefined
  try {
    const threadIds = await Promise.all(spawned.map((worker) => worker.ready))
    const ports = await Promise.all(spawned.map((worker) => worker.ports))
    for (const [index, port] of ports.entries()) {
      if (port !== undefined) leakDetector.trackPort(port, `worker-${index + 1}-egress`)
    }

    const before = threadIds.map((threadId) => readThreadCpuTicks(threadId))
    const probe = createLoopGapProbe({ intervalMs: LOOP_GAP_INTERVAL_MS })
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
      throughputPerSec: sessions / (wallMs / 1000),
      busyThreads,
      threadCpu,
      hubLoopGap,
      metrics: metrics.snapshot(),
      results,
    }
  } finally {
    for (const port of leakDetector.ports) port.close()
    await dispatcher.terminateAll().catch(() => undefined)
    await harness.dispose().catch(() => undefined)
  }

  if (report === undefined) throw new Error("parallel session run produced no report")
  return { ...report, leak: leakDetector.detect() }
}

// --- Hub-side: boot measurement for the real session-worker entry -----------

export interface SessionBootSample {
  readonly workerId: number
  readonly bootMs: number
}

export interface SessionBootLevel {
  readonly workers: number
  readonly samples: ReadonlyArray<SessionBootSample>
  readonly bootMs: ReadonlyArray<number>
  readonly medianBootMs: number
  readonly maxBootMs: number
  readonly rssBytes: number
}

export interface SessionBootReport {
  readonly cores: number
  readonly k: number
  readonly extra: number
  readonly baselineRssBytes: number
  readonly atK: SessionBootLevel
  readonly atKPlusExtra: SessionBootLevel
}

export interface MeasureSessionBootOptions {
  readonly cores?: number
  readonly k?: number
  readonly extra?: number
  readonly entry?: string | URL
  readonly timeoutMs?: number
}

function median(values: ReadonlyArray<number>): number {
  if (values.length === 0) return 0
  const sorted = values.slice().sort((left, right) => left - right)
  const middle = Math.floor(sorted.length / 2)
  if (sorted.length % 2 === 1) return sorted[middle]!
  return (sorted[middle - 1]! + sorted[middle]!) / 2
}

function bootLevel(workers: number, samples: ReadonlyArray<SessionBootSample>, rssBytes: number): SessionBootLevel {
  const bootMs = samples.map((sample) => sample.bootMs)
  return {
    workers,
    samples,
    bootMs,
    medianBootMs: median(bootMs),
    maxBootMs: bootMs.length === 0 ? 0 : Math.max(...bootMs),
    rssBytes,
  }
}

/**
 * Boot the REAL session-worker entry K times, then `extra` more, recording each
 * worker's boot ms and the process RSS at each level. Bun workers are threads in
 * one address space, so RSS is process-wide and recorded, not attributed.
 */
export async function measureSessionWorkerBoot(options: MeasureSessionBootOptions = {}): Promise<SessionBootReport> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_BOOT_TIMEOUT_MS
  const harness = createWorkerHarness({ cores: options.cores, timeoutMs })
  const cores = harness.cores
  const k = options.k ?? deriveWorkerCount(cores)
  const extra = options.extra ?? DEFAULT_EXTRA_WORKERS
  const entry = options.entry ?? MOCK_SSE_WORKER_ENTRY

  if (!Number.isInteger(k) || k < 0) throw new Error(`invalid worker count k: ${k} (expected a non-negative integer)`)
  if (!Number.isInteger(extra) || extra < 0) {
    throw new Error(`invalid extra worker count: ${extra} (expected a non-negative integer)`)
  }
  const baselineRssBytes = readRssBytes()

  const bootOne = async (): Promise<SessionBootSample> => {
    let lastError: unknown
    for (let attempt = 1; attempt <= BOOT_ATTEMPTS; attempt++) {
      const startedAt = performance.now()
      const handle = harness.spawn(entry)
      try {
        await handle.waitFor("ready", { timeoutMs })
        return { workerId: handle.id, bootMs: performance.now() - startedAt }
      } catch (error) {
        lastError = error
        await handle.terminate().catch(() => undefined)
      }
    }
    const reason = lastError instanceof Error ? lastError.message : String(lastError)
    throw new Error(`session worker did not report ready after ${BOOT_ATTEMPTS} attempts: ${reason}`)
  }

  const bootMany = async (count: number): Promise<SessionBootSample[]> => {
    const samples: SessionBootSample[] = []
    for (let index = 0; index < count; index++) samples.push(await bootOne())
    return samples
  }

  try {
    const atKSamples = await bootMany(k)
    const atKRssBytes = readRssBytes()
    const extraSamples = await bootMany(extra)
    const atKPlusExtraRssBytes = readRssBytes()
    const allSamples = [...atKSamples, ...extraSamples]

    return {
      cores,
      k,
      extra,
      baselineRssBytes,
      atK: bootLevel(atKSamples.length, atKSamples, atKRssBytes),
      atKPlusExtra: bootLevel(allSamples.length, allSamples, atKPlusExtraRssBytes),
    }
  } finally {
    await harness.dispose()
  }
}
