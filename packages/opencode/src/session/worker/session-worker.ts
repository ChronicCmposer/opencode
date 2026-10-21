// Phase 12 — production session-worker entry.
//
// This is the isolate the hub's `createBunExecutorWorkerPool` spawns. It builds
// the reduced session layer (`buildSessionWorkerLayer`, Phase 6) and installs the
// Phase 5 executor with a work factory whose session work runs the reduced
// `SessionPipeline` — i.e. `buildSessionWorkerLayer` IS the work factory.
//
// Initialisation arrives through `workerData` (structured-cloned, never a
// closure): the immutable `WorkerSnapshot`, the `WorkerEnv` overlay, and the
// model bootstrap (the hub's mock provider URL in tests). Client handles
// (writer/egress/broker/background) are worker-local for now; the real
// port-backed clients land with the Phase 7/8/10 wiring.
//
// Protocol: the `execute.ts` frame protocol over the default worker channel.

import { threadId, workerData } from "node:worker_threads"
import { Effect, Layer } from "effect"
import { createJobClient, jobPortFromMessagePort } from "./job-client"
import {
  installSessionExecutor,
  type SessionExecutionResult,
  type SessionWork,
  type SessionWorkFactory,
} from "./execute"
import {
  ModelClient,
  SessionPipeline,
  buildSessionWorkerLayer,
  type ModelClientService,
  type WorkerEnv,
  type WorkerSnapshot,
} from "./layer"

export interface SessionWorkerModel {
  readonly baseUrl: string
  readonly apiKey: string
  readonly model: string
}

export interface SessionWorkerInit {
  readonly snapshot: WorkerSnapshot
  readonly env: WorkerEnv
  readonly model: SessionWorkerModel
}

interface WorkerScope {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
  onmessage: ((event: { data: unknown }) => void) | null
}

/**
 * Parse the structured-cloned `workerData` at the isolate boundary. A missing or
 * malformed payload must fail loud with a descriptive error, not surface later
 * as an opaque `undefined` property access.
 */
export function parseSessionWorkerInit(data: unknown): SessionWorkerInit {
  if (typeof data !== "object" || data === null) {
    throw new Error(`session worker requires a SessionWorkerInit workerData, received ${typeof data}`)
  }
  const candidate = data as Partial<SessionWorkerInit>
  if (typeof candidate.snapshot !== "object" || candidate.snapshot === null) {
    throw new Error("session worker workerData.snapshot is required")
  }
  if (typeof candidate.env !== "object" || candidate.env === null) {
    throw new Error("session worker workerData.env is required")
  }
  if (candidate.model === undefined || typeof candidate.model.baseUrl !== "string") {
    throw new Error("session worker workerData.model.baseUrl is required")
  }
  return candidate as SessionWorkerInit
}

const scope = globalThis as unknown as WorkerScope
const init = parseSessionWorkerInit(workerData)

/** Observability sink the worker's mock writer/egress adapters record into. */
export interface WorkerRecordSink {
  readonly publish: (result: { readonly seq: number; readonly committed: boolean }) => void
  readonly emit: (type: string) => void
}

/**
 * Build the worker-local mock clients. This replaces the former module-level
 * `published`/`emitted` arrays: the adapters are now pure over an injected sink,
 * so the only mutable state is the writer's monotonic `seq` counter.
 */
function createMockClients(sink: WorkerRecordSink) {
  let publishedCount = 0
  return {
    writer: {
      query: async () => [],
      publish: async () => {
        publishedCount += 1
        const result = { seq: publishedCount, committed: true }
        sink.publish(result)
        return result
      },
    },
    egress: { emit: (event: { type: string }) => sink.emit(event.type) },
    broker: { call: async () => null },
    backgroundJob,
  }
}

// The worker's dedicated job control link: transfer one end to the hub (which
// serves the cross-worker registry on it) and drive the thin client with the
// other. This is how `task.ts`'s `background.*` calls reach the hub registry
// (plan tasks 9.2/9.3).
const jobChannel = new MessageChannel()
scope.postMessage({ type: "job-port", port: jobChannel.port2 }, [jobChannel.port2 as unknown as Transferable])
const backgroundJob = createJobClient(jobPortFromMessagePort(jobChannel.port1))

/** Consume a mock-provider SSE body into the concatenated assistant text. */
async function consumeChat(model: SessionWorkerModel): Promise<string> {
  const response = await fetch(`${model.baseUrl}/chat/completions`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${model.apiKey}` },
    body: JSON.stringify({ model: model.model, stream: true, messages: [] }),
  })
  if (!response.ok) throw new Error(`mock provider responded ${response.status}`)
  if (!response.body) throw new Error("mock provider returned an empty body")

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  let text = ""

  const apply = (frame: string): void => {
    for (const rawLine of frame.split("\n")) {
      const line = rawLine.trim()
      if (!line.startsWith("data:")) continue
      const data = line.slice("data:".length).trim()
      if (!data || data === "[DONE]") continue
      try {
        const payload = JSON.parse(data) as { choices?: Array<{ delta?: { content?: unknown } }> }
        const content = payload.choices?.[0]?.delta?.content
        if (typeof content === "string") text += content
      } catch {
        // The mock server only emits JSON frames; ignore anything malformed.
      }
    }
  }

  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let boundary = buffer.indexOf("\n\n")
    while (boundary !== -1) {
      apply(buffer.slice(0, boundary))
      buffer = buffer.slice(boundary + 2)
      boundary = buffer.indexOf("\n\n")
    }
  }
  buffer += decoder.decode()
  if (buffer.trim()) apply(buffer)
  return text
}

function modelLayer(model: SessionWorkerModel): Layer.Layer<ModelClient> {
  const service: ModelClientService = {
    complete: () =>
      Effect.tryPromise({
        try: () => consumeChat(model).then((text) => ({ text })),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      }),
  }
  return Layer.succeed(ModelClient, service)
}

const runtime = buildSessionWorkerLayer({
  snapshot: init.snapshot,
  env: init.env,
  clients: createMockClients({ publish: () => {}, emit: () => {} }),
  modelClient: modelLayer(init.model),
})

const work: SessionWorkFactory = () => {
  const run: SessionWork = (sessionID) =>
    Effect.gen(function* () {
      const pipeline = yield* SessionPipeline
      const result = yield* pipeline.run({ sessionID, messages: [] })
      return { sessionID, value: { ...result, threadId } } satisfies SessionExecutionResult
    }).pipe(Effect.provide(runtime.layer), Effect.orDie)
  return run
}

installSessionExecutor(scope, { work, threadId })
