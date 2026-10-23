// Phase 6.1 / 6.2 — worker fixture that boots the reduced session layer inside
// a real Bun worker isolate and runs the pipeline against the hub's mock
// provider.
//
// Protocol (default channel):
//   hub -> worker : { type:"init", snapshot, env, model }   (model = mock bootstrap)
//   worker -> hub : { type:"ready" }
//   hub -> worker : { type:"run", id, sessionID, messages }
//   worker -> hub : { type:"result", id, value }
//   hub -> worker : { type:"snapshot", id }
//   worker -> hub : { type:"snapshot-result", id, value }
//   worker -> hub : { type:"error", id, message }

import { Effect, Layer } from "effect"
import {
  ModelClient,
  SessionPipeline,
  WorkerEnvService,
  WorkerSnapshotService,
  buildSessionWorkerLayer,
  type ModelClientService,
  type PipelineMessage,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import { readSseStream } from "../lib/sse"

interface MockModelBootstrap {
  readonly baseUrl: string
  readonly apiKey: string
  readonly model: string
}

interface InitMessage {
  readonly type: "init"
  readonly snapshot: WorkerSnapshot
  readonly env: { readonly values: Record<string, string> }
  readonly model: MockModelBootstrap
}

interface RunMessage {
  readonly type: "run"
  readonly id: number
  readonly sessionID: string
  readonly messages: readonly PipelineMessage[]
}

interface SnapshotMessage {
  readonly type: "snapshot"
  readonly id: number
}

type Inbound = InitMessage | RunMessage | SnapshotMessage

interface WorkerScope {
  postMessage(message: unknown): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as WorkerScope

const published: Array<{ seq: number; committed: boolean }> = []
const emitted: string[] = []

/** Parse one SSE body into the assistant text (the mock provider only emits JSON frames). */
async function consumeChat(bootstrap: MockModelBootstrap, messages: readonly PipelineMessage[]): Promise<string> {
  const response = await fetch(`${bootstrap.baseUrl}/chat/completions`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${bootstrap.apiKey}` },
    body: JSON.stringify({ model: bootstrap.model, stream: true, messages }),
  })
  if (!response.ok) throw new Error(`mock provider responded ${response.status}`)
  if (!response.body) throw new Error("mock provider returned an empty body")

  const accumulator = await readSseStream(response.body)
  return accumulator.text
}

function modelLayer(bootstrap: MockModelBootstrap): Layer.Layer<ModelClient> {
  const service: ModelClientService = {
    complete: (input) =>
      Effect.tryPromise({
        try: () => consumeChat(bootstrap, input.messages).then((text) => ({ text })),
        catch: (error) => (error instanceof Error ? error : new Error(String(error))),
      }),
  }
  return Layer.succeed(ModelClient, service)
}

let runtime: ReturnType<typeof buildSessionWorkerLayer> | undefined

/**
 * Tear down the layer built by a previous `init`.
 *
 * `buildSessionWorkerLayer` returns a compiled layer descriptor; every run
 * builds and releases its own services through `Effect.provide(runtime.layer)`,
 * so there is no long-lived scope to interrupt. Closing therefore drops the
 * handle so a restarted worker can never reuse the stale layer.
 */
async function close(layer: ReturnType<typeof buildSessionWorkerLayer>): Promise<void> {
  if (layer === runtime) runtime = undefined
}

async function handleInit(message: InitMessage): Promise<void> {
  if (runtime !== undefined) await close(runtime)
  // A restart must start from a clean slate: the previous init's publishes and
  // emitted events must not leak into the new worker.
  published.length = 0
  emitted.length = 0
  runtime = buildSessionWorkerLayer({
    snapshot: message.snapshot,
    env: message.env,
    clients: {
      writer: {
        query: async () => [],
        publish: async () => {
          const result = { seq: published.length + 1, committed: true }
          published.push(result)
          return result
        },
      },
      egress: { emit: (event) => void emitted.push(event.type) },
      broker: { call: async () => null },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    },
    modelClient: modelLayer(message.model),
  })
  scope.postMessage({ type: "ready" })
}

async function handleRun(message: RunMessage): Promise<void> {
  if (runtime === undefined) {
    scope.postMessage({ type: "error", id: message.id, message: "worker received run before init" })
    return
  }
  try {
    const value = await Effect.runPromise(
      Effect.gen(function* () {
        const pipeline = yield* SessionPipeline
        return yield* pipeline.run({ sessionID: message.sessionID, messages: message.messages })
      }).pipe(Effect.provide(runtime.layer)),
    )
    scope.postMessage({ type: "result", id: message.id, value })
  } catch (error) {
    scope.postMessage({ type: "error", id: message.id, message: error instanceof Error ? error.message : String(error) })
  }
}

async function handleSnapshot(message: SnapshotMessage): Promise<void> {
  if (runtime === undefined) {
    scope.postMessage({ type: "error", id: message.id, message: "worker received snapshot before init" })
    return
  }
  try {
    const value = await Effect.runPromise(
      Effect.gen(function* () {
        const snapshot = yield* WorkerSnapshotService
        const env = yield* WorkerEnvService
        return { epoch: snapshot.epoch, directory: snapshot.directory, env: env.values }
      }).pipe(Effect.provide(runtime.layer)),
    )
    scope.postMessage({ type: "snapshot-result", id: message.id, value })
  } catch (error) {
    scope.postMessage({ type: "error", id: message.id, message: error instanceof Error ? error.message : String(error) })
  }
}

scope.onmessage = (event) => {
  const message = event.data as Inbound | undefined
  if (message === undefined) return
  if (message.type === "init") {
    void handleInit(message)
    return
  }
  if (message.type === "run") {
    void handleRun(message)
    return
  }
  if (message.type === "snapshot") {
    void handleSnapshot(message)
  }
}
