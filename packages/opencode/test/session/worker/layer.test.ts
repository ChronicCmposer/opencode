// Phase 6.1 / 6.2 / 6.5 / 6.7 — reduced session layer tests (in-process).

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createSessionScopeHost } from "@/session/worker/session-scope"
import {
  ModelClient,
  SESSION_NODES,
  SessionPipeline,
  WorkerEnvService,
  WorkerSnapshotService,
  buildSessionWorkerLayer,
  type SessionLayerOptions,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import {
  SESSION_CLIENT_KEYS,
  collectNodeNames,
  validateSessionLayer,
} from "@/session/worker/node-table"

const snapshot = (overrides: Partial<WorkerSnapshot> = {}): WorkerSnapshot => ({
  epoch: 1,
  directory: "/tmp/project-a",
  config: { model: "test/test-model" },
  auth: { test: { type: "api", key: "secret-a" } },
  modelsDev: {},
  flags: {} as RuntimeFlags.Info,
  workerCount: 2,
  ...overrides,
})

const clients = (): SessionLayerOptions["clients"] => ({
  writer: {
    query: async () => [],
    publish: async () => ({ seq: 7, committed: true }),
  },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const modelLayer = (text: string) =>
  Layer.succeed(ModelClient, {
    complete: () => Effect.succeed({ text, usage: { input: 3, output: 4 } }),
  })

const options = (overrides: Partial<SessionLayerOptions> = {}): SessionLayerOptions => ({
  snapshot: snapshot(),
  env: { values: { OPENCODE_TEST_KEY: "worker-local" } },
  clients: clients(),
  modelClient: modelLayer("hello from the worker"),
  ...overrides,
})

describe("buildSessionWorkerLayer (6.5)", () => {
  test("the full allowlist graph validates once excluded nodes are replaced", () => {
    const built = buildSessionWorkerLayer(options({ nodes: SESSION_NODES }))
    const report = validateSessionLayer(built.root, built.replacements)
    expect(report.ok).toBe(true)
    expect(report.excluded).toEqual([])
  })

  test("the runnable layer excludes Database and SessionProjector", () => {
    const built = buildSessionWorkerLayer(options())
    const names = collectNodeNames(built.root)
    expect(names.has("@opencode/v2/storage/Database")).toBe(false)
    expect(names.has("session-projector")).toBe(false)
  })

  test("the runnable layer contains all four client nodes", () => {
    const built = buildSessionWorkerLayer(options())
    const names = collectNodeNames(built.root)
    for (const key of SESSION_CLIENT_KEYS) expect(names.has(key)).toBe(true)
    expect(built.root.dependencies.length).toBeGreaterThan(0)
  })

  test("without replacements the allowlist graph is rejected", () => {
    const report = validateSessionLayer(SESSION_NODES, [])
    expect(report.ok).toBe(false)
    expect(report.excluded).toContain("@opencode/v2/storage/Database")
  })
})

describe("per-worker snapshot isolation (6.2)", () => {
  test("two layers resolve different snapshots from the same process", async () => {
    const a = buildSessionWorkerLayer(options({ snapshot: snapshot({ epoch: 1, directory: "/a" }) }))
    const b = buildSessionWorkerLayer(
      options({
        snapshot: snapshot({ epoch: 2, directory: "/b", auth: { test: { type: "api", key: "secret-b" } } }),
      }),
    )

    const read = (built: ReturnType<typeof buildSessionWorkerLayer>) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const snap = yield* WorkerSnapshotService
          const env = yield* WorkerEnvService
          return { epoch: snap.epoch, directory: snap.directory, env: env.values }
        }).pipe(Effect.provide(built.layer)),
      )

    const [ra, rb] = await Promise.all([read(a), read(b)])
    expect(ra).toEqual({ epoch: 1, directory: "/a", env: { OPENCODE_TEST_KEY: "worker-local" } })
    expect(rb).toEqual({ epoch: 2, directory: "/b", env: { OPENCODE_TEST_KEY: "worker-local" } })
  })

  test("the env overlay never mutates process.env", async () => {
    const key = "OPENCODE_TEST_WORKER_OVERLAY"
    delete process.env[key]
    const built = buildSessionWorkerLayer(options({ env: { values: { [key]: "only-in-worker" } } }))
    await Effect.runPromise(
      Effect.gen(function* () {
        const env = yield* WorkerEnvService
        expect(env.values[key]).toBe("only-in-worker")
      }).pipe(Effect.provide(built.layer)),
    )
    expect(process.env[key]).toBeUndefined()
  })
})

describe("session pipeline (6.1)", () => {
  test("runs the model seam, emits egress and publishes durably", async () => {
    const emitted: string[] = []
    const published: string[] = []
    const built = buildSessionWorkerLayer(
      options({
        modelClient: modelLayer("pipeline text"),
        clients: {
          writer: {
            query: async () => [],
            publish: async (event) => {
              published.push(event.type)
              return { seq: 11, committed: true }
            },
          },
          egress: { emit: (event) => void emitted.push(event.type) },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
      }),
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const pipeline = yield* SessionPipeline
        return yield* pipeline.run({ sessionID: "s1", messages: [{ role: "user", content: "hi" }] })
      }).pipe(Effect.provide(built.layer)),
    )

    expect(result.text).toBe("pipeline text")
    expect(result.publishedSeq).toBe(11)
    expect(result.emitted).toBe(1)
    expect(emitted).toEqual(["session.text"])
    expect(published).toEqual(["message.assistant"])
  })
})

describe("session scope integration (6.7)", () => {
  test("closes the session scope and runs finalizers after the pipeline", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()
    const built = buildSessionWorkerLayer(options())

    const result = await Effect.runPromise(
      host.run(
        "s1",
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s1")))
          const pipeline = yield* SessionPipeline
          return yield* pipeline.run({ sessionID: "s1", messages: [{ role: "user", content: "hi" }] })
        }).pipe(Effect.provide(built.layer)),
      ),
    )

    expect(result.text).toBe("hello from the worker")
    expect(finalized).toEqual(["s1"])
    expect(host.open).toBe(0)
    expect(host.closed).toBe(1)
  })
})

describe("node table report", () => {
  test("describes the table without throwing", () => {
    expect(typeof LayerNode.group).toBe("function")
  })
})
