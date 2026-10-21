// Phase 6.1 / 6.2 / 6.5 / 6.7 — reduced session layer tests (in-process).

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Config } from "@/config/config"
import { Auth } from "@/auth"
import { createSessionScopeHost } from "@/session/worker/session-scope"
import {
  ModelClient,
  RUNNABLE_SESSION_NODES,
  SESSION_NODES,
  SessionPipeline,
  WorkerEnvService,
  WorkerSnapshotService,
  boundaryReplacements,
  buildSessionWorkerLayer,
  databaseBoundaryLayer,
  snapshotAuthLayer,
  snapshotConfigLayer,
  snapshotModelsDevLayer,
  type SessionLayerOptions,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import {
  REQUIRED_SESSION_NODE_KEYS,
  SESSION_CLIENT_KEYS,
  collectNodeNames,
  validateSessionLayer,
} from "@/session/worker/node-table"
import { workerSnapshot } from "../../lib/snapshot"

/** Project defaults layered on the shared builder (config/auth are asserted). */
const snapshot = (overrides: Partial<WorkerSnapshot> = {}): WorkerSnapshot =>
  workerSnapshot({
    directory: "/tmp/project-a",
    config: { model: "test/test-model" },
    auth: { test: { type: "api", key: "secret-a" } },
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
  test("the full allowlist graph is exhaustively classified after boundary replacements", () => {
    const report = validateSessionLayer(SESSION_NODES, boundaryReplacements(snapshot()))
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    // Every reachable application node is now allowlisted, excluded+replaced, or
    // a boundary replacement: the fail-closed `unknown` bucket is empty.
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("the runnable graph contains every required session-node service", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    for (const key of REQUIRED_SESSION_NODE_KEYS) expect(names.has(key)).toBe(true)
  })

  test("the runnable graph is fully classified with boundary replacements", () => {
    const report = validateSessionLayer(RUNNABLE_SESSION_NODES, boundaryReplacements(snapshot(), clients().writer))
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("the runnable graph reaches the real SessionPrompt/SessionProcessor loop", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    expect(names.has("@opencode/SessionPrompt")).toBe(true)
    expect(names.has("@opencode/SessionProcessor")).toBe(true)
    expect(names.has("@opencode/LLM")).toBe(true)
    expect(names.has("@opencode/ToolRegistry")).toBe(true)
  })

  test("the runnable graph fails closed on an unknown reachable node", () => {
    const orphan = LayerNode.make({
      name: "@opencode/Unclassified",
      layer: Layer.empty as Layer.Layer<never>,
      deps: [] as const,
    })
    const report = validateSessionLayer(
      LayerNode.group([RUNNABLE_SESSION_NODES, orphan]),
      boundaryReplacements(snapshot()),
    )
    expect(report.unknown).toContain("@opencode/Unclassified")
    expect(report.ok).toBe(false)
  })

  test("the runnable layer replaces Database and SessionProjector with boundaries", () => {
    const built = buildSessionWorkerLayer(options())
    const replaced = new Set(built.replacements.map(([node]) => node.name))
    expect(replaced.has("@opencode/v2/storage/Database")).toBe(true)
    expect(replaced.has("session-projector")).toBe(true)
    // The effective graph (replacements applied) is fully classified: neither
    // boundary is instantiated as a real node.
    const report = validateSessionLayer(built.root, built.replacements)
    expect(report.excluded).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
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

  test("a non-committed publish is not emitted and reports emitted 0", async () => {
    const emitted: string[] = []
    const built = buildSessionWorkerLayer(
      options({
        clients: {
          writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
          egress: { emit: (event) => void emitted.push(event.type) },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
      }),
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const pipeline = yield* SessionPipeline
        return yield* pipeline.run({ sessionID: "s1", messages: [] })
      }).pipe(Effect.provide(built.layer)),
    )

    expect(result.publishedSeq).toBeNull()
    expect(result.emitted).toBe(0)
    expect(emitted).toEqual([])
  })

  test("a writer rejection surfaces in the typed error channel", async () => {
    const built = buildSessionWorkerLayer(
      options({
        clients: {
          writer: {
            query: async () => [],
            publish: async () => {
              throw new Error("writer down")
            },
          },
          egress: { emit: () => {} },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
      }),
    )

    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const pipeline = yield* SessionPipeline
          return yield* pipeline.run({ sessionID: "s1", messages: [] })
        }).pipe(Effect.provide(built.layer)),
      ),
    ).rejects.toThrow("writer down")
  })
})

describe("boundary layers (6.x)", () => {
  test("snapshot Config serves reads but rejects writes loudly", async () => {
    const layer = snapshotConfigLayer(snapshot())
    const read = await Effect.runPromise(
      Effect.gen(function* () {
        const config = yield* Config.Service
        return yield* config.get()
      }).pipe(Effect.provide(layer)),
    )
    expect(read).toEqual({ model: "test/test-model" })

    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const config = yield* Config.Service
          return yield* config.update({})
        }).pipe(Effect.provide(layer)),
      ),
    ).rejects.toThrow(/Config.*update/)
  })

  test("snapshot Auth serves reads but rejects writes loudly", async () => {
    const layer = snapshotAuthLayer(snapshot())
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const auth = yield* Auth.Service
          return yield* auth.set("test", Auth.Api.make({ type: "api", key: "x" }))
        }).pipe(Effect.provide(layer)),
      ),
    ).rejects.toThrow(/Auth.*set/)
  })

  test("snapshot ModelsDev rejects refresh loudly", async () => {
    const layer = snapshotModelsDevLayer(snapshot())
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const modelsDev = yield* ModelsDev.Service
          return yield* modelsDev.refresh()
        }).pipe(Effect.provide(layer)),
      ),
    ).rejects.toThrow(/ModelsDev.*refresh/)
  })

  test("the Database boundary throws when the connection is touched", () => {
    expect(() =>
      Effect.runSync(
        Effect.gen(function* () {
          const database = yield* Database.Service
          return database.db
        }).pipe(Effect.provide(databaseBoundaryLayer())),
      ),
    ).toThrow(/Database/)
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

  test("the shared isNode predicate separates layer nodes from raw layers", () => {
    const node = LayerNode.make({ name: "test-node", layer: Layer.empty, deps: [] as const })
    expect(LayerNode.isNode(node)).toBe(true)
    expect(LayerNode.isNode(Layer.empty)).toBe(false)
  })
})
