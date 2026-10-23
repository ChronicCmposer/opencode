// Session worker pool sizing and the production pool wrapper.
//
// K = `floor(cores × F)`, floor 1, where F defaults to
// `DEFAULT_OVERSUBSCRIPTION_FACTOR` (2) and cores defaults to
// `os.availableParallelism()`. `OPENCODE_SESSION_WORKER_COUNT` overrides K
// absolutely; `OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION` retunes F. Cores are
// injected so sizing is deterministic.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import {
  WorkerModelNotSerializableError,
  buildSessionWorkerInit,
  buildWorkerEnv,
  resolveWorkerModel,
} from "@/session/worker/init"
import {
  DEFAULT_OVERSUBSCRIPTION_FACTOR,
  OVERSUBSCRIPTION_ENV,
  WORKER_COUNT_ENV,
  deriveWorkerCount,
  resolveWorkerCount,
} from "@/session/worker/pool"
import { createProductionWorkerPool } from "@/session/worker/hub"
import { buildWorkerSnapshot, nextEpoch } from "@/session/worker/snapshot"
import { assertStructuredCloneable, sessionWorkerInit } from "../../lib/session-worker-init"
import { bootSessionWorker } from "../../lib/session-worker-harness"
import { sid } from "../../lib/async"

/** The dependency-light executor fixture (speaks the executor protocol). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

describe("worker count sizing", () => {
  test("oversubscribes cores by the default factor", () => {
    expect(resolveWorkerCount({ availableParallelism: () => 4, env: {} })).toBe(8)
    expect(resolveWorkerCount({ availableParallelism: () => 8, env: {} })).toBe(16)
  })

  test("derives nproc × factor on small machines", () => {
    for (const [cores, expected] of [
      [1, 2],
      [2, 4],
      [3, 6],
    ] as const) {
      expect(resolveWorkerCount({ availableParallelism: () => cores, env: {} })).toBe(expected)
    }
  })

  test("deriveWorkerCount floors cores × factor with a floor of one", () => {
    expect(deriveWorkerCount(4)).toBe(4 * DEFAULT_OVERSUBSCRIPTION_FACTOR)
    expect(deriveWorkerCount(4, 3)).toBe(12)
    expect(deriveWorkerCount(1)).toBe(2)
  })

  test("the oversubscription factor is overridable", () => {
    expect(resolveWorkerCount({ availableParallelism: () => 4, env: { [OVERSUBSCRIPTION_ENV]: "3" } })).toBe(12)
    expect(resolveWorkerCount({ availableParallelism: () => 4, env: { [OVERSUBSCRIPTION_ENV]: "1.5" } })).toBe(6)
  })

  test("the absolute count override wins over the factor", () => {
    expect(resolveWorkerCount({ availableParallelism: () => 2, env: { [WORKER_COUNT_ENV]: "5" } })).toBe(5)
  })

  test("an invalid factor, override or core count fails loud", () => {
    for (const factor of ["0", "abc"]) {
      expect(() =>
        resolveWorkerCount({ availableParallelism: () => 4, env: { [OVERSUBSCRIPTION_ENV]: factor } }),
      ).toThrow(/OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION/)
    }
    expect(() => resolveWorkerCount({ availableParallelism: () => 8, env: { [WORKER_COUNT_ENV]: "0" } })).toThrow(
      /OPENCODE_SESSION_WORKER_COUNT/,
    )
    expect(() => resolveWorkerCount({ availableParallelism: () => 8, env: { [WORKER_COUNT_ENV]: "abc" } })).toThrow(
      /OPENCODE_SESSION_WORKER_COUNT/,
    )
    expect(() => resolveWorkerCount({ availableParallelism: () => 0, env: {} })).toThrow(/availableParallelism/)
    expect(() => deriveWorkerCount(0)).toThrow(/availableParallelism/)
    expect(() => deriveWorkerCount(2.5)).toThrow(/availableParallelism/)
  })

  test("falls back to the real available parallelism when not injected", () => {
    expect(resolveWorkerCount({ env: {} })).toBeGreaterThanOrEqual(1)
  })
})

// ---------------------------------------------------------------------------
// Production snapshot producer (steps 5 + 6).
// ---------------------------------------------------------------------------

const instanceCtx = {
  directory: "/tmp/opencode-proj",
  worktree: "/tmp/opencode-proj",
  project: {} as never,
}

/** The hub config roots the snapshot must carry so a worker scans the same roots. */
const hubDirectories = ["/cfg/opencode", "/proj/.opencode"]

const snapshotLayers = Layer.mergeAll(
  Layer.mock(Config.Service, {
    get: () => Effect.succeed({ model: "test/model" } as ConfigV1.Info),
    directories: () => Effect.succeed([...hubDirectories]),
  }),
  Layer.mock(Auth.Service, {
    all: () => Effect.succeed({ anthropic: new Auth.Api({ type: "api", key: "sk-secret" }) }),
  }),
  Layer.mock(ModelsDev.Service, {
    get: () =>
      Effect.succeed({
        anthropic: { id: "anthropic", name: "Anthropic", env: [], models: {} } as ModelsDev.Provider,
      }),
  }),
  RuntimeFlags.layer({ experimentalSessionWorkers: true }),
)

const buildSnapshot = (options: { workerCount?: number; clock?: () => number }) =>
  buildWorkerSnapshot(options).pipe(Effect.provide(snapshotLayers), Effect.provideService(InstanceRef, instanceCtx))

describe("buildWorkerSnapshot", () => {
  test("produces the layer snapshot shape from hub services", async () => {
    const snapshot = await Effect.runPromise(buildSnapshot({ workerCount: 3, clock: () => 1000 }))

    expect(snapshot.directory).toBe("/tmp/opencode-proj")
    expect(snapshot.directories).toEqual(hubDirectories)
    expect(snapshot.workerCount).toBe(3)
    expect(snapshot.flags.experimentalSessionWorkers).toBe(true)
    expect(snapshot.config).toEqual({ model: "test/model" })
    expect(snapshot.auth.anthropic).toEqual({ type: "api", key: "sk-secret" })
    expect(snapshot.modelsDev.anthropic?.id).toBe("anthropic")
    expect(typeof snapshot.epoch).toBe("number")
  })

  test("is JSON-serializable (structured-clone friendly)", async () => {
    const snapshot = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 5 }))

    expect(JSON.parse(JSON.stringify(snapshot))).toEqual(snapshot)
  })
})

describe("snapshot epoch", () => {
  test("nextEpoch never regresses even when the clock does", () => {
    expect(nextEpoch(5, 5)).toBe(6)
    expect(nextEpoch(5, 100)).toBe(100)
    expect(nextEpoch(5, 1)).toBe(6)
  })

  test("successive builds are strictly monotonic with an injectable clock", async () => {
    const first = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 1_000 }))
    const second = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 10 }))
    const third = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 5 }))

    expect(second.epoch).toBeGreaterThan(first.epoch)
    expect(third.epoch).toBeGreaterThan(second.epoch)
  })
})

// ---------------------------------------------------------------------------
// Worker env overlay, model resolution and init assembly (steps 7 + 8 + 9).
// ---------------------------------------------------------------------------

describe("buildWorkerEnv", () => {
  test("merges an overlay over a base", () => {
    expect(buildWorkerEnv({ base: { A: "1", B: "2" }, overlay: { B: "3", C: "4" } })).toEqual({
      values: { A: "1", B: "3", C: "4" },
    })
  })

  test("an undefined overlay value deletes the inherited key", () => {
    expect(buildWorkerEnv({ base: { A: "1", B: "2" }, overlay: { B: undefined } })).toEqual({ values: { A: "1" } })
  })

  test("undefined base values are skipped", () => {
    expect(buildWorkerEnv({ base: { A: "1", B: undefined } })).toEqual({ values: { A: "1" } })
  })

  test("defaults the base to process.env without mutating it", () => {
    const before = { ...process.env }
    const env = buildWorkerEnv({ overlay: { OPENCODE_WORKER_TEST_MARKER: "1" } })

    expect(env.values.OPENCODE_WORKER_TEST_MARKER).toBe("1")
    expect({ ...process.env }).toEqual(before)
  })
})

const providerInfo = {
  id: "anthropic",
  name: "Anthropic",
  source: "api",
  env: [],
  options: { baseURL: "https://api.example.com/v1", headers: { "x-provider": "1" } },
  models: {},
} as unknown as Provider.Info

const providerModel = {
  id: "claude",
  providerID: "anthropic",
  api: { id: "claude-api-id", url: "https://fallback.example/v1", npm: "@ai-sdk/anthropic" },
  name: "Claude",
  capabilities: {},
  cost: {},
  limit: { context: 1_000, output: 1_000 },
  status: "active",
  options: {},
  headers: { "x-model": "1" },
  release_date: "2024-01-01",
} as unknown as Provider.Model

interface ModelLayerInput {
  readonly providerOptions?: Record<string, unknown>
  readonly modelHeaders?: Record<string, string>
  readonly credential?: Auth.Info
  readonly config?: Partial<ConfigV1.Info>
}

const modelLayers = (input: ModelLayerInput = {}) =>
  Layer.mergeAll(
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make("anthropic"), modelID: ModelV2.ID.make("claude") }),
      getProvider: () => Effect.succeed({ ...providerInfo, options: input.providerOptions ?? providerInfo.options }),
      getModel: () => Effect.succeed({ ...providerModel, headers: input.modelHeaders ?? providerModel.headers }),
    }),
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(input.credential ?? new Auth.Api({ type: "api", key: "sk-auth" })),
      all: () => Effect.succeed({ anthropic: { type: "api", key: "sk-auth" } as unknown as Auth.Info }),
    }),
    Layer.mock(Config.Service, {
      get: () => Effect.succeed((input.config ?? {}) as ConfigV1.Info),
      directories: () => Effect.succeed([...hubDirectories]),
    }),
    Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
    RuntimeFlags.layer({ experimentalSessionWorkers: true }),
  )

const runResolve = (
  input: ModelLayerInput,
  options: { providerID?: string; modelID?: string; env?: Record<string, string | undefined> } = {},
) => Effect.runPromise(resolveWorkerModel(options).pipe(Effect.provide(modelLayers(input))))

describe("resolveWorkerModel", () => {
  test("resolves the serializable subset from provider + auth", async () => {
    const model = await runResolve({})

    expect(model).toEqual({
      providerID: "anthropic",
      modelID: "claude",
      apiId: "claude-api-id",
      npm: "@ai-sdk/anthropic",
      baseUrl: "https://api.example.com/v1",
      apiKey: "sk-auth",
      headers: { "x-provider": "1", "x-model": "1" },
      capabilities: { temperature: false, reasoning: false, interleaved: undefined },
      limit: { context: 1_000, output: 1_000 },
      options: {},
      providerOptions: { baseURL: "https://api.example.com/v1", headers: { "x-provider": "1" } },
    })
  })

  test("falls back to the model api url when no provider baseURL is set", async () => {
    const model = await runResolve({ providerOptions: { headers: {} } })

    expect(model.baseUrl).toBe("https://fallback.example/v1")
  })

  test("substitutes ${VAR} from the injected env", async () => {
    const model = await runResolve(
      { providerOptions: { baseURL: "https://${HOST}/v1" } },
      { env: { HOST: "example.test" } },
    )

    expect(model.baseUrl).toBe("https://example.test/v1")
  })

  test("a provider option apiKey wins over the auth credential", async () => {
    const model = await runResolve({ providerOptions: { apiKey: "sk-option" } })

    expect(model.apiKey).toBe("sk-option")
  })

  test("honours an explicit provider/model selection", async () => {
    const model = await runResolve({}, { providerID: "openai", modelID: "gpt" })

    expect(model.providerID).toBe("openai")
    expect(model.modelID).toBe("gpt")
  })

  test("rejects function-bearing provider options with a typed error", async () => {
    const result = await Effect.runPromise(
      Effect.result(resolveWorkerModel().pipe(Effect.provide(modelLayers({ providerOptions: { fetch: () => {} } })))),
    )

    expect(result._tag).toBe("Failure")
    if (result._tag === "Failure") {
      expect(result.failure).toBeInstanceOf(WorkerModelNotSerializableError)
      expect((result.failure as WorkerModelNotSerializableError).field).toBe("fetch")
    }
  })

  test("rejects a function nested inside options", async () => {
    const result = await Effect.runPromise(
      Effect.result(
        resolveWorkerModel().pipe(Effect.provide(modelLayers({ providerOptions: { headers: { auth: () => {} } } }))),
      ),
    )

    expect(result._tag).toBe("Failure")
    if (result._tag === "Failure") {
      expect(result.failure).toBeInstanceOf(WorkerModelNotSerializableError)
      expect((result.failure as WorkerModelNotSerializableError).field).toBe("headers.auth")
    }
  })
})

describe("buildSessionWorkerInit", () => {
  const runInit = (options: Parameters<typeof buildSessionWorkerInit>[0] = {}) =>
    Effect.runPromise(
      buildSessionWorkerInit(options).pipe(
        Effect.provide(modelLayers({})),
        Effect.provideService(InstanceRef, instanceCtx),
      ),
    )

  test("assembles a structured-cloneable init from snapshot + env + model", async () => {
    const init = await runInit({
      snapshot: { workerCount: 2, clock: () => 100 },
      env: { base: {}, overlay: { A: "1" } },
    })

    expect(init.env).toEqual({ values: { A: "1" } })
    expect(init.model).toEqual({
      providerID: "anthropic",
      modelID: "claude",
      apiId: "claude-api-id",
      npm: "@ai-sdk/anthropic",
      baseUrl: "https://api.example.com/v1",
      apiKey: "sk-auth",
      headers: { "x-provider": "1", "x-model": "1" },
      capabilities: { temperature: false, reasoning: false, interleaved: undefined },
      limit: { context: 1_000, output: 1_000 },
      options: {},
      providerOptions: { baseURL: "https://api.example.com/v1", headers: { "x-provider": "1" } },
    })
    expect(init.snapshot.directory).toBe("/tmp/opencode-proj")
    expect(init.snapshot.workerCount).toBe(2)
    expect(assertStructuredCloneable(init)).toEqual(init)
  })

  test("carries the requested worker capabilities", async () => {
    const init = await runInit({
      snapshot: { workerCount: 1, clock: () => 300 },
      capabilities: { sessionWorkerPorts: true },
    })

    expect(init.capabilities).toEqual({ sessionWorkerPorts: true })
    expect(assertStructuredCloneable(init)).toEqual(init)
  })

  test("the production entry accepts the built init", async () => {
    const init = await runInit({ snapshot: { workerCount: 1, clock: () => 200 } })

    await using worker = await bootSessionWorker({ init, timeoutMs: 20_000 })
    expect(worker.ready.type).toBe("ready")
  }, 30_000)
})

// ---------------------------------------------------------------------------
// Production pool wrapper (step 10).
// ---------------------------------------------------------------------------

describe("createProductionWorkerPool", () => {
  test("wraps the production entry with an oversubscribed pool that becomes ready on use", async () => {
    const cores = 4
    const count = resolveWorkerCount({ availableParallelism: () => cores, env: {} })
    const pool = createProductionWorkerPool({
      init: sessionWorkerInit(),
      count,
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })

    expect(count).toBeGreaterThan(cores)
    expect(pool.workers).toHaveLength(count)

    await Promise.all(pool.workers.map((worker, index) => worker.execute({ sessionID: sid(`ses_${index}`), depth: 0 })))

    expect(pool.workers.every((worker) => worker.alive)).toBe(true)
    for (const index of pool.workers.keys()) {
      expect(pool.resultFor(sid(`ses_${index}`))?.sessionID).toBe(sid(`ses_${index}`))
    }

    await pool.terminateAll()
  }, 30_000)
})
