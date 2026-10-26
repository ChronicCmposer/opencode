// Session worker pool configuration and the production pool wrapper.
//
// The sized pool is gone: one lazily-materialized worker per session replaces
// `OPENCODE_SESSION_WORKER_COUNT` / `OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION`.
// Those knobs only produce obsolete warnings now. `OPENCODE_SESSION_WORKER_REAP_MS`
// still bounds an idle worker's lifetime.

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
  DEFAULT_REAP_MS,
  OVERSUBSCRIPTION_ENV,
  REAP_MS_ENV,
  WORKER_COUNT_ENV,
  obsoleteOversubscriptionWarning,
  obsoleteWorkerCountWarning,
  reapMsWarning,
  removedKnobWarnings,
  resolveReapMs,
} from "@/session/worker/pool"
import { createProductionWorkerPool } from "@/session/worker/hub"
import { buildWorkerSnapshot, createEpochSequence, nextEpoch } from "@/session/worker/snapshot"
import { assertStructuredCloneable, sessionWorkerInit } from "../../lib/session-worker-init"
import { bootSessionWorker } from "../../lib/session-worker-harness"
import { sid } from "../../lib/async"

/** The dependency-light executor fixture (speaks the executor protocol). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

describe("pool sizing removal", () => {
  test("no longer exports a machine-derived pool size", async () => {
    const pool = await import("@/session/worker/pool")

    for (const name of ["resolveWorkerCount", "deriveWorkerCount", "DEFAULT_OVERSUBSCRIPTION_FACTOR"]) {
      expect(pool).not.toHaveProperty(name)
    }
  })
})

describe("reap window", () => {
  test("defaults to five minutes", () => {
    expect(DEFAULT_REAP_MS).toBe(300_000)
    expect(resolveReapMs({})).toBe(DEFAULT_REAP_MS)
  })

  test("parses a positive integer", () => {
    expect(resolveReapMs({ [REAP_MS_ENV]: "1500" })).toBe(1500)
    expect(resolveReapMs({ [REAP_MS_ENV]: " 42 " })).toBe(42)
    expect(resolveReapMs({ [REAP_MS_ENV]: "1" })).toBe(1)
  })

  test("an empty value falls back to the default and reports why", () => {
    for (const raw of ["", "   "]) {
      expect(resolveReapMs({ [REAP_MS_ENV]: raw })).toBe(DEFAULT_REAP_MS)
      const warning = reapMsWarning({ [REAP_MS_ENV]: raw })
      expect(warning).toContain(REAP_MS_ENV)
      expect(warning).toContain("empty")
      expect(warning).toContain(String(DEFAULT_REAP_MS))
    }
  })

  test("a malformed value falls back to the default and reports the raw input", () => {
    for (const raw of ["0", "-1", "1.5", "nope", "NaN", "Infinity"]) {
      expect(resolveReapMs({ [REAP_MS_ENV]: raw })).toBe(DEFAULT_REAP_MS)
      const warning = reapMsWarning({ [REAP_MS_ENV]: raw })
      expect(warning).toContain(REAP_MS_ENV)
      expect(warning).toContain(raw)
      expect(warning).toContain("positive integer")
      expect(warning).toContain(String(DEFAULT_REAP_MS))
    }
  })

  test("warns only when the value is present but unusable", () => {
    expect(reapMsWarning({})).toBeUndefined()
    expect(reapMsWarning({ [REAP_MS_ENV]: "1" })).toBeUndefined()
  })
})

describe("removed pool knobs", () => {
  test("absent and empty knobs produce no warnings", () => {
    expect(removedKnobWarnings({})).toEqual([])
    expect(obsoleteWorkerCountWarning({})).toBeUndefined()
    expect(obsoleteOversubscriptionWarning({})).toBeUndefined()

    const empty = { [WORKER_COUNT_ENV]: "", [OVERSUBSCRIPTION_ENV]: "   " }
    expect(removedKnobWarnings(empty)).toEqual([])
    expect(obsoleteWorkerCountWarning(empty)).toBeUndefined()
    expect(obsoleteOversubscriptionWarning(empty)).toBeUndefined()
  })

  test("each set knob yields exactly one warning naming it as obsolete and ignored", () => {
    const count = obsoleteWorkerCountWarning({ [WORKER_COUNT_ENV]: "5" })
    if (count === undefined) throw new Error("expected an obsolete worker-count warning")
    expect(count).toContain(WORKER_COUNT_ENV)
    expect(count).toContain("obsolete; ignored")
    const countWarnings = removedKnobWarnings({ [WORKER_COUNT_ENV]: "5" })
    expect(countWarnings).toHaveLength(1)
    expect(countWarnings[0]).toBe(count)

    const oversubscription = obsoleteOversubscriptionWarning({ [OVERSUBSCRIPTION_ENV]: "3" })
    if (oversubscription === undefined) throw new Error("expected an obsolete oversubscription warning")
    expect(oversubscription).toContain(OVERSUBSCRIPTION_ENV)
    expect(oversubscription).toContain("obsolete; ignored")
    const oversubscriptionWarnings = removedKnobWarnings({ [OVERSUBSCRIPTION_ENV]: "3" })
    expect(oversubscriptionWarnings).toHaveLength(1)
    expect(oversubscriptionWarnings[0]).toBe(oversubscription)
  })

  test("both knobs yield exactly two warnings in a stable order", () => {
    const warnings = removedKnobWarnings({ [WORKER_COUNT_ENV]: "5", [OVERSUBSCRIPTION_ENV]: "3" })
    expect(warnings).toHaveLength(2)
    expect(warnings[0]).toContain(WORKER_COUNT_ENV)
    expect(warnings[1]).toContain(OVERSUBSCRIPTION_ENV)
  })

  test("malformed values are still only reported as obsolete and never throw", () => {
    const env = { [WORKER_COUNT_ENV]: "abc", [OVERSUBSCRIPTION_ENV]: "0" }
    expect(() => removedKnobWarnings(env)).not.toThrow()
    expect(removedKnobWarnings(env)).toHaveLength(2)
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

const buildSnapshot = (options: {
  workerCount?: number
  clock?: () => number
  epoch?: number
  epochSequence?: ReturnType<typeof createEpochSequence>
}) =>
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

  test("defaults to one worker per session instead of sizing a pool", async () => {
    const snapshot = await Effect.runPromise(buildSnapshot({ clock: () => 1000 }))

    expect(snapshot.workerCount).toBe(1)
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

  test("successive builds are strictly monotonic through one explicit holder", async () => {
    const epochSequence = createEpochSequence()
    const first = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 1_000, epochSequence }))
    const second = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 10, epochSequence }))
    const third = await Effect.runPromise(buildSnapshot({ workerCount: 1, clock: () => 5, epochSequence }))

    expect(second.epoch).toBeGreaterThan(first.epoch)
    expect(third.epoch).toBeGreaterThan(second.epoch)
  })

  test("an explicit holder makes the epoch deterministic regardless of build order", async () => {
    const ascending = async () => {
      const sequence = createEpochSequence()
      return [
        (await Effect.runPromise(buildSnapshot({ clock: () => 100, epochSequence: sequence }))).epoch,
        (await Effect.runPromise(buildSnapshot({ clock: () => 200, epochSequence: sequence }))).epoch,
      ]
    }
    const descending = async () => {
      const sequence = createEpochSequence()
      return [
        (await Effect.runPromise(buildSnapshot({ clock: () => 200, epochSequence: sequence }))).epoch,
        (await Effect.runPromise(buildSnapshot({ clock: () => 100, epochSequence: sequence }))).epoch,
      ]
    }

    // Same clock readings => same epochs on each independently-owned holder,
    // regardless of the order in which they were produced.
    expect(await ascending()).toEqual([100, 200])
    expect(await descending()).toEqual([200, 201])
  })

  test("an explicit epoch wins over the holder and is order-independent", async () => {
    const first = await Effect.runPromise(buildSnapshot({ clock: () => 999, epoch: 7 }))
    const second = await Effect.runPromise(buildSnapshot({ clock: () => 1, epoch: 7 }))

    expect(first.epoch).toBe(7)
    expect(second.epoch).toBe(7)
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
  test("wraps the production entry with a pool that becomes ready on use", async () => {
    const pool = createProductionWorkerPool({
      init: sessionWorkerInit(),
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })

    // No prewarm: each worker materializes lazily under the dispatcher's id.
    expect(pool.workers).toHaveLength(0)
    const workers = Array.from({ length: 4 }, () => pool.spawn())

    await Promise.all(workers.map((worker, index) => worker.execute({ sessionID: sid(`ses_${index}`), depth: 0 })))

    expect(pool.workers).toHaveLength(4)
    expect(pool.workers.every((worker) => worker.alive)).toBe(true)
    for (const index of pool.workers.keys()) {
      expect(pool.resultFor(sid(`ses_${index}`))?.sessionID).toBe(sid(`ses_${index}`))
    }

    await pool.terminateAll()
  }, 30_000)
})
