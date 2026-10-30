import { describe, expect, test } from "bun:test"
import { createSessionBoundExecutorWorker } from "../../lib/lazy-worker-pool"
import { Effect, Exit, Layer, Scope } from "effect"
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
import { DEFAULT_REAP_MS, REAP_MS_ENV, reapMsWarning, resolveReapMs } from "@/session/worker/pool"
import { createSessionHub, makeProduction } from "@/session/worker/hub"
import type { IdleBoundedExecutorWorkerPool, ProductionWorkerPoolOptions } from "@/session/worker/hub"
import { createTestBarrier } from "../../lib/run-barrier-test"
import type { DbExecutor } from "@/session/worker/db-executor"
import { createDispatcher } from "../../lib/dispatcher"
import type { ExecutorWorker } from "@/session/worker/execute"
import { buildWorkerSnapshot, createEpochSequence, nextEpoch } from "@/session/worker/snapshot"
import { assertStructuredCloneable, sessionWorkerInit } from "../../lib/session-worker-init"
import { bootSessionWorker } from "../../lib/session-worker-harness"
import { createFakeWorkerPool } from "../../lib/fake-worker-pool"
import { flush, pollUntil, sid } from "../../lib/async"

/** The dependency-light executor fixture (speaks the executor protocol). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)

/** Build the one scoped production pool for a test and dispose it afterwards. */
async function createProductionPool(options: ProductionWorkerPoolOptions) {
  const scope = Effect.runSync(Scope.make())
  const pools = await Effect.runPromise(Scope.provide(scope)(makeProduction(options)))
  return {
    pool: pools.executorPool as IdleBoundedExecutorWorkerPool,
    dispose: () => Effect.runPromise(Scope.close(scope, Exit.void)).then(() => undefined),
  }
}

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
  clock?: () => number
  epoch?: number
  epochSequence?: ReturnType<typeof createEpochSequence>
}) =>
  buildWorkerSnapshot(options).pipe(Effect.provide(snapshotLayers), Effect.provideService(InstanceRef, instanceCtx))

describe("buildWorkerSnapshot", () => {
  test("produces the layer snapshot shape from hub services", async () => {
    const snapshot = await Effect.runPromise(buildSnapshot({ clock: () => 1000 }))

    expect(snapshot.directory).toBe("/tmp/opencode-proj")
    expect(snapshot.directories).toEqual(hubDirectories)
    expect("workerCount" in snapshot).toBe(false)
    expect(snapshot.flags.experimentalSessionWorkers).toBe(true)
    expect(snapshot.config).toEqual({ model: "test/model" })
    expect(snapshot.auth.anthropic).toEqual({ type: "api", key: "sk-secret" })
    expect(snapshot.modelsDev.anthropic?.id).toBe("anthropic")
    expect(typeof snapshot.epoch).toBe("number")
  })

  test("is JSON-serializable (structured-clone friendly)", async () => {
    const snapshot = await Effect.runPromise(buildSnapshot({ clock: () => 5 }))

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
    const first = await Effect.runPromise(buildSnapshot({ clock: () => 1_000, epochSequence }))
    const second = await Effect.runPromise(buildSnapshot({ clock: () => 10, epochSequence }))
    const third = await Effect.runPromise(buildSnapshot({ clock: () => 5, epochSequence }))

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
      cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
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
      snapshot: { clock: () => 100 },
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
      cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
    })
    expect(init.snapshot.directory).toBe("/tmp/opencode-proj")
    expect("workerCount" in init.snapshot).toBe(false)
    expect(assertStructuredCloneable(init)).toEqual(init)
  })

  test("carries the requested worker capabilities", async () => {
    const init = await runInit({
      snapshot: { clock: () => 300 },
      capabilities: { sessionWorkerPorts: true },
    })

    expect(init.capabilities).toEqual({ sessionWorkerPorts: true })
    expect(assertStructuredCloneable(init)).toEqual(init)
  })

  test("the production entry accepts the built init", async () => {
    const init = await runInit({ snapshot: { clock: () => 200 } })

    await using worker = await bootSessionWorker({ init, timeoutMs: 20_000 })
    expect(worker.ready.type).toBe("ready")
  }, 90_000)
})

// ---------------------------------------------------------------------------
// Production pool wrapper.
// ---------------------------------------------------------------------------

describe("makeProduction pool", () => {
  test("wraps the production entry with a pool that becomes ready on use", async () => {
    const { pool, dispose } = await createProductionPool({
      init: sessionWorkerInit(),
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
    })

    try {
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
    } finally {
      await dispose()
    }
  }, 90_000)
})

describe("idle eviction retirement", () => {
  test("an idle-cap eviction runs the dispatcher retirement and releases hub ports + lease", async () => {
    const served: number[] = []
    const disposedPorts: number[] = []
    const zeroed: string[] = []
    let leaseOwnerQueries = 0
    let leaseObservedSession: string | undefined
    let retiredOwner = ""
    let hub: ReturnType<typeof createSessionHub> | undefined

    const db: DbExecutor = {
      threadId: 0,
      query: async (sql, params) => {
        if (!sql.includes("event_sequence")) return []
        // Count only the retired session's lease reads: the successor's own boot
        // poll is expected and must not mask a retirement re-read.
        if (Array.isArray(params) && params[0] === leaseObservedSession) leaseOwnerQueries += 1
        return [{ worker_owner_id: retiredOwner }]
      },
      run: async (sql, params) => {
        if (sql.includes("worker_expires_at = 0") && Array.isArray(params)) zeroed.push(String(params[0]))
        return { changes: 1, lastInsertRowid: 0 }
      },
      transaction: async () => [],
      close: async () => {},
    }
    const scoped = await createProductionPool({
      init: sessionWorkerInit(),
      entry: EXECUTOR_FIXTURE,
      workKind: "instant",
      timeoutMs: 15_000,
      maxIdleWorkers: 1,
      // The production hub's hook: idle eviction takes the dispatcher retirement
      // path (isolate exit, hub ports, pending asks, retired lease), not only the
      // pool's local `releaseSession`.
      onIdleEvicted: (sessionID) => hub?.dispatcher.retire(sessionID) ?? Promise.resolve(),
    })
    const pool = scoped.pool
    hub = createSessionHub({ ...{ barrier: createTestBarrier() },
      pool,
      db,
      ports: {
        egress: (workerID) => {
          served.push(workerID)
          return () => {
            disposedPorts.push(workerID)
          }
        },
      },
      onWorkerCrashed: () => {},
    })
    const sessionHub = hub
    try {
      const first = sid("ses_idle_evict_first")
      leaseObservedSession = String(first)
      await sessionHub.ensureRunning(first)
      const firstWorker = sessionHub.dispatcher.registry.worker(first)?.id
      expect(firstWorker).toBeDefined()
      retiredOwner = `worker-${firstWorker}-1-uuid`
      const beforeBoot = leaseOwnerQueries
      await pollUntil(() => leaseOwnerQueries > beforeBoot, {
        message: "the boot lease-owner poll never observed the claimed owner",
      })
      sessionHub.attachWorkerPorts(firstWorker!, { egress: {} as MessagePort })
      expect(served).toEqual([firstWorker!])

      const beforeEviction = leaseOwnerQueries
      await sessionHub.ensureRunning(sid("ses_idle_evict_second"))
      await pollUntil(
        () => zeroed.includes(String(first)) && disposedPorts.includes(firstWorker!),
        { message: "the evicted generation's hub ports/lease were not released", timeoutMs: 10_000 },
      )

      expect(leaseOwnerQueries).toBe(beforeEviction)
      expect(zeroed).toEqual([String(first)])
      expect(disposedPorts).toEqual([firstWorker!])
      expect(sessionHub.dispatcher.workerForSession(first)).toBeUndefined()
      expect(pool.idleEvictions).toBe(1)
    } finally {
      await sessionHub.terminateAll()
      await scoped.dispose()
    }
  }, 90_000)
})

describe("dispatch pool constants", () => {
  test("dispatch does not re-export duplicate pool constants", async () => {
    const dispatch = await import("@/session/worker/dispatch")
    expect("DEFAULT_SESSION_WORKER_REAP_MS" in dispatch).toBe(false)
    expect("SESSION_WORKER_REAP_ENV" in dispatch).toBe(false)
    expect("resolveSessionWorkerReapMs" in dispatch).toBe(false)
  })
})

describe("one production pool", () => {
  test("test-only lazy factories and the duplicate production factory are gone", async () => {
    const workerPools = await import("@/session/worker/worker-pools")
    expect(workerPools).not.toHaveProperty("createLazyExecutorWorker")
    expect(workerPools).not.toHaveProperty("createLazyExecutorWorkerPool")
    expect(workerPools).not.toHaveProperty("createProductionWorkerPool")
    expect(workerPools).not.toHaveProperty("fromPool")
    expect(typeof workerPools.makeProduction).toBe("function")
  })
})

describe("termination ordering", () => {
  test("a repeated terminate joins the in-flight orderly close", async () => {
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    let calls = 0
    const inner: ExecutorWorker = {
      id: 1,
      get alive() {
        return true
      },
      execute: async () => {},
      cancel: async () => {},
      assertNotBusy: async () => false,
      resultFor: () => undefined,
      terminate: async () => {
        calls += 1
        await gate
      },
    }
    const worker = createSessionBoundExecutorWorker(1, async () => inner)
    await worker.execute({ sessionID: sid("ses_close_cache"), depth: 0 })

    const first = worker.terminate()
    const second = worker.terminate()
    let secondSettled = false
    void second.then(() => {
      secondSettled = true
    })
    await flush()

    // The second caller must wait for the same close, not observe a fake exit.
    expect(calls).toBe(1)
    expect(secondSettled).toBe(false)

    release()
    await Promise.all([first, second])
    expect(secondSettled).toBe(true)
  })

  test("a dead idle replacement finishes closing before its successor starts", async () => {
    const pool = createFakeWorkerPool(1)
    const dispatcher = createDispatcher({ pool, terminateTimeoutMs: 5_000 })
    const sessionID = sid("ses_dead_idle_ordering")

    const first = dispatcher.dispatch({ sessionID })
    await flush()
    const idle = pool.spawned[0]!
    idle.finish(sessionID)
    await first

    // The parked worker dies between turns. Its orderly close is slow.
    idle.alive = false
    let releaseClose!: () => void
    const close = new Promise<void>((resolve) => {
      releaseClose = resolve
    })
    let closed = false
    idle.terminate = async () => {
      await close
      closed = true
    }

    const second = dispatcher.dispatch({ sessionID })
    await flush()

    // The dead generation is still closing: no successor may be materialized.
    expect(closed).toBe(false)
    expect(pool.spawned).toHaveLength(1)

    releaseClose()
    await pollUntil(() => closed && pool.spawned[1]?.runs.length === 1, {
      message: "the successor never started after the old isolate closed",
      timeoutMs: 1_000,
    })
    pool.spawned[1]!.finish(sessionID)
    await second
    await dispatcher.terminateAll()
  })
})
