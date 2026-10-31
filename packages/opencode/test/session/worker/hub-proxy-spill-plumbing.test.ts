import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import { createSessionHub } from "@/session/worker/hub"
import { buildSessionWorkerInit } from "@/session/worker/init"
import type { HubProxySpill } from "@/session/worker/hub-proxy-spill"
import { sweepHubProxySpillAtStartup } from "@/session/worker/production-hub"
import { hubProxyClientOptions, parseSessionWorkerInit } from "@/session/worker/session-worker"
import { makeAttachments } from "@/session/worker/worker-ports"
import { createTestBarrier } from "../../lib/run-barrier-test"
import { createFakeExecutorPool } from "../../lib/fake-worker-pool"
import { sessionWorkerInit } from "../../lib/session-worker-init"
import { sid } from "../../lib/async"

const testLayers = () =>
  Layer.mergeAll(
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make("mock"), modelID: ModelV2.ID.make("m") }),
      getProvider: () =>
        Effect.succeed({ id: "mock", name: "Mock", source: "config", env: [], key: "k", options: {}, models: {} } as never),
      getModel: () =>
        Effect.succeed({
          id: "m",
          providerID: "mock",
          api: { id: "mock-api", url: "http://127.0.0.1:0/v1", npm: "@ai-sdk/openai-compatible" },
          name: "Mock",
          family: "mock",
          capabilities: { temperature: true, reasoning: false },
          cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
          limit: { context: 128_000, output: 4096 },
          status: "active",
          options: {},
          headers: {},
          release_date: "2025-01-01",
        } as never),
    }),
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-auth" })),
      all: () => Effect.succeed({}),
    }),
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({} as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
    Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
    RuntimeFlags.layer({ experimentalSessionWorkers: true }),
  )

const instance = {
  directory: "/tmp/spill-plumbing",
  worktree: "/tmp/spill-plumbing",
  project: { id: "prj_spill_plumbing", worktree: "/tmp/spill-plumbing", vcs: "git" },
} as never

const fakeSpill = (overrides: Partial<HubProxySpill> = {}): HubProxySpill & { readonly scopes: string[] } => {
  const scopes: string[] = []
  return {
    scopes,
    write: async () => {
      throw new Error("unused")
    },
    read: async () => {
      throw new Error("unused")
    },
    remove: async () => {},
    removeScope: async (scope) => {
      scopes.push(scope)
      return 0
    },
    sweep: async () => 0,
    ...overrides,
  }
}

describe("hub-proxy spill plumbing", () => {
  test("the hub-proxy server for a worker receives the spill and its worker scope", () => {
    const spill = fakeSpill()
    const seen: unknown[] = []
    const attachments = makeAttachments({
      backends: { hubProxy: {} },
      hubProxySpill: spill,
      serveHubProxy: (_handlers, _port, _streaming, options) => {
        seen.push(options)
        return () => {}
      },
    })
    attachments.attachHubProxyPort(7, new MessageChannel().port1)
    expect(seen).toEqual([{ spill, spillScope: "worker-7" }])
  })

  test("without a spill the hub-proxy server gets no spill options", () => {
    const seen: unknown[] = []
    const attachments = makeAttachments({
      backends: { hubProxy: {} },
      serveHubProxy: (_handlers, _port, _streaming, options) => {
        seen.push(options)
        return () => {}
      },
    })
    attachments.attachHubProxyPort(7, new MessageChannel().port1)
    expect(seen).toEqual([{}])
  })

  test("releasing a generation removes the retired worker's scope and survives a failing removal", async () => {
    const good = fakeSpill()
    const hub = createSessionHub({
      barrier: createTestBarrier(),
      pool: createFakeExecutorPool(1),
      hubProxySpill: good,
    })
    await hub.releaseGeneration(sid("ses_spill"), 3)
    expect(good.scopes).toEqual(["worker-3"])

    let attempts = 0
    const bad = fakeSpill({
      removeScope: async () => {
        attempts += 1
        throw new Error("disk gone")
      },
    })
    const failing = createSessionHub({ barrier: createTestBarrier(), pool: createFakeExecutorPool(1), hubProxySpill: bad })
    await failing.releaseGeneration(sid("ses_spill"), 4)
    expect(attempts).toBe(1)
  })

  test("the startup sweep removes everything once and survives a rejection", async () => {
    const calls: number[] = []
    const ok = fakeSpill({
      sweep: async (age) => {
        calls.push(age)
        return 2
      },
    })
    expect(await sweepHubProxySpillAtStartup(ok)).toBe(2)
    expect(calls).toEqual([0])
    const bad = fakeSpill({
      sweep: async () => {
        throw new Error("boom")
      },
    })
    expect(await sweepHubProxySpillAtStartup(bad)).toBe(0)
  })

  test("the worker init carries the spill directory only when one is given", async () => {
    const withDir = await Effect.runPromise(
      buildSessionWorkerInit({
        model: { session: { providerID: "mock", modelID: "m" } },
        hubProxySpillDir: "/db/hub-proxy-spill",
      }).pipe(Effect.provide(testLayers()), Effect.provideService(InstanceRef, instance)),
    )
    expect(withDir.hubProxySpillDir).toBe("/db/hub-proxy-spill")
    const without = await Effect.runPromise(
      buildSessionWorkerInit({ model: { session: { providerID: "mock", modelID: "m" } } }).pipe(
        Effect.provide(testLayers()),
        Effect.provideService(InstanceRef, instance),
      ),
    )
    expect("hubProxySpillDir" in without).toBe(false)
  })

  test("the parsed init keeps the spill directory", () => {
    const init = sessionWorkerInit({})
    expect(parseSessionWorkerInit({ ...init, hubProxySpillDir: "/x" }).hubProxySpillDir).toBe("/x")
    expect(parseSessionWorkerInit(init).hubProxySpillDir).toBeUndefined()
  })

  test("the worker's hub-proxy client gets a spill and worker scope only when the directory is set", () => {
    expect(hubProxyClientOptions({ workerID: 5 }, 9)).toEqual({})
    const options = hubProxyClientOptions({ workerID: 5, hubProxySpillDir: "/db/hub-proxy-spill" }, 9)
    expect(options.spillScope).toBe("worker-5")
    expect(options.spill).toBeDefined()
    expect(hubProxyClientOptions({ hubProxySpillDir: "/d" }, 9).spillScope).toBe("worker-9")
  })
})
