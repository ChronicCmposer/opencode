// PT-2 — worker-side plugin tools: init descriptor shipping, the stub
// `Plugin.Service.list()` exposure, schema parity and the degraded boot.
//
// The hub owns plugin loading, so a plugin tool's `execute` (which closes over
// hub plugin state) can never cross the isolate. The hub instead ships
// clone-safe `PluginToolDescriptor`s in `SessionWorkerInit`; the worker's
// reduced `Plugin.Service.list()` exposes one `RemotePluginToolDefinition` per
// descriptor so the real `ToolRegistry` registers a proxy stub. These tests pin:
//   * `SessionWorkerInit.pluginTools` ships (and survives structuredClone);
//   * the stub list() exposes a remote definition per descriptor;
//   * the descriptor's `jsonSchema`/`description` reach the stub verbatim;
//   * a stub execute forwards `{ toolID, sessionID, messageID, callID, agent,
//     args }` to the executor and returns its result;
//   * degraded boot (no executor) lists nothing.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Plugin } from "@/plugin"
import { ModelClient, buildSessionWorkerLayer, type SessionLayerOptions } from "@/session/worker/layer"
import { buildSessionWorkerInit } from "@/session/worker/init"
import { parseSessionWorkerInit } from "@/session/worker/session-worker"
import {
  isRemotePluginToolDefinition,
  type PluginToolDescriptor,
  type PluginToolExecuteInput,
  type PluginToolExecuteResult,
  type PluginToolExecutor,
} from "@/session/worker/plugin-tool-wire"
import { assertStructuredCloneable, sessionWorkerInit } from "../../lib/session-worker-init"
import { workerSnapshot } from "../../lib/snapshot"

/** A clone-safe descriptor, the only plugin-tool data that crosses the isolate. */
const descriptor = (
  id: string,
  jsonSchema: unknown = { type: "object", properties: {}, additionalProperties: false },
): PluginToolDescriptor => ({ id, description: `${id} tool`, jsonSchema })

/** The clone-safe result a hub execution returns. */
const executeResult: PluginToolExecuteResult = {
  title: "remote title",
  output: "hub output",
  metadata: { ranOnHub: true },
}

/** A recording executor that captures every proxied invocation. */
const recordingExecutor = (calls: PluginToolExecuteInput[], result = executeResult): PluginToolExecutor => ({
  execute: async (input) => {
    calls.push(input)
    return result
  },
})

// ---------------------------------------------------------------------------
// Worker `Plugin.Service.list()` stub exposure
// ---------------------------------------------------------------------------

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const stubModel = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) })

/** Build the full worker graph with the supplied remote plugin tools. */
const built = (pluginTools?: SessionLayerOptions["pluginTools"]) =>
  buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: clients(),
    modelClient: stubModel(),
    ...(pluginTools === undefined ? {} : { pluginTools }),
  })

/** Resolve the graph's `Plugin.Service.list()` exactly as `ToolRegistry` reads it. */
const listPlugins = (layer: ReturnType<typeof buildSessionWorkerLayer>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.list()
    }).pipe(Effect.provide(layer.layer)),
  )

describe("worker plugin tool descriptors (PT-2)", () => {
  test("list() exposes one remote definition per shipped descriptor", async () => {
    const hooks = await listPlugins(
      built({
        descriptors: [descriptor("compress"), descriptor("prune")],
        executor: recordingExecutor([]),
      }),
    )

    expect(hooks).toHaveLength(1)
    const tools = hooks[0].tool ?? {}
    expect(Object.keys(tools).sort()).toEqual(["compress", "prune"])
    for (const value of Object.values(tools)) {
      expect(isRemotePluginToolDefinition(value)).toBe(true)
    }
  })

  test("a stub carries the descriptor's description and jsonSchema verbatim (schema parity)", async () => {
    const jsonSchema = {
      type: "object",
      properties: { reason: { type: "string", description: "why" } },
      required: ["reason"],
    }
    const hooks = await listPlugins(
      built({ descriptors: [descriptor("compress", jsonSchema)], executor: recordingExecutor([]) }),
    )

    const def = hooks[0].tool?.compress
    if (!isRemotePluginToolDefinition(def)) throw new Error("compress was not a remote plugin tool")
    expect(def.description).toBe("compress tool")
    // The worker must advertise the hub-computed schema, never re-derive it.
    expect(def.jsonSchema).toEqual(jsonSchema)
  })

  test("a stub execute forwards the frozen proxy shape and returns the executor result", async () => {
    const calls: PluginToolExecuteInput[] = []
    const hooks = await listPlugins(
      built({ descriptors: [descriptor("compress")], executor: recordingExecutor(calls) }),
    )

    const def = hooks[0].tool?.compress
    if (!isRemotePluginToolDefinition(def)) throw new Error("compress was not a remote plugin tool")

    const args = { reason: "prune" }
    const result = await def.execute(args, {
      sessionID: "ses_1",
      messageID: "msg_1",
      callID: "call_1",
      agent: "build",
    })

    expect(calls).toEqual([
      { toolID: "compress", sessionID: "ses_1", messageID: "msg_1", callID: "call_1", agent: "build", args },
    ])
    expect(result).toEqual(executeResult)
  })

  test("degraded boot (no remote plugin tools) lists nothing", async () => {
    expect(await listPlugins(built())).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// `SessionWorkerInit.pluginTools` shipping
// ---------------------------------------------------------------------------

describe("SessionWorkerInit.pluginTools (PT-2)", () => {
  test("parseSessionWorkerInit preserves shipped descriptors", () => {
    const pluginTools = [descriptor("compress")]
    const parsed = parseSessionWorkerInit({ ...sessionWorkerInit(), pluginTools })
    expect(parsed.pluginTools).toEqual(pluginTools)
  })

  test("parseSessionWorkerInit fails loud on a non-array pluginTools", () => {
    expect(() =>
      parseSessionWorkerInit({ ...sessionWorkerInit(), pluginTools: "nope" as unknown as PluginToolDescriptor[] }),
    ).toThrow(/pluginTools must be an array/)
  })

  test("buildSessionWorkerInit ships clone-safe descriptors", async () => {
    const init = await Effect.runPromise(
      buildSessionWorkerInit({
        model: { session: { providerID: FIXTURE_PROVIDER, modelID: FIXTURE_MODEL } },
        pluginTools: [descriptor("compress", { type: "object", properties: { reason: { type: "string" } } })],
      }).pipe(
        Effect.provide(initLayers()),
        Effect.provideService(InstanceRef, instanceCtx),
      ),
    )

    expect(init.pluginTools).toHaveLength(1)
    expect(init.pluginTools?.[0].id).toBe("compress")
    expect(assertStructuredCloneable(init).pluginTools).toEqual(init.pluginTools)
  })
})

// ---------------------------------------------------------------------------
// Minimal model/init layer harness (mirrors `init.test.ts`)
// ---------------------------------------------------------------------------

const FIXTURE_PROVIDER = "fixture"
const FIXTURE_MODEL = "fixture-model"

const fixtureModel = {
  id: FIXTURE_MODEL,
  providerID: FIXTURE_PROVIDER,
  api: { id: "fixture-api-id", url: "https://fallback.example/v1", npm: "@ai-sdk/anthropic" },
  name: "Fixture Model",
  family: "fixture",
  capabilities: { temperature: false, reasoning: true, attachment: false, toolcall: true },
  cost: { input: 1, output: 2, cache: { read: 0, write: 0 } },
  limit: { context: 200_000, output: 32_000 },
  status: "active",
  options: { temperature: 0.2 },
  headers: {},
  release_date: "2025-01-01",
} as unknown as Provider.Model

const initLayers = () =>
  Layer.mergeAll(
    Layer.mock(Provider.Service, {
      defaultModel: () =>
        Effect.succeed({ providerID: ProviderV2.ID.make(FIXTURE_PROVIDER), modelID: ModelV2.ID.make(FIXTURE_MODEL) }),
      getProvider: (providerID) =>
        Effect.succeed({
          id: providerID,
          name: "Fixture",
          source: "config",
          env: [],
          key: "k",
          options: {},
          models: {},
        } as unknown as Provider.Info),
      getModel: (providerID, modelID) =>
        Effect.succeed({ ...fixtureModel, id: modelID, providerID } as unknown as Provider.Model),
    }),
    Layer.mock(Auth.Service, {
      get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk" })),
      all: () => Effect.succeed({}),
    }),
    Layer.mock(Config.Service, {
      get: () => Effect.succeed({} as ConfigV1.Info),
      directories: () => Effect.succeed(["/cfg/opencode"]),
    }),
    Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
    RuntimeFlags.layer({ experimentalSessionWorkers: true }),
  )

const instanceCtx = {
  directory: "/tmp/opencode-pt2",
  worktree: "/tmp/opencode-pt2",
  project: { id: "prj_pt2", worktree: "/tmp/opencode-pt2", vcs: "git" } as never,
}
