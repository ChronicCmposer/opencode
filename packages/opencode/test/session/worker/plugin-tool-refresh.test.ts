// L1 — the worker-side layer seam for hub-executed plugin tools.
//
// F3: `RemotePluginTools` descriptors are refreshable. A hub-side plugin reload
//     can replace the set after the worker graph is built, and the worker's
//     `Plugin.Service.list()` (the exact call `ToolRegistry` makes) must expose
//     the new set on its next read.
// F4: `pluginTools` must reach the reduced LLM sub-graph too, not just the full
//     worker graph, so a future tool-consuming reduced node sees remote tools.
// F7: the layer exposes the descriptor collision policy (fail loud on a
//     duplicate remote id or a reserved local/builtin id) before registration.
// Degraded boot (no remote tools) still lists nothing.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Plugin } from "@/plugin"
import {
  ModelClient,
  PluginToolIDCollisionError,
  assertPluginToolDescriptorsUnique,
  buildSessionWorkerLayer,
  llmBoundaryReplacements,
  refreshablePluginTools,
  type RemotePluginTools,
  type SessionLayerOptions,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import type {
  PluginToolDescriptor,
  PluginToolExecuteInput,
  PluginToolExecuteResult,
  PluginToolExecutor,
} from "@/session/worker/plugin-tool-wire"
import { workerSnapshot } from "../../lib/snapshot"

/** A clone-safe descriptor, the only plugin-tool data that crosses the isolate. */
const descriptor = (id: string): PluginToolDescriptor => ({
  id,
  description: `${id} tool`,
  jsonSchema: { type: "object", properties: {}, additionalProperties: false },
})

const executeResult: PluginToolExecuteResult = { title: "remote", output: "hub", metadata: {} }

/** A recording executor; refresh tests never call it, only construction does. */
const recordingExecutor = (calls: PluginToolExecuteInput[] = []): PluginToolExecutor => ({
  execute: async (input) => {
    calls.push(input)
    return executeResult
  },
})

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const stubModel = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) })

/** Build the full worker graph with the supplied remote plugin tools. */
const built = (pluginTools?: RemotePluginTools) =>
  buildSessionWorkerLayer({
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: clients(),
    modelClient: stubModel(),
    ...(pluginTools === undefined ? {} : { pluginTools }),
  })

/** Resolve a graph's `Plugin.Service.list()` exactly as `ToolRegistry` reads it. */
const listPlugins = (layer: ReturnType<typeof buildSessionWorkerLayer>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.list()
    }).pipe(Effect.provide(layer.layer)),
  )

/** The sorted tool ids in the first (and only) synthetic hooks entry. */
const listedIDs = (hooks: Awaited<ReturnType<typeof listPlugins>>): string[] =>
  Object.keys(hooks[0]?.tool ?? {}).sort()

// ---------------------------------------------------------------------------
// F3 — refreshable descriptors
// ---------------------------------------------------------------------------

describe("refreshable plugin tools (L1/F3)", () => {
  test("refresh replaces what Plugin.Service.list() exposes on the same graph", async () => {
    const tools = refreshablePluginTools([descriptor("compress")], recordingExecutor())
    const layer = built(tools)

    expect(listedIDs(await listPlugins(layer))).toEqual(["compress"])

    tools.refresh([descriptor("prune"), descriptor("recall")])

    // The same built graph now lists the refreshed set: `list()` reads the live
    // descriptors, so the caller need not rebuild the worker layer.
    expect(listedIDs(await listPlugins(layer))).toEqual(["prune", "recall"])
  })

  test("a refresh to an empty set lists no plugin tools", async () => {
    const tools = refreshablePluginTools([descriptor("compress")], recordingExecutor())
    const layer = built(tools)

    tools.refresh([])

    expect(await listPlugins(layer)).toEqual([])
  })

  test("degraded boot (no remote plugin tools) lists nothing", async () => {
    expect(await listPlugins(built())).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// F4 — pluginTools reach the reduced LLM sub-graph
// ---------------------------------------------------------------------------

const config: WorkerModelConfig = {
  providerID: "mock",
  modelID: "mock-model",
  apiId: "mock-model",
  npm: "@ai-sdk/openai-compatible",
  baseUrl: "http://127.0.0.1:1/v1",
  apiKey: "k",
  headers: {},
}

/** The reduced LLM sub-graph's `Plugin` replacement for the supplied tools. */
const reducedPluginLayer = (pluginTools?: RemotePluginTools): Layer.Layer<Plugin.Service> => {
  const entry = llmBoundaryReplacements(workerSnapshot(), config, undefined, undefined, pluginTools).find(
    ([node]) => node.name === Plugin.node.name,
  )
  if (entry === undefined) throw new Error("reduced LLM sub-graph has no Plugin replacement")
  return entry[1] as Layer.Layer<Plugin.Service>
}

const listReduced = (pluginTools?: RemotePluginTools) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.list()
    }).pipe(Effect.provide(reducedPluginLayer(pluginTools))),
  )

describe("reduced LLM sub-graph plugin tools (L1/F4)", () => {
  test("pluginTools reaches the reduced sub-graph's Plugin replacement", async () => {
    const hooks = await listReduced({ descriptors: [descriptor("compress")], executor: recordingExecutor() })

    expect(Object.keys(hooks[0]?.tool ?? {})).toEqual(["compress"])
  })

  test("a refreshable handle is live in the reduced sub-graph too", async () => {
    const tools = refreshablePluginTools([descriptor("compress")], recordingExecutor())

    expect(Object.keys((await listReduced(tools))[0]?.tool ?? {})).toEqual(["compress"])

    tools.refresh([descriptor("prune")])
    expect(Object.keys((await listReduced(tools))[0]?.tool ?? {})).toEqual(["prune"])
  })

  test("absent pluginTools leaves the reduced sub-graph listing nothing", async () => {
    expect(await listReduced(undefined)).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// F7 — collision policy
// ---------------------------------------------------------------------------

/** Run `run` and return its collision error (asserting it is the typed one). */
const collisionOf = (run: () => unknown): PluginToolIDCollisionError => {
  try {
    run()
  } catch (error) {
    if (error instanceof PluginToolIDCollisionError) return error
    throw error
  }
  throw new Error("expected a PluginToolIDCollisionError")
}

describe("plugin tool id collisions (L1/F7)", () => {
  test("distinct ids pass through unchanged", () => {
    const descriptors = [descriptor("compress"), descriptor("prune")]
    expect(assertPluginToolDescriptorsUnique(descriptors, ["read", "bash"])).toBe(descriptors)
  })

  test("a duplicate remote id fails loud as a duplicate", () => {
    const error = collisionOf(() =>
      assertPluginToolDescriptorsUnique([descriptor("compress"), descriptor("compress")]),
    )
    expect(error.kind).toBe("duplicate")
    expect(error.id).toBe("compress")
  })

  test("a remote id colliding with a reserved local/builtin id fails loud", () => {
    const error = collisionOf(() => assertPluginToolDescriptorsUnique([descriptor("read")], ["read", "bash"]))
    expect(error.kind).toBe("reserved")
    expect(error.id).toBe("read")
    expect(error.message).toMatch(/collides with a locally registered tool/)
  })

  test("refreshablePluginTools rejects a colliding initial set", () => {
    expect(() => refreshablePluginTools([descriptor("x"), descriptor("x")], recordingExecutor())).toThrow(
      PluginToolIDCollisionError,
    )
  })

  test("a rejected refresh keeps the previous descriptor set", () => {
    const tools = refreshablePluginTools([descriptor("compress")], recordingExecutor())

    expect(() => tools.refresh([descriptor("prune"), descriptor("prune")])).toThrow(PluginToolIDCollisionError)
    expect(tools.descriptors.map((item) => item.id)).toEqual(["compress"])
  })

  test("Plugin.Service.list() fails loud on a duplicate set before registration", async () => {
    const tools: RemotePluginTools = {
      descriptors: [descriptor("compress"), descriptor("compress")],
      executor: recordingExecutor(),
    }

    await expect(listPlugins(built(tools))).rejects.toThrow(/declared by more than one remote descriptor/)
  })
})
