// fix2 / ws2 — the worker graph replaces `Plugin.node` for the WHOLE graph.
//
// The session worker must never build the global/heavy `Plugin` layer: its
// model-facing hooks run either in the worker-hosted {@link WorkerPluginHost}
// (when one is supplied) or as a no-op pass-through. These tests pin that the
// top-level worker layer owns the `Plugin.node` replacement and that a shipped
// host's hook reaches `Plugin.Service`.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Plugin } from "@/plugin"
import { ModelClient, buildSessionWorkerLayer, type SessionLayerOptions } from "@/session/worker/layer"
import { createWorkerPluginHost } from "@/session/worker/plugin-host"
import { workerSnapshot } from "../../lib/snapshot"

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const stubModel = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) })

const options = (overrides: Partial<SessionLayerOptions> = {}): SessionLayerOptions => ({
  snapshot: workerSnapshot(),
  env: { values: {} },
  clients: clients(),
  modelClient: stubModel(),
  ...overrides,
})

/** Trigger `chat.headers` through the graph's `Plugin.Service`. */
const triggerHeaders = (built: ReturnType<typeof buildSessionWorkerLayer>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return yield* plugin.trigger("chat.headers", {} as never, { headers: {} })
    }).pipe(Effect.provide(built.layer)),
  )

describe("worker graph Plugin.node replacement (fix2-ws2)", () => {
  test("a supplied WorkerPluginHost serves Plugin.Service, so its hook reaches the worker", async () => {
    const reached: string[] = []
    const host = createWorkerPluginHost([
      {
        name: "worker-headers",
        hooks: {
          "chat.headers": (_input, output) => {
            reached.push("worker-headers")
            output.headers["x-worker-plugin"] = "1"
          },
        },
      },
    ])

    const built = buildSessionWorkerLayer(options({ plugins: host }))
    const result = await triggerHeaders(built)

    expect(result.headers).toEqual({ "x-worker-plugin": "1" })
    expect(reached).toEqual(["worker-headers"])
  })

  test("without a host, Plugin.Service is the no-op pass-through (no header injected)", async () => {
    const built = buildSessionWorkerLayer(options())
    const result = await triggerHeaders(built)

    expect(result.headers).toEqual({})
  })

  test("the worker graph always replaces Plugin.node, never building the global/heavy layer", () => {
    const builtWithHost = (plugins?: SessionLayerOptions["plugins"]) =>
      buildSessionWorkerLayer(options(plugins === undefined ? {} : { plugins }))

    for (const built of [builtWithHost(), builtWithHost(createWorkerPluginHost([{ name: "p", hooks: {} }]))]) {
      const replacement = built.replacements.find(([node]) => node === Plugin.node)?.[1]
      expect(replacement).toBeDefined()
      // The worker supplies its own layer; the real `Plugin.node` implementation
      // (whose state imports the server and loads plugins) is never built.
      expect(replacement).not.toBe(Plugin.node.implementation)
    }
  })
})
