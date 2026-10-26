// sys-transform / ws3 — the worker plugin host proxies
// `experimental.chat.system.transform` to the hub over the control port.
//
// The hub owns plugin loading, so the system-prompt rewrite runs on the hub and
// returns the mutated `{ system }` to the isolate. `session-worker.ts` derives
// that bridge (`hubProxySystemTransform`) from the shared hub-proxy client; these
// tests pin the bridge and the worker `Plugin.Service` path, plus the degraded
// (no client) pass-through.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Plugin } from "@/plugin"
import {
  ModelClient,
  buildSessionWorkerLayer,
  type HubProxyClient,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import {
  createWorkerPluginHost,
  type SystemTransformInput,
  type SystemTransformOutput,
} from "@/session/worker/plugin-host"
import { hubProxySystemTransform } from "@/session/worker/session-worker"
import { workerSnapshot } from "../../lib/snapshot"

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const options = (overrides: Partial<SessionLayerOptions> = {}): SessionLayerOptions => ({
  snapshot: workerSnapshot(),
  env: { values: {} },
  clients: clients(),
  modelClient: Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "stub" }) }),
  ...overrides,
})

/** Trigger `experimental.chat.system.transform` through the worker `Plugin.Service`. */
const triggerSystemTransform = (
  built: ReturnType<typeof buildSessionWorkerLayer>,
  input: SystemTransformInput,
  output: SystemTransformOutput,
): Promise<SystemTransformOutput> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const plugin = (yield* Plugin.Service) as Plugin.Interface
      return (yield* plugin.trigger(
        "experimental.chat.system.transform",
        input as never,
        output as never,
      )) as SystemTransformOutput
    }).pipe(Effect.provide(built.layer)),
  )

describe("worker system.transform hub proxy (sys-transform-ws3)", () => {
  test("a hub-proxy client's systemTransform mutation reaches Plugin.trigger", async () => {
    const calls: Array<{ method: string; args: readonly unknown[] }> = []
    // A fake hub-proxy client: the hub runs the loaded plugins and returns the
    // mutated system array. All authorities multiplex one port, so the worker
    // hands this single client to the plugin host.
    const client: HubProxyClient = {
      call: async (method, args) => {
        calls.push({ method, args })
        const [, output] = args as [SystemTransformInput, SystemTransformOutput]
        return { system: [...output.system, "hub-added"] }
      },
      dispose: () => {},
    }
    const systemTransform = hubProxySystemTransform(client)
    expect(systemTransform).toBeDefined()

    const host = createWorkerPluginHost([], { systemTransform: systemTransform! })
    const result = await triggerSystemTransform(
      buildSessionWorkerLayer(options({ plugins: host })),
      { sessionID: "ses_1", model: { providerID: "mock", modelID: "m" } },
      { system: ["base"] },
    )

    expect(result.system).toEqual(["base", "hub-added"])
    // The worker sent the exact frozen proxy shape: method + [input, output].
    expect(calls).toEqual([
      {
        method: "systemTransform",
        args: [{ sessionID: "ses_1", model: { providerID: "mock", modelID: "m" } }, { system: ["base"] }],
      },
    ])
  })

  test("the bridge is undefined without a hub client (degraded boot)", () => {
    expect(hubProxySystemTransform(undefined)).toBeUndefined()
  })

  test("without a hub client the system array passes through unchanged", async () => {
    // Degraded boot: no hub-proxy client, so `session-worker.ts` omits the
    // transform and the host's system array passes through by identity.
    const systemTransform = hubProxySystemTransform(undefined)
    const host = createWorkerPluginHost([], systemTransform === undefined ? undefined : { systemTransform })
    const output: SystemTransformOutput = { system: ["base"] }

    const result = await triggerSystemTransform(
      buildSessionWorkerLayer(options({ plugins: host })),
      { sessionID: "ses_1", model: {} },
      output,
    )

    expect(result.system).toEqual(["base"])
    expect(result).toBe(output)
  })
})
