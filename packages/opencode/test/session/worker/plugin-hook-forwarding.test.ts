// The four hooks the worker triggers outside the chat path (slash commands,
// compaction, text completion) are forwarded to the hub, where the plugin runs,
// and the hub's result is applied back to the caller.

import { describe, expect, test } from "bun:test"
import { Effect, Option } from "effect"
import { autocontinueWireInput, createWorkerPluginHost } from "@/session/worker/plugin-host"
import { pluginTriggerAuthority } from "@/session/worker/hub-proxy-authorities"
import type { Plugin } from "@/plugin"
import type { Provider } from "@/provider/provider"

const plugins = [
  {
    name: "fixture",
    hooks: {},
    proxied: [
      "command.execute.before",
      "experimental.session.compacting",
      "experimental.compaction.autocontinue",
      "experimental.text.complete",
    ],
  },
]

describe("worker plugin hooks forwarded to the hub", () => {
  test("text.complete returns the hub's text", async () => {
    const host = createWorkerPluginHost(plugins, {
      hubHook: async (_name, _input, output) => ({ text: `${(output as { text: string }).text}!` }),
    })
    const result = await host.textComplete({ sessionID: "s" }, { text: "done" })
    expect(result.text).toBe("done!")
  })

  test("command.execute.before splices the hub's parts into the caller's array", async () => {
    const host = createWorkerPluginHost(plugins, {
      hubHook: async () => ({ parts: [{ type: "text", text: "injected" }] }),
    })
    const parts: unknown[] = [{ type: "text", text: "original" }]
    await host.commandBefore({ command: "review" }, { parts })
    expect(parts).toEqual([{ type: "text", text: "injected" }])
  })

  test("session.compacting applies the hub's context and prompt", async () => {
    const host = createWorkerPluginHost(plugins, {
      hubHook: async () => ({ context: ["keep this"], prompt: "summarize" }),
    })
    const output = { context: [] as string[], prompt: undefined as string | undefined }
    await host.sessionCompacting({ sessionID: "s" }, output)
    expect(output).toEqual({ context: ["keep this"], prompt: "summarize" })
  })

  test("compaction.autocontinue sends only the model identity across the isolate", async () => {
    const sent: unknown[] = []
    const host = createWorkerPluginHost(plugins, {
      hubHook: async (_name, input) => {
        sent.push(input)
        return { enabled: false }
      },
    })
    const input = {
      sessionID: "s",
      agent: "build",
      model: { id: "m1", providerID: "p1" },
      provider: { source: "config", info: {}, options: { fetch: () => {} } },
      message: { id: "msg" },
      overflow: true,
    }
    const result = await host.compactionAutocontinue(input, { enabled: true })
    expect(result.enabled).toBe(false)
    expect(sent).toEqual([
      { sessionID: "s", agent: "build", message: { id: "msg" }, overflow: true, modelRef: { providerID: "p1", modelID: "m1" } },
    ])
    expect(JSON.stringify(sent[0])).not.toContain("fetch")
  })

  test("the hub rebuilds model and provider before running autocontinue", async () => {
    const seen: unknown[] = []
    const plugin = {
      trigger: (_name: string, input: unknown, output: unknown) =>
        Effect.sync(() => {
          seen.push(input)
          return output
        }),
    } as unknown as Plugin.Interface
    const info = { id: "p1", source: "config", options: { baseURL: "http://x" } }
    const model = { id: "m1", providerID: "p1" }
    const provider = {
      getProvider: () => Effect.succeed(info),
      getModel: () => Effect.succeed(model),
    } as unknown as Provider.Interface
    const authority = pluginTriggerAuthority({
      plugin: () => Option.some(plugin),
      provider: () => Option.some(provider),
      run: (effect) => Effect.runPromise(effect),
    })
    await authority(
      "experimental.compaction.autocontinue",
      autocontinueWireInput({ sessionID: "s", model, provider: {}, overflow: false }),
      { enabled: true },
    )
    expect(seen).toEqual([
      { sessionID: "s", overflow: false, model, provider: { source: "config", info, options: info.options } },
    ])
  })
})
