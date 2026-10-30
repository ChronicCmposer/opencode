// F6 — hub-side argument validation before `definition.execute`.
//
// The worker ships args verbatim (`Schema.Unknown`), so the hub must validate them
// against the tool's own decoder before running it — the same Zod decoder the
// registry's `fromPlugin` uses (and the descriptor's JSON Schema is generated
// from). This suite pins:
//   * an invalid arg rejects fail-loud with a descriptive error naming the tool;
//   * valid args run the tool;
//   * a legacy JSON-schema tool (no local Zod decoder) passes args through.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option } from "effect"
import { z } from "zod"
import { pluginToolAuthority, InvalidPluginToolArgsError, type HubProxyRunner } from "@/session/worker/hub"
import { Plugin } from "@/plugin"
import { Permission } from "@/permission"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import { Agent } from "@/agent/agent"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"

const services = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { messages: () => Effect.succeed([]), get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
)

const run: HubProxyRunner = (effect) => Effect.runPromise(Effect.provide(effect, services))

function fakePlugin(hooks: readonly { readonly tool?: Record<string, unknown> }[]): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed(hooks as never),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

const input: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_validate",
  messageID: "msg_validate",
  callID: "call_validate",
  agent: "build",
  args: {},
}

describe("pluginToolAuthority argument validation", () => {
  test("invalid args reject fail-loud, before execute runs", async () => {
    let executed = false
    const compress = {
      description: "compress a range",
      args: { range: z.string() },
      execute: async () => {
        executed = true
        return { title: "t", output: "o", metadata: {} }
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
    })

    let caught: unknown
    try {
      await authority({ ...input, args: { range: 123 } })
    } catch (error) {
      caught = error
    }

    expect(caught).toBeInstanceOf(InvalidPluginToolArgsError)
    expect((caught as InvalidPluginToolArgsError)._tag).toBe("PluginTool.InvalidArgs")
    expect((caught as Error).message).toContain("compress")
    expect(executed).toBe(false)
  })

  test("valid args run the tool", async () => {
    let received: unknown
    const compress = {
      description: "compress a range",
      args: { range: z.string() },
      execute: async (args: unknown) => {
        received = args
        return { title: "t", output: "compressed", metadata: {} }
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
    })

    const result = await authority({ ...input, args: { range: "*" } })

    expect(result.output).toBe("compressed")
    expect(received).toEqual({ range: "*" })
  })

  test("a legacy JSON-schema tool (no Zod decoder) passes args through", async () => {
    let executed = false
    const legacy = {
      description: "legacy",
      // A plain JSON-schema map, not a Zod raw shape: the registry has no decoder
      // for it (`Schema.Unknown`), so the hub must not invent one either.
      args: { range: { type: "string" } },
      execute: async () => {
        executed = true
        return { title: "t", output: "o", metadata: {} }
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { legacy } }])),
      run,
    })

    const result = await authority({ ...input, toolID: "legacy", args: { whatever: true } })

    expect(executed).toBe(true)
    expect(result.output).toBe("o")
  })
})
