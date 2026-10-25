// F5 — `sanitizePluginToolResult`, the fail-loud clone probe on the plugin-tool
// result before it crosses the control port.
//
// A plugin tool's `metadata`/`attachments` are arbitrary data; a function (or any
// non-structured-cloneable value) leaked into the result must fail with a
// descriptive `PluginToolNotSerializableError` naming the tool and the offending
// field — never the generic `serveHubProxy` clone-guard message. This suite pins:
//   * a non-cloneable field throws `PluginToolNotSerializableError` (tag surfaces);
//   * every result field is probed (metadata, attachments, output, title);
//   * a clone-safe result passes through unchanged;
//   * `pluginToolAuthority` runs the probe before returning (execute path).

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option } from "effect"
import { pluginToolAuthority, sanitizePluginToolResult, type HubProxyRunner } from "@/session/worker/hub"
import { Plugin } from "@/plugin"
import { Permission } from "@/permission"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import { Agent } from "@/agent/agent"
import { PluginToolNotSerializableError } from "@/session/worker/plugin-tool-wire"
import type { PluginToolExecuteInput } from "@/session/worker/plugin-tool-wire"

describe("sanitizePluginToolResult", () => {
  test("a clone-safe result passes through unchanged", () => {
    const result = { title: "t", output: "o", metadata: { kept: [1, 2] }, attachments: [{ type: "file" }] }
    expect(sanitizePluginToolResult("compress", result)).toBe(result)
  })

  test("a non-cloneable metadata field fails loud and names the tool + field", () => {
    const result = { title: "t", output: "o", metadata: { cb: () => {} } }
    let caught: unknown
    try {
      sanitizePluginToolResult("compress", result)
    } catch (error) {
      caught = error
    }
    expect(caught).toBeInstanceOf(PluginToolNotSerializableError)
    const error = caught as PluginToolNotSerializableError
    // The tag is what the wire carries to the worker for `instanceof` rebuilding.
    expect(error._tag).toBe("PluginToolNotSerializableError")
    expect(error.toolID).toBe("compress")
    expect(error.detail).toBe("metadata")
    expect(error.message).toContain("compress")
    expect(error.message).toContain("metadata")
  })

  test("a non-cloneable attachments field fails loud", () => {
    const result = { title: "t", output: "o", metadata: {}, attachments: { handler: () => {} } }
    expect(() => sanitizePluginToolResult("compress", result)).toThrow(PluginToolNotSerializableError)
  })
})

// ---------------------------------------------------------------------------
// Authority integration: the probe runs on the real execute path.
// ---------------------------------------------------------------------------

const services = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Session.Info) }),
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
  sessionID: "ses_serialize",
  messageID: "msg_serialize",
  callID: "call_serialize",
  agent: "build",
  args: {},
}

describe("pluginToolAuthority result serialization", () => {
  test("a plugin tool leaking a function into metadata rejects with the tagged error", async () => {
    const compress = {
      description: "leaky",
      args: {},
      execute: async () => ({ title: "t", output: "o", metadata: { cb: () => {} } }),
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
    })

    let caught: unknown
    try {
      await authority(input)
    } catch (error) {
      caught = error
    }
    expect(caught).toBeInstanceOf(PluginToolNotSerializableError)
    expect((caught as PluginToolNotSerializableError).toolID).toBe("compress")
    expect((caught as PluginToolNotSerializableError)._tag).toBe("PluginToolNotSerializableError")
  })

  test("a clone-safe plugin tool resolves normally", async () => {
    const compress = {
      description: "clean",
      args: {},
      execute: async () => ({ title: "t", output: "o", metadata: { ok: true } }),
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
    })

    const result = await authority(input)
    expect(result.output).toBe("o")
    expect(result.metadata).toEqual({ ok: true, truncated: false })
  })
})
