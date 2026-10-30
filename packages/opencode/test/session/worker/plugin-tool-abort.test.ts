// F1 — the hub-side in-flight abort registry for plugin tools.
//
// A plugin tool's `execute` runs on the hub under a per-call `AbortController`.
// The registry keys those controllers by `(sessionID, callID)` so the hub cancel
// path (and the worker's `pluginTool.abort` RPC) can unwind an in-flight call.
// This suite pins:
//   * `abort(sessionID, callID)` fires exactly that call's signal;
//   * `abortSession(sessionID)` fires every in-flight call of a session;
//   * `dispose` unregisters, so a settled call is never aborted;
//   * `pluginToolAuthority` disposes its registration on BOTH the success and
//     failure exits (and never registers when the tool is unknown).

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option } from "effect"
import {
  createPluginToolAbortRegistry,
  pluginToolAuthority,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { Plugin } from "@/plugin"
import { Permission } from "@/permission"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import { Agent } from "@/agent/agent"
import type { ToolContext as PluginToolContext } from "@opencode-ai/plugin"
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

type ToolHooks = { readonly tool?: Record<string, unknown> }

function fakePlugin(hooks: readonly ToolHooks[]): Plugin.Interface {
  return {
    trigger: () => Effect.succeed({} as never),
    list: () => Effect.succeed(hooks as never),
    init: () => Effect.void,
  } as unknown as Plugin.Interface
}

const input: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_abort",
  messageID: "msg_abort",
  callID: "call_abort",
  agent: "build",
  args: {},
}

describe("createPluginToolAbortRegistry", () => {
  test("abort fires the matching call's signal only", () => {
    const registry = createPluginToolAbortRegistry()
    const first = registry.registerAbort("ses_1", "call_1")
    const second = registry.registerAbort("ses_1", "call_2")

    registry.abort("ses_1", "call_1")

    expect(first.signal.aborted).toBe(true)
    expect(second.signal.aborted).toBe(false)
  })

  test("abortSession fires every in-flight call of that session, no other session", () => {
    const registry = createPluginToolAbortRegistry()
    const a = registry.registerAbort("ses_1", "call_1")
    const b = registry.registerAbort("ses_1", "call_2")
    const other = registry.registerAbort("ses_2", "call_3")

    registry.abortSession("ses_1")

    expect(a.signal.aborted).toBe(true)
    expect(b.signal.aborted).toBe(true)
    expect(other.signal.aborted).toBe(false)
  })

  test("dispose unregisters, so a settled call is never aborted", () => {
    const registry = createPluginToolAbortRegistry()
    const handle = registry.registerAbort("ses_1", "call_1")

    handle.dispose()
    registry.abortSession("ses_1")

    expect(handle.signal.aborted).toBe(false)
  })
})

describe("pluginToolAuthority abort lifecycle", () => {
  test("a deferred execute's ctx.abort fires when the session is aborted", async () => {
    const registry = createPluginToolAbortRegistry()
    let seenSignal: AbortSignal | undefined
    let started: () => void = () => {}
    const startedPromise = new Promise<void>((resolve) => (started = resolve))
    let release: () => void = () => {}
    const gate = new Promise<void>((resolve) => (release = resolve))

    const compress = {
      description: "deferred",
      args: {},
      execute: async (_args: unknown, ctx: PluginToolContext) => {
        seenSignal = ctx.abort
        started()
        await gate
        return { title: "t", output: "ok", metadata: {} }
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
      registerAbort: (sessionID, callID) => registry.registerAbort(sessionID, callID),
    })

    const pending = authority(input)
    await startedPromise
    expect(seenSignal?.aborted).toBe(false)

    registry.abortSession(input.sessionID)
    expect(seenSignal?.aborted).toBe(true)

    release()
    await pending
  })

  test("the registration is disposed on the success exit", async () => {
    let disposed = 0
    const controller = new AbortController()
    const compress = {
      description: "ok",
      args: {},
      execute: async () => ({ title: "t", output: "ok", metadata: {} }),
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
      registerAbort: () => ({ signal: controller.signal, dispose: () => void disposed++ }),
    })

    await authority(input)

    expect(disposed).toBe(1)
  })

  test("the registration is disposed on the failure exit", async () => {
    let disposed = 0
    const compress = {
      description: "boom",
      args: {},
      execute: async () => {
        throw new Error("execute blew up")
      },
    }
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress } }])),
      run,
      registerAbort: () => ({ signal: new AbortController().signal, dispose: () => void disposed++ }),
    })

    await expect(authority(input)).rejects.toThrow("execute blew up")

    expect(disposed).toBe(1)
  })

  test("an unknown tool never registers an abort", async () => {
    let registered = 0
    const authority = pluginToolAuthority({
      plugin: () => Option.some(fakePlugin([{ tool: { compress: { description: "d", args: {}, execute: async () => "" } } }])),
      run,
      registerAbort: () => {
        registered++
        return { signal: new AbortController().signal, dispose: () => {} }
      },
    })

    await expect(authority({ ...input, toolID: "nope" })).rejects.toThrow(/unknown plugin tool/)

    expect(registered).toBe(0)
  })
})
