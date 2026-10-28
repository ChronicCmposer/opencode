// A hub-executed MCP (and LSP) tool must be abortable.
//
// The worker owns no MCP/LSP client, so the hub runs the tool over the
// plugin-tool channel. Before the fix the hub created a throwaway
// `AbortController` that was never registered, so the hub cancel path (or the
// worker's `pluginTool.abort` RPC) could not unwind an in-flight MCP call: the
// session waited for the provider timeout. This test drives the authority with a
// tool whose MCP call settles only on its `abortSignal` and proves the shared
// registry reaches it.

import { describe, expect, test } from "bun:test"
import { Effect, Option } from "effect"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Truncate } from "@/tool/truncate"
import { Agent } from "@/agent/agent"
import { Session } from "@/session/session"
import {
  createPluginToolAbortRegistry,
  pluginToolAuthority,
} from "@/session/worker/hub"
import { pollUntil } from "../lib/poll"

const SESSION = "ses_mcp_abort"
const CALL = "call_mcp_abort"

interface FakeClient {
  callTool: (
    params: unknown,
    schema: unknown,
    options?: { readonly signal?: AbortSignal },
  ) => Promise<unknown>
}

function authorityWith(
  registry: ReturnType<typeof createPluginToolAbortRegistry>,
  client: FakeClient,
  onRegister?: () => void,
) {
  const mcp = {
    tools: () =>
      Effect.succeed({
        "fake.tool": {
          def: {
            name: "fake.tool",
            description: "waits for its abort signal",
            inputSchema: { type: "object", properties: {} },
          },
          client,
        },
      }),
  }
  const permission = {
    ask: () => Effect.succeed(undefined as never),
    reply: () => Effect.succeed(undefined as never),
    list: () => Effect.succeed([]),
  }
  const truncate = {
    output: (text: string) => Effect.succeed({ content: text, truncated: false }),
    write: (text: string) => Effect.succeed(text),
  }
  const agent = { get: () => Effect.succeed({ permission: [] } as never) }
  const sessions = { get: () => Effect.succeed({ permission: [] } as never) }
  const run = <A, E>(effect: Effect.Effect<A, E>) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provideService(MCP.Service, mcp as never),
        Effect.provideService(Permission.Service, permission as never),
        Effect.provideService(Truncate.Service, truncate as never),
        Effect.provideService(Agent.Service, agent as never),
        Effect.provideService(Session.Service, sessions as never),
      ),
    )
  return pluginToolAuthority({
    plugin: () => Option.none(),
    run,
    registerAbort: (sessionID, callID) => {
      onRegister?.()
      return registry.registerAbort(sessionID, callID)
    },
  })
}

/** An MCP client whose call settles only when its abort signal fires. */
function abortableClient(report: { aborted: boolean }): FakeClient {
  return {
    callTool: (_params, _schema, options) =>
      new Promise((_resolve, reject) => {
        const signal = options?.signal
        const fail = () => {
          report.aborted = true
          reject(new Error("mcp call aborted"))
        }
        if (signal?.aborted === true) return fail()
        signal?.addEventListener("abort", fail, { once: true })
      }),
  }
}

describe("hub-executed MCP tools are abortable", () => {
  test("the per-call abort unwinds an in-flight MCP call", async () => {
    const registry = createPluginToolAbortRegistry()
    const report = { aborted: false }
    let registered = false
    const authority = authorityWith(registry, abortableClient(report), () => {
      registered = true
    })

    const call = authority({
      sessionID: SESSION,
      messageID: "msg_1",
      callID: CALL,
      toolID: "fake.tool",
      args: {},
      agent: "build",
    })

    await pollUntil(() => registered, { message: "MCP tool never registered its abort handle" })
    registry.abort(SESSION, CALL)
    await expect(call).rejects.toThrow(/aborted/)
    expect(report.aborted).toBe(true)
  })

  test("the cancel-cascade abortSession unwinds every call of the session", async () => {
    const registry = createPluginToolAbortRegistry()
    const report = { aborted: false }
    let registered = false
    const authority = authorityWith(registry, abortableClient(report), () => {
      registered = true
    })

    const call = authority({
      sessionID: SESSION,
      messageID: "msg_2",
      callID: CALL,
      toolID: "fake.tool",
      args: {},
      agent: "build",
    })

    await pollUntil(() => registered, { message: "MCP tool never registered its abort handle" })
    registry.abortSession(SESSION)
    await expect(call).rejects.toThrow(/aborted/)
    expect(report.aborted).toBe(true)
  })
})
