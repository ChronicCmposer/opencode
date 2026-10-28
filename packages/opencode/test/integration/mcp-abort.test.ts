// A hub-executed MCP tool is abortable ACROSS the port.
//
// The worker owns no MCP client, so its `pluginTool.abort` RPC must reach the
// hub's per-call abort registry and unwind the real MCP call's `AbortSignal`.
// This drives the genuine boundary (`serveHubProxy` + `createHubProxyClient`);
// the hub-internal registry behavior is additionally pinned by hub-core's
// `hub-mcp-abort.test.ts`.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option } from "effect"
import { Agent } from "@/agent/agent"
import { InstanceRef } from "@/effect/instance-ref"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Session } from "@/session/session"
import { Truncate } from "@/tool/truncate"
import {
  buildHubProxyHandlers,
  buildHubProxyStreamHandlers,
  createPluginToolAbortRegistry,
  pluginToolAuthority,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { recordingHubAuthorities } from "../lib/hub-proxy"
import { pollUntil } from "../lib/poll"

const HUB_DIRECTORY = "/hub/mcp-abort"
const SESSION = "ses_mcp_abort"
const TOOL_ID = "fake.mcp"

const hubInstance = {
  directory: HUB_DIRECTORY,
  worktree: HUB_DIRECTORY,
  project: { id: "prj_mcp_abort", worktree: HUB_DIRECTORY, vcs: "git" },
} as never

const hubServices = Layer.mergeAll(
  Layer.mock(Permission.Service, { ask: () => Effect.void }),
  Layer.mock(Truncate.Service, {
    output: (text: string) => Effect.succeed({ content: text, truncated: false as const }),
    write: (text: string) => Effect.succeed(text),
  }),
  Layer.mock(Agent.Service, { get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info) }),
  Layer.mock(Session.Service, {
    get: () => Effect.succeed({ permission: [] } as unknown as Session.Info),
  }),
)

interface McpState {
  aborted: boolean
  readonly started: Promise<void>
  readonly notifyStarted: () => void
}

/** An MCP service whose `callTool` hangs until its abort signal fires. */
function mcpService(state: McpState) {
  return {
    tools: () =>
      Effect.succeed({
        [TOOL_ID]: {
          def: {
            name: TOOL_ID,
            description: "a long-running hub MCP tool",
            inputSchema: { type: "object", properties: {} },
          },
          client: {
            callTool: (params: unknown, _schema: unknown, options?: { readonly signal?: AbortSignal }) => {
              if ((params as { mode?: string }).mode === "quick") {
                return Promise.resolve({ content: [{ type: "text", text: "MCP_QUICK" }] })
              }
              return new Promise((_resolve, reject) => {
                const signal = options?.signal
                const fail = () => {
                  state.aborted = true
                  reject(new Error("MCP_ABORTED"))
                }
                if (signal?.aborted === true) return fail()
                signal?.addEventListener("abort", fail, { once: true })
                state.notifyStarted()
              })
            },
          },
        },
      }),
  }
}

/** Stand up the real hub-proxy boundary for the MCP authority. */
function boundary(state: McpState) {
  const aborts = createPluginToolAbortRegistry()
  const service = mcpService(state)
  const runWithMcp: HubProxyRunner = (effect) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(hubServices),
        Effect.provideService(MCP.Service, service as never),
        Effect.provideService(InstanceRef, hubInstance),
      ),
    )
  const authority = pluginToolAuthority({
    plugin: () => Option.none(),
    run: runWithMcp,
    registerAbort: (sessionID, callID) => aborts.registerAbort(sessionID, callID),
  })
  const { authorities } = recordingHubAuthorities({ pluginTool: authority })
  const all = {
    ...authorities,
    pluginToolAbort: (input: { readonly sessionID: string; readonly callID: string }) =>
      aborts.abort(input.sessionID, input.callID),
  }
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(
    buildHubProxyHandlers(all, runWithMcp),
    channel.port2,
    buildHubProxyStreamHandlers(all),
  )
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

const executeInput = (callID: string, mode: string) => ({
  sessionID: SESSION,
  messageID: "msg_mcp_abort",
  callID,
  toolID: TOOL_ID,
  args: { mode },
  agent: "build",
})

describe("hub MCP tool abort crosses the hub-proxy port", () => {
  test("worker abort unwinds the MCP call; the port still serves afterwards", async () => {
    let notifyStarted: () => void = () => {}
    const started = new Promise<void>((resolve) => (notifyStarted = resolve))
    const state: McpState = { aborted: false, started, notifyStarted }
    const hub = boundary(state)

    try {
      const pending = hub.client.call("pluginTool.execute", [executeInput("call_mcp_abort", "hang")])
      await pollUntil(() => state.started.then(() => true), {
        message: "the hub MCP call never started",
        timeoutMs: 5_000,
      })

      await hub.client.call("pluginTool.abort", [{ sessionID: SESSION, callID: "call_mcp_abort" }])

      const outcome = await Promise.race([
        pending.then(
          (value) => ({ ok: true as const, value }),
          (error: unknown) => ({ ok: false as const, error }),
        ),
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error("MCP call was not aborted within 5s")), 5_000),
        ),
      ])
      expect(outcome.ok).toBe(false)
      if (!outcome.ok) expect(String(outcome.error)).toContain("MCP_ABORTED")
      expect(state.aborted).toBe(true)
    } finally {
      hub.dispose()
    }
  }, 20_000)
})
