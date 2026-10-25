// PT-5 — the clone guard on the `pluginTool.execute` reply path.
//
// `pluginToolAuthority` runs a plugin tool on the hub and returns a clone-safe
// `{ title, output, metadata, attachments }` that crosses the worker control
// port. A plugin is arbitrary user code: it can leak a function (or any other
// non-structured-cloneable value) into `result.metadata`, which the authority
// passes through after `Truncate.boundResult`. `serveHubProxy` catches the
// synchronous `DataCloneError` from `port.postMessage` and posts ONE clone-safe
// `ok:false` frame echoing the request id, so the worker fails LOUD with a
// descriptive `HubProxyRemoteError` ("not structured-cloneable") instead of
// hanging or timing out.
//
// This suite pins that contract for the `pluginTool.execute` method specifically
// (the generic guard is exercised in `hub-proxy-clone-failure.test.ts`), and
// proves the multiplexed control port stays usable for the next call.

import { describe, expect, test } from "bun:test"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import type { PluginToolExecuteInput, PluginToolExecuteResult } from "@/session/worker/plugin-tool-wire"
import { instantRunner, recordingHubAuthorities } from "../../lib/hub-proxy"

/** A representative `pluginTool.execute` request frame. */
const input: PluginToolExecuteInput = {
  toolID: "compress",
  sessionID: "ses_clone_guard",
  messageID: "msg_clone_guard",
  callID: "call_clone_guard",
  agent: "build",
  args: {},
}

/**
 * The full authority map with only `pluginTool` overridden to `execute`. The
 * other authorities come from the shared recording fixture so the handler map is
 * complete without ad-hoc empty casts.
 */
function authorities(
  execute: (request: PluginToolExecuteInput) => Promise<PluginToolExecuteResult>,
): HubProxyAuthorities {
  return { ...recordingHubAuthorities().authorities, pluginTool: execute }
}

/** Serve `handlers` on a fresh channel and return the worker-end client. */
function clientFor(
  handlers: Parameters<typeof serveHubProxy>[0],
  timeoutMs?: number,
): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1, timeoutMs === undefined ? {} : { timeoutMs })
}

/**
 * Resolve when `signal` fires, or reject after `timeoutMs`. The timeout is a
 * backstop against a hung suite — never the synchronization primitive itself.
 */
function awaitSignal(signal: Promise<void>, timeoutMs: number, description: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`timed out after ${timeoutMs}ms waiting for ${description}`))
    }, timeoutMs)
    void signal.then(() => {
      clearTimeout(timer)
      resolve()
    })
  })
}

describe("pluginTool.execute reply clone guard", () => {
  test("a non-cloneable success reply fails loud (descriptive error, not a timeout) and the port stays usable", async () => {
    let calls = 0
    const client = clientFor(
      buildHubProxyHandlers(
        authorities(async () => {
          calls += 1
          // First call leaks a function into `metadata`; the second is clone-safe.
          return calls === 1
            ? { title: "compress", output: "HUB_RAN", metadata: { fn: () => {} } }
            : { title: "compress", output: "HUB_RAN", metadata: { ok: true } }
        }),
        instantRunner,
      ),
      // A generous backstop: a silent drop (the pre-fix behaviour) is caught by
      // the message assertion, not by racing a short timeout.
      2000,
    )

    try {
      let caught: unknown
      try {
        await client.call("pluginTool.execute", [input])
      } catch (error) {
        caught = error
      }

      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      const message = (caught as Error).message
      // The real cause is named, not masked as a timeout.
      expect(message).toContain("not structured-cloneable")
      expect(message).not.toContain("timed out")
      // The authority ran (it was the reply, not the request, that failed).
      expect(calls).toBe(1)

      // Fail loud must not poison the multiplexed control port.
      const ok = (await client.call("pluginTool.execute", [input])) as PluginToolExecuteResult
      expect(ok).toEqual({ title: "compress", output: "HUB_RAN", metadata: { ok: true } })
      expect(calls).toBe(2)
    } finally {
      client.dispose()
    }
  })

  test("recovery is exactly one clone-safe ok:false frame echoing the request id", async () => {
    const channel = new MessageChannel()
    // Intercept the reply path: the first post (the un-cloneable success value)
    // throws as a real port would; the fallback is captured rather than sent.
    const posted: Array<Record<string, unknown>> = []
    let replyAttempts = 0
    let signalFallbackPosted: (() => void) | undefined
    const fallbackPosted = new Promise<void>((resolve) => {
      signalFallbackPosted = resolve
    })
    channel.port2.postMessage = ((frame: Record<string, unknown>) => {
      replyAttempts += 1
      if (replyAttempts === 1) throw new DOMException("The object can not be cloned.", "DataCloneError")
      posted.push(frame)
      signalFallbackPosted?.()
    }) as never

    serveHubProxy(
      buildHubProxyHandlers(
        authorities(async () => ({ title: "compress", output: "HUB_RAN", metadata: { leak: () => {} } })),
        instantRunner,
      ),
      channel.port2,
    )
    const client = createHubProxyClient(channel.port1, { timeoutMs: 2000 })

    try {
      void client.call("pluginTool.execute", [input]).catch(() => {})
      await awaitSignal(fallbackPosted, 2000, "the clone-safe fallback post")

      // Exactly one success post (threw) plus exactly one fallback post (kept).
      expect(replyAttempts).toBe(2)

      const envelope = posted[0]
      expect(envelope?.kind).toBe("reply")
      expect(envelope?.id).toBe(1)
      const payload = envelope?.payload as Record<string, unknown>
      expect(payload.ok).toBe(false)
      expect(payload.id).toBe(1)
      expect(String(payload.message)).toContain("not structured-cloneable")
      expect(String(payload.message)).toContain("id 1")
    } finally {
      client.dispose()
    }
  })
})
