// The hub-proxy method lookup guard (review finding m1).
//
// `serveHubProxy` dispatches on a plain object literal. A bare `handlers[method]`
// lookup walks the prototype chain, so a frame naming an inherited
// `Object.prototype` member (`toString`, `valueOf`, ...) resolves to a function
// instead of `undefined`. The `handler === undefined` guard misses, the inherited
// function runs, and the hub replies with a spurious success value — the caller
// never sees the `unknown hub-proxy method` error it deserves.
//
// This suite pins the own-property contract:
//   * an inherited name (`toString`, `valueOf`) rejected exactly like any other
//     unknown method, with the descriptive `unknown hub-proxy method` message;
//   * a genuinely served method is unaffected.

import { describe, expect, test } from "bun:test"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import type { MessagesTransformInput, MessagesTransformOutput } from "@/session/worker/plugin-host"

const instantRunner = (effect: unknown): Promise<never> => Promise.resolve(effect as never)

/** The authority map `buildHubProxyHandlers` accepts, with no transform authority wired. */
function authorities(): HubProxyAuthorities {
  return {
    sessionStatus: {} as never,
    permission: {} as never,
    question: {} as never,
  }
}

/** Serve `handlers` on a fresh channel and return the worker-end client. */
function clientFor(handlers: Parameters<typeof serveHubProxy>[0]): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1)
}

/** Call an inherited `Object.prototype` name and return the rejection it produced. */
async function rejectionFor(client: ReturnType<typeof createHubProxyClient>, method: string): Promise<unknown> {
  try {
    await client.call(method as never, [])
  } catch (error) {
    return error
  }
  throw new Error(`expected "${method}" to reject`)
}

describe("hub-proxy method lookup", () => {
  test('an inherited name ("toString") is rejected as an unknown method', async () => {
    const client = clientFor(buildHubProxyHandlers(authorities(), instantRunner))
    try {
      const caught = await rejectionFor(client, "toString")
      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      expect((caught as Error).message).toContain('unknown hub-proxy method "toString"')
    } finally {
      client.dispose()
    }
  })

  test('an inherited name ("valueOf") is rejected as an unknown method', async () => {
    const client = clientFor(buildHubProxyHandlers(authorities(), instantRunner))
    try {
      const caught = await rejectionFor(client, "valueOf")
      expect(caught).toBeInstanceOf(HubProxyRemoteError)
      expect((caught as Error).message).toContain('unknown hub-proxy method "valueOf"')
    } finally {
      client.dispose()
    }
  })

  test("a genuinely served method still reaches its handler", async () => {
    const client = clientFor(buildHubProxyHandlers(authorities(), instantRunner))
    try {
      const input: MessagesTransformInput = { sessionID: "ses_lookup" }
      const output: MessagesTransformOutput = { messages: ["base"] }
      const returned = (await client.call("messagesTransform", [input, output])) as MessagesTransformOutput
      expect(returned).toEqual({ messages: ["base"] })
    } finally {
      client.dispose()
    }
  })
})
