// Hub-proxy control wire — client correlation contract.
//
// `createHubProxyClient` is a thin request/reply client over the multiplexed
// hub-proxy `MessagePort`. These tests pin the three behaviours the shared
// `createRequestChannel` provides: out-of-order replies correlate by id,
// `dispose` rejects every in-flight request, and an optional timeout rejects an
// unanswered request.

import { describe, expect, test } from "bun:test"
import {
  HubProxyRemoteError,
  createHubProxyClient,
  serveHubProxy,
  type HubProxyHandlers,
} from "@/session/worker/hub-proxy-wire"

/** Serve `handlers` on a fresh channel and return the client end plus a disposer. */
function pair(handlers: HubProxyHandlers): { client: ReturnType<typeof createHubProxyClient>; dispose: () => void } {
  const channel = new MessageChannel()
  const dispose = serveHubProxy(handlers, channel.port2)
  return { client: createHubProxyClient(channel.port1), dispose }
}

describe("createHubProxyClient correlation", () => {
  test("correlates out-of-order replies by request id", async () => {
    let resolveFirst!: (value: unknown) => void
    const firstGate = new Promise((resolve) => {
      resolveFirst = resolve
    })
    const { client, dispose } = pair({
      "sessionStatus.get": async (sessionID: unknown) => {
        await firstGate
        return `first:${String(sessionID)}`
      },
      "sessionStatus.list": () => "second",
    })

    const first = client.call("sessionStatus.get", ["a"])
    const second = client.call("sessionStatus.list", [])

    // The second request is answered while the first is still in flight.
    expect(await second).toBe("second")
    resolveFirst(undefined)
    expect(await first).toBe("first:a")

    client.dispose()
    dispose()
  })

  test("dispose rejects every in-flight request and later calls", async () => {
    const noReply: HubProxyHandlers = {}
    const { client, dispose } = pair(noReply)

    const pending = client.call("sessionStatus.get", ["a"])
    client.dispose()

    const error = await pending.catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(HubProxyRemoteError)
    expect(String(error)).toContain("disposed")

    await expect(client.call("sessionStatus.list", [])).rejects.toBeInstanceOf(HubProxyRemoteError)
    dispose()
  })

  test("an unanswered request rejects once a timeout is configured", async () => {
    const channel = new MessageChannel()
    // No responder on port2, so nothing ever replies.
    const client = createHubProxyClient(channel.port1, { timeoutMs: 30 })

    const error = await client.call("sessionStatus.get", ["a"]).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(HubProxyRemoteError)
    expect(String(error)).toMatch(/timed out/)

    client.dispose()
  })

  test("a remote error reply rejects with its tag and data preserved", async () => {
    const { client, dispose } = pair({
      "sessionStatus.get": () => {
        throw Object.assign(new Error("nope"), { _tag: "SessionNotFound", sessionID: "ses_x" })
      },
    })

    const error = await client.call("sessionStatus.get", ["a"]).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(HubProxyRemoteError)
    expect((error as HubProxyRemoteError).tag).toBe("SessionNotFound")
    expect((error as HubProxyRemoteError).data).toMatchObject({ sessionID: "ses_x" })

    client.dispose()
    dispose()
  })
})

describe("hub-proxy wire encoding", () => {
  test("a request crosses the port as one JSON string envelope", async () => {
    const posted: unknown[] = []
    const port = {
      postMessage: (message: unknown) => {
        posted.push(message)
      },
      addEventListener: () => {},
      removeEventListener: () => {},
      start: () => {},
    } as unknown as MessagePort

    const client = createHubProxyClient(port)
    const pending = client.call("sessionStatus.list", [])
    try {
      expect(posted).toHaveLength(1)
      expect(typeof posted[0]).toBe("string")
      expect(JSON.parse(posted[0] as string)).toMatchObject({
        v: 1,
        port: "hub-proxy",
        kind: "request",
        payload: { kind: "request", method: "sessionStatus.list", args: [] },
      })
    } finally {
      client.dispose()
      await pending.catch(() => {})
    }
  })
})
