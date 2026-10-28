// The untrusted control boundary in front of the hub-proxy port.
//
// `encodeWorkerFrame`'s fast path is a plain `JSON.stringify` because it runs on
// the egress throughput hot path; `JSON.stringify` silently drops functions and
// symbols and throws on bigints/cycles. The worker→hub request direction carries
// untrusted plugin/model values, so the hub-proxy client must reject anything
// JSON cannot represent BEFORE a frame posts — loudly, with the frame codec's
// typed `ProtocolError`, not by sending a truncated payload.
//
// This suite pins that boundary directly: function, symbol, BigInt and circular
// payloads reject with `tag: "PROTOCOL"`, the hub handler is never invoked, and
// ordinary JSON still round-trips through the same channel.

import { describe, expect, test } from "bun:test"
import { createHubProxyClient, HubProxyRemoteError, serveHubProxy } from "@/session/worker/hub-proxy-wire"

const SESSION_ID = "ses_boundary"

/** A client wired to a handler that echoes its second argument and counts calls. */
function boundaryClient(): {
  readonly call: ReturnType<typeof createHubProxyClient>["call"]
  readonly calls: () => number
  readonly dispose: () => void
} {
  let calls = 0
  const channel = new MessageChannel()
  const stopServing = serveHubProxy(
    {
      messagesTransform: (_input, output) => {
        calls += 1
        return output
      },
    },
    channel.port2,
  )
  const client = createHubProxyClient(channel.port1)
  return {
    call: client.call,
    calls: () => calls,
    dispose: () => {
      stopServing()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** A `messagesTransform` call whose message content is the value under test. */
const callWith = (client: ReturnType<typeof boundaryClient>, content: unknown): Promise<unknown> =>
  client.call("messagesTransform", [
    { sessionID: SESSION_ID },
    { messages: [{ role: "user", content }] },
  ])

async function rejectionOf(client: ReturnType<typeof boundaryClient>, content: unknown): Promise<unknown> {
  try {
    await callWith(client, content)
  } catch (error) {
    return error
  }
  throw new Error("expected the control boundary to reject a non-JSON payload")
}

describe("hub-proxy control boundary rejects values JSON would drop", () => {
  test("a function payload fails loud before posting", async () => {
    const client = boundaryClient()
    try {
      const caught = await rejectionOf(client, () => "not json")
      expect((caught as { tag?: string }).tag).toBe("PROTOCOL")
      expect((caught as Error).message).toMatch(/not JSON-serialisable/)
      expect((caught as Error).message).toMatch(/function/)
      expect(caught).not.toBeInstanceOf(HubProxyRemoteError)
      expect(client.calls()).toBe(0)
    } finally {
      client.dispose()
    }
  })

  test("a symbol payload fails loud before posting", async () => {
    const client = boundaryClient()
    try {
      const caught = await rejectionOf(client, Symbol("not-json"))
      expect((caught as { tag?: string }).tag).toBe("PROTOCOL")
      expect((caught as Error).message).toMatch(/symbol/)
      expect(client.calls()).toBe(0)
    } finally {
      client.dispose()
    }
  })

  test("a BigInt payload fails loud before posting", async () => {
    const client = boundaryClient()
    try {
      const caught = await rejectionOf(client, BigInt(1))
      expect((caught as { tag?: string }).tag).toBe("PROTOCOL")
      expect((caught as Error).message).toMatch(/not JSON-serialisable/)
      expect(client.calls()).toBe(0)
    } finally {
      client.dispose()
    }
  })

  test("a circular payload fails loud before posting", async () => {
    const client = boundaryClient()
    try {
      const circular: Record<string, unknown> = {}
      circular.self = circular
      const caught = await rejectionOf(client, circular)
      expect((caught as { tag?: string }).tag).toBe("PROTOCOL")
      expect((caught as Error).message).toMatch(/not JSON-serialisable/)
      expect(client.calls()).toBe(0)
    } finally {
      client.dispose()
    }
  })

  test("ordinary JSON still posts, and a rejected payload does not poison the channel", async () => {
    const client = boundaryClient()
    try {
      const rejected = await rejectionOf(client, () => "not json")
      expect((rejected as { tag?: string }).tag).toBe("PROTOCOL")

      const returned = (await callWith(client, "hello")) as {
        readonly messages: ReadonlyArray<{ readonly role: unknown; readonly content: unknown }>
      }
      expect(returned.messages).toEqual([{ role: "user", content: "hello" }])
      expect(client.calls()).toBe(1)
    } finally {
      client.dispose()
    }
  })
})
