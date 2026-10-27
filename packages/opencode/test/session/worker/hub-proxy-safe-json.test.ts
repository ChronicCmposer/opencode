// Hub-proxy error payloads cross the same JSON boundary as any other frame.
//
// A thrown error's own enumerable fields are carried back to the worker as
// `data`, but a `__proto__`/`constructor`/`prototype` field would turn a later
// merge into a prototype write. The boundary drops such data instead of shipping
// it.

import { describe, expect, test } from "bun:test"
import {
  HubProxyRemoteError,
  createHubProxyClient,
  serveHubProxy,
  type HubProxyHandlers,
} from "@/session/worker/hub-proxy-wire"

function pair(handlers: HubProxyHandlers): { client: ReturnType<typeof createHubProxyClient>; dispose: () => void } {
  const channel = new MessageChannel()
  const dispose = serveHubProxy(handlers, channel.port2)
  return { client: createHubProxyClient(channel.port1), dispose }
}

function selfPrototypeError(): Error {
  const error = new Error("boom")
  Object.defineProperty(error, "__proto__", {
    value: { polluted: true },
    enumerable: true,
    configurable: true,
  })
  return error
}

describe("hub-proxy error data boundary", () => {
  test("drops an own __proto__ field instead of shipping it to the worker", async () => {
    const { client, dispose } = pair({
      "sessionStatus.get": () => {
        throw selfPrototypeError()
      },
    })

    const error = await client.call("sessionStatus.get", ["a"]).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(HubProxyRemoteError)
    const data = (error as HubProxyRemoteError).data as Record<string, unknown> | undefined
    if (data !== undefined) expect(Object.hasOwn(data, "__proto__")).toBe(false)
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()

    client.dispose()
    dispose()
  })

  test("drops nested dangerous keys in an otherwise ordinary error field", async () => {
    const { client, dispose } = pair({
      "sessionStatus.get": () => {
        throw Object.assign(new Error("nope"), {
          _tag: "SessionNotFound",
          nested: JSON.parse('{"__proto__":{"polluted":true}}'),
        })
      },
    })

    const error = await client.call("sessionStatus.get", ["a"]).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(HubProxyRemoteError)
    expect((error as HubProxyRemoteError).tag).toBe("SessionNotFound")
    const data = (error as HubProxyRemoteError).data as Record<string, unknown> | undefined
    if (data !== undefined) expect(Object.hasOwn(data, "nested")).toBe(false)

    client.dispose()
    dispose()
  })

  test("keeps ordinary error data intact", async () => {
    const { client, dispose } = pair({
      "sessionStatus.get": () => {
        throw Object.assign(new Error("nope"), { _tag: "SessionNotFound", sessionID: "ses_x" })
      },
    })

    const error = await client.call("sessionStatus.get", ["a"]).catch((reason: unknown) => reason)
    expect((error as HubProxyRemoteError).data).toEqual({ sessionID: "ses_x" })

    client.dispose()
    dispose()
  })
})
