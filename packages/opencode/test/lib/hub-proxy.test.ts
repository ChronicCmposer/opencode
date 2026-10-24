import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { sid } from "./async"
import { instantRunner, recordingHubAuthorities, serveHubProxyFor } from "./hub-proxy"

describe("instantRunner", () => {
  test("runs a hub authority effect on the current runtime and resolves its value", async () => {
    await expect(instantRunner(Effect.succeed(42))).resolves.toBe(42)
  })
})

describe("serveHubProxyFor", () => {
  test("serves the handlers and returns the worker-side client", async () => {
    const served = serveHubProxyFor({
      "sessionStatus.get": () => ({ type: "idle" }),
    })
    try {
      await expect(served.client.call("sessionStatus.get", ["ses_1"])).resolves.toEqual({ type: "idle" })
    } finally {
      served.dispose()
    }
  })

  test("an unhandled method fails loudly across the port", async () => {
    const served = serveHubProxyFor({})
    try {
      await expect(served.client.call("permission.list", [])).rejects.toThrow(/unknown hub-proxy method/)
    } finally {
      served.dispose()
    }
  })
})

describe("recordingHubAuthorities", () => {
  test("records each authority call with its arguments", async () => {
    const { authorities, calls } = recordingHubAuthorities()

    await Effect.runPromise(authorities.sessionStatus.set(sid("ses_1"), { type: "busy" }))

    expect(calls).toEqual([{ method: "sessionStatus.set", args: [sid("ses_1"), { type: "busy" }] }])
  })

  test("an override changes behaviour and is still recorded", async () => {
    const { authorities, calls } = recordingHubAuthorities({
      sessionStatus: { get: () => Effect.succeed({ type: "busy" }) },
    })

    await expect(Effect.runPromise(authorities.sessionStatus.get(sid("ses_1")))).resolves.toEqual({ type: "busy" })
    expect(calls).toEqual([{ method: "sessionStatus.get", args: [sid("ses_1")] }])
  })
})
