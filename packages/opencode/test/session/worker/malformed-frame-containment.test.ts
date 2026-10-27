// Step 36 — a malformed inbound frame is contained at the host boundary.
//
// A worker peer that posts garbage must not throw out of the host's message
// listener. The failure is surfaced as a typed channel error, the host reports
// it once, and the offending channel is isolated (no further frames served) so
// the host keeps running for every other worker.

import { describe, expect, test } from "bun:test"
import { ProtocolError } from "@/session/worker/frame"
import { serveHubProxy } from "@/session/worker/hub-proxy-wire"

const tick = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

describe("malformed inbound frame containment", () => {
  test("garbage is a typed channel error, not an uncaught host exception", async () => {
    const channel = new MessageChannel()
    const errors: unknown[] = []
    const handled: unknown[] = []
    const uncaught: unknown[] = []
    const onUncaught = (error: unknown) => uncaught.push(error)
    process.on("uncaughtException", onUncaught)

    let answered = false
    channel.port1.addEventListener("message", () => {
      answered = true
    })
    channel.port1.start()

    try {
      serveHubProxy({ "sessionStatus.list": () => "never" }, channel.port2, undefined, {
        onProtocolError: (error) => errors.push(error),
      })

      channel.port1.postMessage("{not-json")
      await tick(50)

      expect(errors).toHaveLength(1)
      expect(errors[0]).toBeInstanceOf(ProtocolError)
      expect(handled).toEqual([])
      expect(uncaught).toEqual([])

      // The poisoned channel is isolated: a later valid request is not served.
      channel.port1.postMessage(
        JSON.stringify({ v: 1, port: "hub-proxy", kind: "request", payload: { kind: "request", id: 1, method: "sessionStatus.list", args: [] } }),
      )
      await tick(50)
      expect(answered).toBe(false)
    } finally {
      process.off("uncaughtException", onUncaught)
      channel.port1.close()
      channel.port2.close()
    }
  })
})
