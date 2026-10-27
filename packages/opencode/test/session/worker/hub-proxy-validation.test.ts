// Step 37 — hub-proxy request payload validation and exactly-once replies.
//
// The control port is attacker-adjacent: every inbound request payload is
// validated at the boundary. A missing/non-numeric id is a typed protocol error
// that isolates the poisoned channel; non-array args and unknown methods get
// exactly one typed error reply. A non-serialisable success value posts exactly
// one clone-safe fallback reply.

import { describe, expect, test } from "bun:test"
import { ProtocolError } from "@/session/worker/frame"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/hub-proxy-wire"

const tick = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

function envelope(payload: unknown, id?: unknown): string {
  return JSON.stringify({
    v: 1,
    port: "hub-proxy",
    kind: "request",
    ...(id === undefined ? {} : { id }),
    payload,
  })
}

async function collect(port: MessagePort): Promise<string[]> {
  const seen: string[] = []
  port.addEventListener("message", (event) => seen.push(String(event.data)))
  port.start()
  await tick(50)
  return seen
}

describe("hub-proxy payload validation", () => {
  test("a missing id is a typed protocol error and the channel is isolated", async () => {
    const channel = new MessageChannel()
    const errors: unknown[] = []
    let called = false
    const replies = collect(channel.port1)
    serveHubProxy({ "sessionStatus.list": () => { called = true; return "x" } }, channel.port2, undefined, {
      onProtocolError: (error) => errors.push(error),
    })

    channel.port1.postMessage(envelope({ kind: "request", method: "sessionStatus.list", args: [] }))
    await tick(50)

    expect(called).toBe(false)
    expect(errors).toHaveLength(1)
    expect(errors[0]).toBeInstanceOf(ProtocolError)
    expect(await replies).toEqual([])

    // Isolated: a valid request afterwards is ignored.
    channel.port1.postMessage(envelope({ kind: "request", id: 2, method: "sessionStatus.list", args: [] }, 2))
    await tick(50)
    expect(called).toBe(false)
    channel.port1.close()
    channel.port2.close()
  })

  test("a non-numeric id is rejected as a typed protocol error", async () => {
    const channel = new MessageChannel()
    const errors: unknown[] = []
    let called = false
    serveHubProxy({ "sessionStatus.list": () => { called = true } }, channel.port2, undefined, {
      onProtocolError: (error) => errors.push(error),
    })

    channel.port1.postMessage(envelope({ kind: "request", id: "one", method: "sessionStatus.list", args: [] }, "one"))
    await tick(50)

    expect(called).toBe(false)
    expect(errors[0]).toBeInstanceOf(ProtocolError)
    channel.port1.close()
    channel.port2.close()
  })

  test("non-array args get exactly one typed error reply", async () => {
    const channel = new MessageChannel()
    const replies = collect(channel.port1)
    serveHubProxy({ "sessionStatus.list": () => "never" }, channel.port2)

    channel.port1.postMessage(envelope({ kind: "request", id: 9, method: "sessionStatus.list", args: "nope" }, 9))
    const seen = await replies

    expect(seen).toHaveLength(1)
    const payload = JSON.parse(seen[0]!) as { payload: { ok: boolean; id: number; message: string } }
    expect(payload.payload).toMatchObject({ kind: "reply", ok: false, id: 9 })
    expect(payload.payload.message).toContain("args")
    channel.port1.close()
    channel.port2.close()
  })

  test("an unknown method gets exactly one typed error reply", async () => {
    const channel = new MessageChannel()
    const replies = collect(channel.port1)
    serveHubProxy({}, channel.port2)

    channel.port1.postMessage(envelope({ kind: "request", id: 3, method: "not.aMethod", args: [] }, 3))
    const seen = await replies

    expect(seen).toHaveLength(1)
    const payload = JSON.parse(seen[0]!) as { payload: { ok: boolean; message: string } }
    expect(payload.payload.ok).toBe(false)
    expect(payload.payload.message).toContain("unknown hub-proxy method")
    channel.port1.close()
    channel.port2.close()
  })

  test("a clone-failure reply posts exactly one clone-safe fallback", async () => {
    const channel = new MessageChannel()
    const posted: string[] = []
    channel.port2.postMessage = ((frame: string) => posted.push(frame)) as never
    serveHubProxy({ messagesTransform: () => ({ onEvent: () => {} }) }, channel.port2)
    const client = createHubProxyClient(channel.port1, { timeoutMs: 1000 })
    try {
      void client.call("messagesTransform", [{}]).catch(() => {})
      for (let i = 0; i < 100 && posted.length === 0; i += 1) await tick(10)
      expect(posted).toHaveLength(1)
      const payload = JSON.parse(posted[0]!) as { payload: { ok: boolean; message: string } }
      expect(payload.payload.ok).toBe(false)
      expect(payload.payload.message).toContain("not structured-cloneable")
    } finally {
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
