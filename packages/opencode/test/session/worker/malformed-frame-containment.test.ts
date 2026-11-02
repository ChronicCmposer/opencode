// A malformed inbound frame is contained at the host boundary.
//
// A worker peer that posts garbage must not throw out of the host's message
// listener. The failure is surfaced as a typed channel error, the host reports
// it once, and the offending channel is isolated (no further frames served) so
// the host keeps running for every other worker.

import { describe, expect, test } from "bun:test"
import { ProtocolError } from "@/session/worker/frame"
import { serveHubProxy } from "@/session/worker/hub-proxy-wire"
import { makeAttachments, servePidReportPort } from "@/session/worker/worker-ports"
import { pollUntil } from "../../lib/async"

const tick = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

const pidReport = (workerID: number, pgid: number) => ({
  type: "pid-report",
  workerID,
  entries: [{ pid: pgid, pgid, label: "shell", workerID }],
})

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

  test("a malformed hub-proxy frame through the worker channel reports (workerID, error, hub-proxy)", async () => {
    const channel = new MessageChannel()
    const failures: Array<{ readonly workerID: number; readonly error: unknown; readonly source: string }> = []
    const uncaught: unknown[] = []
    const onUncaught = (error: unknown) => uncaught.push(error)
    process.on("uncaughtException", onUncaught)
    const attachments = makeAttachments({
      backends: { hubProxy: { "sessionStatus.get": () => "pong" } },
      onProtocolError: (workerID, error, source) => failures.push({ workerID, error, source }),
    })

    try {
      const dispose = attachments.attachHubProxyPort(7, channel.port1)
      channel.port2.postMessage("{not-json")
      await pollUntil(() => failures.length === 1, {
        message: "the host never reported the malformed hub-proxy frame",
      })

      expect(failures[0]!.workerID).toBe(7)
      expect(failures[0]!.source).toBe("hub-proxy")
      expect(failures[0]!.error).toBeInstanceOf(ProtocolError)
      expect(uncaught).toEqual([])

      dispose()
    } finally {
      attachments.disposeAll()
      process.off("uncaughtException", onUncaught)
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a throwing pid-report onReport reports (workerID, error, pid-report) and never throws", async () => {
    const channel = new MessageChannel()
    const failures: Array<{ readonly workerID: number; readonly error: unknown; readonly source: string }> = []
    const uncaught: unknown[] = []
    const onUncaught = (error: unknown) => uncaught.push(error)
    process.on("uncaughtException", onUncaught)
    const failure = new Error("pid report handler failed")
    const attachments = makeAttachments({
      backends: { hubProxy: { "sessionStatus.get": () => "pong" } },
      onPidReport: () => {
        throw failure
      },
      onProtocolError: (workerID, error, source) => failures.push({ workerID, error, source }),
    })

    try {
      const dispose = attachments.attachHubProxyPort(3, channel.port1)
      channel.port2.postMessage(pidReport(3, 4242))
      await pollUntil(() => failures.length === 1, {
        message: "the host never reported the throwing pid-report handler",
      })

      expect(failures[0]).toEqual({ workerID: 3, error: failure, source: "pid-report" })
      expect(uncaught).toEqual([])

      dispose()
    } finally {
      attachments.disposeAll()
      process.off("uncaughtException", onUncaught)
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("without an onProtocolError handler the pid-report failure still throws", () => {
    // Drive the listener directly: a real MessagePort reports listener throws
    // asynchronously, which would attribute them to the test runner, not prove
    // the helper's synchronous throw path.
    const listeners: Array<(event: MessageEvent) => void> = []
    const port = {
      addEventListener: (type: string, listener: (event: MessageEvent) => void) => {
        if (type === "message") listeners.push(listener)
      },
      removeEventListener: () => {},
      start: () => {},
    } as unknown as MessagePort
    const failure = new Error("uncontained pid report failure")
    const dispose = servePidReportPort(
      () => {
        throw failure
      },
      port,
      undefined,
    )

    try {
      const event = { data: pidReport(1, 1111), stopImmediatePropagation: () => {} } as MessageEvent
      expect(() => listeners[0]!(event)).toThrow("uncontained pid report failure")
    } finally {
      dispose()
    }
  })
})
