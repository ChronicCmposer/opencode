// Regression for the egress fatal-callback contract: a callback throw, a
// callback defect, or an Effect-returning handler that fails is a fatal port
// error and must be reported through `onError`/`lastError` exactly once before
// the port closes. An interrupt is teardown, not an error, and stays silent.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { encodeFrame } from "@/session/egress/frame"
import { createEgressReceiver } from "@/session/egress/receiver"
import { type EventV2Event, eventFrame, endFrame } from "@/session/worker/frame"
import { type HubEventSink, serveEventsPort } from "@/session/worker/hub"
import { pollUntil } from "../../lib/poll"

const delta = (id: string): EventV2Event => ({
  id,
  type: "message.part.delta",
  properties: { id },
})

const eventText = (): string => encodeFrame(eventFrame({ event: delta("boom") }))

function openPort() {
  const channel = new MessageChannel()
  return channel
}

describe("egress fatal callback reporting", () => {
  test("a throwing onFrame closes the port and reports the thrown error exactly once", async () => {
    const channel = openPort()
    const thrown = new Error("sink boom")
    const errors: Error[] = []
    const receiver = createEgressReceiver({
      port: channel.port1,
      onFrame: () => {
        throw thrown
      },
      onError: (error) => errors.push(error),
    })
    try {
      channel.port2.postMessage(eventText())
      await pollUntil(() => errors.length === 1, {
        message: "a throwing onFrame closed the port without reporting the error",
      })
      expect(errors).toEqual([thrown])
      expect(receiver.lastError).toBe(thrown)
      // The port is dead: later frames are neither delivered nor reported again.
      channel.port2.postMessage(eventText())
      await pollUntil(() => false, { timeoutMs: 20 }).catch(() => {})
      expect(errors.length).toBe(1)
    } finally {
      receiver.close()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("an Effect-returning onFrame that fails reports that failure exactly once", async () => {
    const channel = openPort()
    const failure = new Error("async sink boom")
    const errors: Error[] = []
    const receiver = createEgressReceiver({
      port: channel.port1,
      onFrame: () => Effect.fail(failure),
      onError: (error) => errors.push(error),
    })
    try {
      channel.port2.postMessage(eventText())
      await pollUntil(() => errors.length === 1, {
        message: "a failing handler Effect closed the port without reporting the error",
      })
      expect(errors).toEqual([failure])
      expect(receiver.lastError).toBe(failure)
    } finally {
      receiver.close()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a throwing control handler is reported through onError", async () => {
    const channel = openPort()
    const thrown = new Error("end boom")
    const errors: Error[] = []
    const receiver = createEgressReceiver({
      port: channel.port1,
      onFrame: () => {},
      onEnd: () => {
        throw thrown
      },
      onError: (error) => errors.push(error),
    })
    try {
      channel.port2.postMessage(encodeFrame(endFrame()))
      await pollUntil(() => errors.length === 1, {
        message: "a throwing onEnd closed the port without reporting the error",
      })
      expect(errors).toEqual([thrown])
      expect(receiver.lastError).toBe(thrown)
    } finally {
      receiver.close()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("an interrupt stays silent: the port closes without calling onError", async () => {
    const channel = openPort()
    const errors: Error[] = []
    let calls = 0
    const receiver = createEgressReceiver({
      port: channel.port1,
      onFrame: () => {
        calls++
        return Effect.interrupt
      },
      onError: (error) => errors.push(error),
    })
    try {
      channel.port2.postMessage(eventText())
      await pollUntil(() => calls === 1 && channel.port1.onmessage === null, {
        message: "the interrupted handler was never reached or the port stayed attached",
      })
      expect(errors).toEqual([])
      expect(receiver.lastError).toBeUndefined()
    } finally {
      receiver.close()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("serveEventsPort reports a throwing hub sink through onError", async () => {
    const channel = openPort()
    const thrown = new Error("hub sink boom")
    const errors: Error[] = []
    const sink: HubEventSink = {
      event() {
        throw thrown
      },
      end() {},
      resync() {},
      ack() {},
      ended: false,
      resyncs: 0,
      acks: 0,
    }
    const dispose = serveEventsPort(channel.port1, sink, (error) => errors.push(error))
    try {
      channel.port2.postMessage(eventText())
      await pollUntil(() => errors.length === 1, {
        message: "serveEventsPort swallowed a throwing sink without calling onError",
      })
      expect(errors).toEqual([thrown])
      // The throw is reported, the frame dropped, and the port stays open.
      expect(channel.port1.onmessage).not.toBeNull()
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
