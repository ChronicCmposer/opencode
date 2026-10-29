// The events port has ONE live implementation: `serveEventsPort` decodes every
// message through the shared `decodeWorkerFrame` boundary (4 MiB cap + safe-json
// guards) and surfaces a rejected frame as an observed error. This pins that a
// bad frame never stops delivery silently and that the production backend logs
// the rejection with worker context.

import { describe, expect, test } from "bun:test"
import type { LiveEventPublisher } from "@/event-v2-bridge"
import { buildProductionPortBackends, createHubEventSink, serveEventsPort } from "@/session/worker/hub"
import { workerLogMessage } from "@/session/worker/worker-logging"
import { FrameTooLargeError, MAX_WORKER_FRAME_BYTES, ProtocolError } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"
import { recordingLogger } from "../../lib/recorders"

const publisher: LiveEventPublisher = {
  publish: () => {},
  publishDefinition: () => {},
}

const sinkFor = () => createHubEventSink({ publisher })

describe("events port failure observation", () => {
  test("a malformed frame is observed, not swallowed, and stops delivery", async () => {
    const channel = new MessageChannel()
    const errors: Error[] = []
    const dispose = serveEventsPort(channel.port1, sinkFor(), (error) => errors.push(error))
    try {
      channel.port2.postMessage("{not json")
      await pollUntil(() => errors.length === 1, { message: "malformed frame was silently swallowed" })
      expect(errors[0]).toBeInstanceOf(ProtocolError)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a non-string frame is rejected with a typed and observed error", async () => {
    const channel = new MessageChannel()
    const errors: Error[] = []
    const dispose = serveEventsPort(channel.port1, sinkFor(), (error) => errors.push(error))
    try {
      channel.port2.postMessage(new Uint8Array([1, 2, 3]))
      await pollUntil(() => errors.length === 1, { message: "non-string frame was silently dropped" })
      expect(errors[0]).toBeInstanceOf(ProtocolError)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("an over-bound frame is rejected with the shared frame-cap error", async () => {
    const channel = new MessageChannel()
    const errors: Error[] = []
    const dispose = serveEventsPort(channel.port1, sinkFor(), (error) => errors.push(error))
    try {
      channel.port2.postMessage("x".repeat(MAX_WORKER_FRAME_BYTES + 1))
      await pollUntil(() => errors.length === 1, { message: "oversized frame was silently dropped" })
      expect(errors[0]).toBeInstanceOf(FrameTooLargeError)
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("the production backend logs a rejected frame with worker context", async () => {
    const channel = new MessageChannel()
    const { bridge, lines: logged } = recordingLogger()
    const backends = buildProductionPortBackends({ events: publisher, bridge })
    const serveEgress = backends.egress
    if (serveEgress === undefined) throw new Error("production backends must serve the egress port")
    const dispose = serveEgress(7, channel.port1)
    try {
      channel.port2.postMessage("{not json")
      await pollUntil(() => logged.length === 1, { message: "rejected egress frame was not logged" })
      expect(logged[0]?.message).toBe(workerLogMessage("egress frame rejected"))
      expect(logged[0]?.level).toBe("error")
      expect(logged[0]?.fields.workerID).toBe(7)
      expect(logged[0]?.fields.errorName).toBe("ProtocolError")
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
      await backends.dispose()
    }
  })
})
