// Egress stays usable across a bad frame, an unresolvable oversized durable, and
// a run reply that overtakes its events-port frames.

import { describe, expect, test } from "bun:test"
import { decodeFrame, encodeFrame } from "@/session/egress/frame"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import { createEgressDeliveryBarrier, createHubEventSink, serveEventsPort } from "@/session/worker/hub"
import { ackFrame, blobPositionReferenceFrame, eventFrame, MAX_WORKER_FRAME_BYTES } from "@/session/worker/frame"
import { flush, pollUntil, sid, withDeadline } from "../../lib/async"

const AGGREGATE = "ses_recovery"

function recordingPublisher(): { publisher: LiveEventPublisher; published: LiveEvent[] } {
  const published: LiveEvent[] = []
  const publisher: LiveEventPublisher = {
    publish: (event) => void published.push(event),
    publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
  }
  return { publisher, published }
}

function durable(seq: number) {
  return encodeFrame(
    eventFrame({
      event: { id: `evt_${seq}`, type: "session.message", properties: { sessionID: AGGREGATE, seq } },
      aggregateID: AGGREGATE,
      seq,
    }),
  )
}

describe("egress recovery", () => {
  test("a frame that fails decode does not stop later durables", async () => {
    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({ publisher })
    const channel = new MessageChannel()
    const resyncs: unknown[] = []
    channel.port2.onmessage = (event) => void resyncs.push(decodeFrame(event.data as string))
    const errors: Error[] = []
    const dispose = serveEventsPort(channel.port1, sink, (error) => void errors.push(error))
    try {
      channel.port2.postMessage(durable(1))
      channel.port2.postMessage("{not json")
      channel.port2.postMessage(durable(2))
      await pollUntil(() => published.length === 2, { message: "durables after a bad frame were not delivered" })

      expect(errors).toHaveLength(1)
      await pollUntil(() => resyncs.length === 1, { message: "no resync was requested for the bad frame" })
      expect((resyncs[0] as { kind: string }).kind).toBe("resync")
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("an unresolved oversized durable is healed through its own position", async () => {
    const { publisher, published } = recordingPublisher()
    const body = { sessionID: AGGREGATE, text: "committed" }
    const sink = createHubEventSink({
      publisher,
      resolveBlob: async () => undefined,
      readDurable: async (_aggregateID, after, upTo) =>
        after < 1 && upTo >= 1 ? [{ seq: 1, event: { id: "evt_big", type: "session.message", properties: body } }] : [],
    })
    const channel = new MessageChannel()
    const resyncs: unknown[] = []
    channel.port2.onmessage = (event) => void resyncs.push(decodeFrame(event.data as string))
    const dispose = serveEventsPort(channel.port1, sink)
    try {
      const reference = blobPositionReferenceFrame(
        eventFrame({
          event: { id: "evt_big", type: "session.message", properties: body },
          aggregateID: AGGREGATE,
          seq: 1,
        }),
        MAX_WORKER_FRAME_BYTES + 1,
      )
      if (reference === undefined) throw new Error("no position reference produced")
      channel.port2.postMessage(encodeFrame(reference))

      await pollUntil(() => published.length === 1, { message: "the unresolved durable was never healed" })
      expect(published[0]?.data).toEqual(body)
      await pollUntil(() => resyncs.length === 1, { message: "the worker was not asked to resend" })
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a durable that lands while its oversized predecessor heals is not lost", async () => {
    const { publisher, published } = recordingPublisher()
    const body = { sessionID: AGGREGATE, text: "committed" }
    const sink = createHubEventSink({
      publisher,
      readDurable: async (_aggregateID, after, upTo) =>
        after < 1 && upTo >= 1 ? [{ seq: 1, event: { id: "evt_big", type: "session.message", properties: body } }] : [],
    })
    sink.event(
      { id: "evt_2", type: "session.message", properties: { sessionID: AGGREGATE, seq: 2 } },
      { durable: true, aggregateID: AGGREGATE, seq: 2 },
    )
    sink.resync({ aggregateID: AGGREGATE, seq: 1 })
    await pollUntil(() => published.length === 2, { message: "the lost position was never published" })
    expect(published.map((event) => event.data)).toContainEqual(body)
  })

  test("a rejected last durable is healed when the run-end ack arrives", async () => {
    const { publisher, published } = recordingPublisher()
    const sink = createHubEventSink({
      publisher,
      readDurable: async (_aggregateID, after) =>
        after < 2
          ? [{ seq: 2, event: { id: "evt_2", type: "session.message", properties: { sessionID: AGGREGATE, seq: 2 } } }]
          : [],
    })
    const channel = new MessageChannel()
    const dispose = serveEventsPort(channel.port1, sink, () => {})
    try {
      channel.port2.postMessage(durable(1))
      channel.port2.postMessage("{not json")
      channel.port2.postMessage(encodeFrame(ackFrame({ count: 0 })))
      await pollUntil(() => published.length === 2, { message: "the rejected last durable was not healed" })
    } finally {
      dispose()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("the barrier holds when the done reply arrives before its events-port frames", async () => {
    const session = sid("ses_done_first")
    const barrier = createEgressDeliveryBarrier({ timeoutMs: 5_000 })
    let resolved = false
    const wait = barrier.wait(session, 2).then(() => {
      resolved = true
    })
    await flush()
    expect(resolved).toBe(false)

    barrier.observeDurable(session)
    barrier.observeAck(session, 1)
    await flush()
    expect(resolved).toBe(false)

    barrier.observeDurable(session)
    barrier.observeAck(session, 1)
    await withDeadline(wait, { timeoutMs: 5_000, message: "barrier never settled" })
    expect(resolved).toBe(true)
  })

  test("release forgets a session's progress", async () => {
    const session = sid("ses_released")
    const barrier = createEgressDeliveryBarrier({ timeoutMs: 5_000 })
    barrier.observeDurable(session)
    const wait = barrier.wait(session)
    barrier.release(session)
    await withDeadline(wait, { timeoutMs: 5_000, message: "release did not free the waiter" })
  })
})
