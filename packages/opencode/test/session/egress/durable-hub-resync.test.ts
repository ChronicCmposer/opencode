// A worker's `(aggregateID, seq)` frame IS the durable
// identity. The hub must mark it durable, invalidate the exact aggregate the
// frame names (not a payload-derived guess), and when the worker's stream skips
// a sequence it must ask the worker to resync from its database instead of
// closing the events port and ending the session stream.

import { describe, expect, test } from "bun:test"
import type { LiveEvent, LiveEventPublisher } from "@/event-v2-bridge"
import { decodeFrame, encodeFrame } from "@/session/egress/frame"
import { createHubEventSink, serveEventsPort, type HubCacheInvalidation } from "@/session/worker/hub"
import { eventFrame, type EventsFrame } from "@/session/worker/frame"
import { pollUntil } from "../../lib/poll"

function publisherHarness(): { readonly publisher: LiveEventPublisher; readonly published: LiveEvent[] } {
  const published: LiveEvent[] = []
  const publisher: LiveEventPublisher = {
    publish: (event) => void published.push(event),
    publishDefinition: (definition, data) => void published.push({ type: definition.type, data }),
  }
  return { publisher, published }
}

const update = (id: string, sessionID: string, title: string): EventsFrame =>
  eventFrame({
    event: { id, type: "session.updated", properties: { sessionID, info: { sessionID, title } } },
  })

const durableUpdate = (id: string, sessionID: string, title: string, seq: number): EventsFrame =>
  eventFrame({
    event: { id, type: "session.updated", properties: { sessionID, info: { sessionID, title } } },
    aggregateID: sessionID,
    seq,
  })

function channelHarness() {
  const channel = new MessageChannel()
  const { publisher, published } = publisherHarness()
  const invalidated: string[] = []
  const cache: HubCacheInvalidation = { invalidate: (aggregateID) => void invalidated.push(aggregateID) }
  const sink = createHubEventSink({ publisher, cache })
  const workerFrames: EventsFrame[] = []
  channel.port2.onmessage = (message: MessageEvent) => {
    if (typeof message.data === "string") workerFrames.push(decodeFrame(message.data))
  }
  channel.port2.start()
  const dispose = serveEventsPort(channel.port1, sink)
  return {
    channel,
    sink,
    published,
    invalidated,
    workerFrames,
    dispose() {
      dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

describe("hub durable aggregate identity", () => {
  test("a (aggregateID, seq) frame is durable and invalidates the frame's aggregate", () => {
    const { publisher } = publisherHarness()
    const invalidated: string[] = []
    const sink = createHubEventSink({
      publisher,
      cache: { invalidate: (aggregateID) => void invalidated.push(aggregateID) },
    })

    // The payload names one session; the committed aggregate is another. The
    // frame's identity is authoritative — deriving from `sessionID` would
    // invalidate the wrong cache entry.
    sink.event(
      { id: "evt_1", type: "session.updated", properties: { sessionID: "ses_payload" } },
      { durable: true, aggregateID: "ses_committed", seq: 4 },
    )

    expect(invalidated).toEqual(["ses_committed"])
  })

  test("serveEventsPort marks worker aggregate frames durable end to end", async () => {
    const harness = channelHarness()
    try {
      harness.channel.port2.postMessage(encodeFrame(durableUpdate("evt_1", "ses_title", "initial", 1)))
      await pollUntil(() => harness.published.length === 1)
      expect(harness.invalidated).toEqual(["ses_title"])
      expect(harness.sink.resyncs).toBe(0)
    } finally {
      harness.dispose()
    }
  })
})

describe("receiver gap resyncs instead of closing the port", () => {
  test("a mid-stream gap surfaces Resync and keeps the stream open", async () => {
    const harness = channelHarness()
    try {
      harness.channel.port2.postMessage(encodeFrame(durableUpdate("evt_1", "ses_title", "initial", 1)))
      // A rename advanced the same aggregate, but the intermediate commit never
      // reached the hub: deliver seq 3, not seq 2.
      harness.channel.port2.postMessage(encodeFrame(durableUpdate("evt_3", "ses_title", "renamed", 3)))
      await pollUntil(() => harness.sink.resyncs === 1, { message: "gap never surfaced as a resync" })

      // The worker is asked to heal from its DB snapshot, with the exact
      // aggregate/sequence that went missing.
      await pollUntil(() => harness.workerFrames.some((frame) => frame.kind === "resync"))
      const resync = harness.workerFrames.find((frame) => frame.kind === "resync")
      expect(resync).toMatchObject({ kind: "resync", payload: { aggregateID: "ses_title", seq: 3 } })

      // Streaming continues: the next durable frame is delivered live-only.
      harness.channel.port2.postMessage(encodeFrame(durableUpdate("evt_4", "ses_title", "still streaming", 4)))
      await pollUntil(() => harness.published.length === 3, { message: "stream closed after a gap" })
      expect(harness.sink.resyncs).toBe(1)
    } finally {
      harness.dispose()
    }
  })

  test("a non-durable delta is never mistaken for a durable frame", async () => {
    const harness = channelHarness()
    try {
      harness.channel.port2.postMessage(encodeFrame(update("evt_delta", "ses_title", "delta")))
      await pollUntil(() => harness.published.length === 1)
      expect(harness.invalidated).toEqual([])
    } finally {
      harness.dispose()
    }
  })
})
