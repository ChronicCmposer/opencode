import { describe, expect, test } from "bun:test"
import { DurableLog } from "@opencode-ai/core/durable-log"
import type { LiveEventPublisher } from "@/event-v2-bridge"
import {
  aggregateIDOfEvent,
  createHubEventSink,
  createSessionHub,
  type HubTruthCache,
} from "@/session/worker/hub"
import { createTestBarrier } from "../../lib/run-barrier-test"
import type { ExecutorWorkerPool } from "@/session/worker/execute"

const publisher: LiveEventPublisher = {
  publish() {},
  publishDefinition() {},
} as unknown as LiveEventPublisher

const emptyPool = (): ExecutorWorkerPool => ({
  workers: [],
  spawn: () => {
    throw new Error("no worker expected")
  },
  terminateAll: async () => {},
  byID: () => undefined,
  resultFor: () => undefined,
})

const cacheOf = (cache: HubTruthCache | undefined): HubTruthCache => {
  if (cache === undefined) throw new Error("hub did not expose its advisory cache")
  return cache
}

describe("hub truth cache", () => {
  test("is keyed by (aggregate_id, seq) and never authoritative across a restart", async () => {
    const hub = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: emptyPool() })
    const cache = cacheOf(hub.cache)
    expect(cache.get("ses_one", 0)).toBeUndefined()

    cache.set("ses_one", 0, "first")
    cache.set("ses_one", 1, "second")
    cache.set("ses_two", 0, "other")
    expect(cache.get("ses_one", 0)).toBe("first")
    expect(cache.get("ses_one", 1)).toBe("second")
    expect(cache.get("ses_two", 0)).toBe("other")

    await hub.terminateAll()
    expect(cache.get("ses_one", 0)).toBeUndefined()

    const restarted = createSessionHub({ ...{ barrier: createTestBarrier() }, pool: emptyPool() })
    expect(restarted.cache?.get("ses_one", 0)).toBeUndefined()
    await restarted.terminateAll()
  })

  test("a durable hub event invalidates its aggregate and fences the old state version", async () => {
    const cache = new DurableLog.StateCache<string>()
    const sink = createHubEventSink({ publisher, cache })
    const event = { id: "evt_1", type: "session.updated", properties: { sessionID: "ses_one" } }
    const before = cache.version
    cache.set("ses_one", 0, "stale", before)
    cache.set("ses_two", 0, "kept", before)
    expect(aggregateIDOfEvent(event)).toBe("ses_one")

    sink.event(event, { durable: true })
    expect(cache.get("ses_one", 0)).toBeUndefined()
    expect(cache.get("ses_two", 0)).toBe("kept")
    expect(cache.version).toBe(before + 1)

    let rejected = false
    try {
      cache.set("ses_one", 0, "old-version", before)
    } catch (error) {
      rejected = error instanceof DurableLog.StaleStateVersionError
    }
    expect(rejected).toBe(true)

    // A non-durable delta must not flap the cache.
    const afterDelta = cache.version
    cache.set("ses_one", 0, "fresh")
    sink.event({ id: "evt_2", type: "message.part.delta", properties: { sessionID: "ses_one" } })
    expect(cache.get("ses_one", 0)).toBe("fresh")
    expect(cache.version).toBe(afterDelta)
  })
})
