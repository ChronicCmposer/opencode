import { describe, expect, test } from "bun:test"

import {
  DELTA_QUEUE_BOUND,
  createDeltaQueue,
  createDeltaSubscription,
  createEventLogSnapshotReader,
  type DeltaDelivery,
  type DeltaResync,
  type DeltaSnapshot,
  type DurableDelivery,
} from "@/session/egress/delta"

const event = (id: string, properties: Record<string, unknown> = { id }) => ({
  id,
  type: "message.part.updated",
  properties,
})

/** Let the resync snapshot chain drain its resolved promises. */
async function settleSnapshotChain(): Promise<void> {
  for (let tick = 0; tick < 10; tick++) await Promise.resolve()
}

function queueHarness(bound: number) {
  const deltas: DeltaDelivery[] = []
  const durables: DurableDelivery[] = []
  const queue = createDeltaQueue({
    bound,
    onDelta: (delta) => deltas.push(delta),
    onDurable: (durable) => durables.push(durable),
  })
  return { queue, deltas, durables }
}

describe("bounded per-session delta queue", () => {
  test("assigns one monotonic delta sequence per subscription after coalescing", () => {
    const { queue, deltas } = queueHarness(8)

    queue.pushDelta({ aggregateID: "s1", coalesceKey: "part-a", event: event("a1") })
    queue.pushDelta({ aggregateID: "s1", coalesceKey: "part-b", event: event("b1") })
    // A later update for the same key folds the intermediate delta in place.
    queue.pushDelta({ aggregateID: "s1", coalesceKey: "part-a", event: event("a2") })
    queue.pushDelta({ aggregateID: "s1", coalesceKey: "part-c", event: event("c1") })
    queue.flush()

    expect(queue.coalesced).toBe(1)
    expect(queue.dropped).toBe(0)
    expect(deltas.map((delta) => delta.event.id)).toEqual(["a2", "b1", "c1"])
    expect(deltas.map((delta) => delta.seq)).toEqual([1, 2, 3])
    expect(queue.lastSeq).toBe(3)
  })

  test("drops the oldest intermediate deltas when the bound is reached", () => {
    const { queue, deltas } = queueHarness(2)

    queue.pushDelta({ aggregateID: "s1", event: event("a") })
    queue.pushDelta({ aggregateID: "s1", event: event("b") })
    queue.pushDelta({ aggregateID: "s1", event: event("c") })
    queue.flush()

    expect(queue.dropped).toBe(1)
    expect(queue.coalesced).toBe(0)
    expect(queue.pending).toBe(0)
    expect(deltas.map((delta) => delta.event.id)).toEqual(["b", "c"])
    expect(deltas.map((delta) => delta.seq)).toEqual([1, 2])
  })

  test("never drops a durable event, even under delta overflow", () => {
    const { queue, deltas, durables } = queueHarness(1)

    queue.pushDelta({ aggregateID: "s1", event: event("d1") })
    queue.pushDurable({ aggregateID: "s1", seq: 1, event: event("durable-1") })
    queue.pushDelta({ aggregateID: "s1", event: event("d2") })
    queue.pushDelta({ aggregateID: "s1", event: event("d3") })
    queue.pushDurable({ aggregateID: "s1", seq: 2, event: event("durable-2") })
    queue.flush()

    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-1", "durable-2"])
    expect(durables.map((durable) => durable.seq)).toEqual([1, 2])
    // One delta survived each bound; the intermediate delta was dropped.
    expect(deltas.map((delta) => delta.event.id)).toEqual(["d1", "d3"])
    expect(queue.dropped).toBe(1)
  })

  test("rejects an invalid durable identity at the boundary", () => {
    const { queue } = queueHarness(DELTA_QUEUE_BOUND)
    expect(() => queue.pushDurable({ aggregateID: "", seq: 1, event: event("bad") })).toThrow(/aggregateID/)
    expect(() => queue.pushDurable({ aggregateID: "s1", seq: 0, event: event("bad") })).toThrow(/seq/)
    expect(() =>
      queue.pushDurable({ aggregateID: "s1", seq: 1.5, event: event("bad") }),
    ).toThrow(/seq/)
  })
})

describe("delta subscription gap detection and resync", () => {
  test("a delta gap emits typed Resync and a snapshot at the last durable seq", async () => {
    const deltas: DeltaDelivery[] = []
    const durables: DurableDelivery[] = []
    const resyncs: DeltaResync[] = []
    const snapshots: DeltaSnapshot[] = []
    const snapshotCalls: Array<readonly [string, number]> = []

    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: (aggregateID, seq) => {
        snapshotCalls.push([aggregateID, seq])
        return Promise.resolve([event("snapshot")])
      },
      onDelta: (delta) => deltas.push(delta),
      onDurable: (durable) => durables.push(durable),
      onResync: (resync) => resyncs.push(resync),
      onSnapshot: (snapshot) => snapshots.push(snapshot),
    })

    subscription.receiveDelta({ aggregateID: "s1", seq: 1, event: event("d1") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 1, event: event("durable-1") })
    // seq 2 is lost in transit; the next delivered delta is seq 3.
    subscription.receiveDelta({ aggregateID: "s1", seq: 3, event: event("d3") })
    await settleSnapshotChain()

    expect(resyncs).toEqual([{ type: "resync", aggregateID: "s1", seq: 1 }])
    expect(snapshotCalls).toEqual([["s1", 1]])
    expect(snapshots).toHaveLength(1)
    expect(snapshots[0]?.events.map((entry) => entry.id)).toEqual(["snapshot"])
    expect(deltas.map((delta) => delta.event.id)).toEqual(["d1"])
    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-1"])

    // Sequence resumes after the gapped frame was observed.
    subscription.receiveDelta({ aggregateID: "s1", seq: 4, event: event("d4") })
    expect(deltas.map((delta) => delta.event.id)).toEqual(["d1", "d4"])
    expect(subscription.lastDeltaSeq).toBe(4)
    expect(subscription.lastDurableSeq).toBe(1)
  })

  test("durable identity is (aggregateID, seq), independent of the delta sequence", async () => {
    const deltas: DeltaDelivery[] = []
    const durables: DurableDelivery[] = []
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: () => Promise.resolve([]),
      onDelta: (delta) => deltas.push(delta),
      onDurable: (durable) => durables.push(durable),
    })

    subscription.receiveDurable({ aggregateID: "s1", seq: 1, event: event("durable-1") })
    subscription.receiveDelta({ aggregateID: "s1", seq: 1, event: event("delta-1") })
    subscription.receiveDelta({ aggregateID: "s1", seq: 2, event: event("delta-2") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 2, event: event("durable-2") })
    // A replayed durable at or below the durable cursor is idempotent.
    subscription.receiveDurable({ aggregateID: "s1", seq: 2, event: event("durable-2-replay") })

    expect(deltas.map((delta) => delta.event.id)).toEqual(["delta-1", "delta-2"])
    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-1", "durable-2"])
    expect(subscription.lastDeltaSeq).toBe(2)
    expect(subscription.lastDurableSeq).toBe(2)
    expect(subscription.stale).toBe(1)
  })

  test("durables stay ordered behind the snapshot that healed their gap", async () => {
    const delivered: string[] = []
    const snapshotCalls: number[] = []
    let releaseSnapshot!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseSnapshot = resolve
    })
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: async (_aggregateID, seq) => {
        snapshotCalls.push(seq)
        await gate
        return [event("snapshot")]
      },
      onDelta: () => undefined,
      onDurable: (durable) => delivered.push(durable.event.id),
    })

    subscription.receiveDurable({ aggregateID: "s1", seq: 1, event: event("durable-1") })
    // Durable 2 is lost; 3 and 4 arrive while the healing snapshot is in flight.
    subscription.receiveDurable({ aggregateID: "s1", seq: 3, event: event("durable-3") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 4, event: event("durable-4") })

    expect(delivered).toEqual(["durable-1"])
    await settleSnapshotChain()
    expect(snapshotCalls).toEqual([1])

    releaseSnapshot()
    await settleSnapshotChain()
    expect(delivered).toEqual(["durable-1", "durable-3", "durable-4"])
  })

  test("rejects a delivery for another aggregate at the boundary", () => {
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: () => Promise.resolve([]),
      onDelta: () => undefined,
      onDurable: () => undefined,
    })
    expect(() => subscription.receiveDelta({ aggregateID: "s2", seq: 1, event: event("x") })).toThrow(
      /aggregateID/,
    )
  })
})

describe("durable snapshot reader", () => {
  test("reads the aggregate log up to and including the requested durable seq", async () => {
    const calls: Array<{ method: string; sql: string; params: readonly unknown[] }> = []
    const reader = createEventLogSnapshotReader(async (method, sql, params) => {
      calls.push({ method, sql, params })
      return [
        { id: "e1", type: "message.updated", data: JSON.stringify({ id: "e1", text: "hello" }) },
        { id: "e2", type: "message.updated", data: { id: "e2" } },
      ]
    })

    const events = await reader("s1", 7)

    expect(calls).toHaveLength(1)
    expect(calls[0]?.method).toBe("all")
    expect(calls[0]?.params).toEqual(["s1", 7])
    expect(calls[0]?.sql).toMatch(/FROM event/i)
    expect(calls[0]?.sql).toMatch(/ORDER BY seq ASC/i)
    expect(events).toEqual([
      { id: "e1", type: "message.updated", properties: { id: "e1", text: "hello" } },
      { id: "e2", type: "message.updated", properties: { id: "e2" } },
    ])
  })

  test("returns no rows for a zero cursor without querying", async () => {
    let queries = 0
    const reader = createEventLogSnapshotReader(async () => {
      queries++
      return []
    })
    expect(await reader("s1", 0)).toEqual([])
    expect(queries).toBe(0)
  })
})
