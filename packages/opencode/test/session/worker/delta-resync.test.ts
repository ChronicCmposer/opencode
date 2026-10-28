import { describe, expect, test } from "bun:test"

import {
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

describe("delta subscription gap detection and resync", () => {
  test("seq 0 is a real durable position, not an unseen sentinel", () => {
    const durables: DurableDelivery[] = []
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: () => Promise.resolve([]),
      onDelta: () => undefined,
      onDurable: (durable) => durables.push(durable),
    })

    subscription.receiveDurable({ aggregateID: "s1", seq: 0, event: event("durable-0") })

    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-0"])
    expect(subscription.lastDurableSeq).toBe(0)
    expect(subscription.stale).toBe(0)
  })

  test("a first observed durable above 0 seeds the cursor: no resync and no full-log read", async () => {
    const snapshots: DeltaSnapshot[] = []
    const snapshotCalls: number[] = []
    const durables: DurableDelivery[] = []
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: (_aggregateID, seq) => {
        snapshotCalls.push(seq)
        return Promise.resolve([event("event-0"), event("event-1")])
      },
      onDelta: () => undefined,
      onDurable: (durable) => durables.push(durable),
      onSnapshot: (snapshot) => snapshots.push(snapshot),
    })

    // A subscription attached to an existing session observes its first commit
    // at sequence 57. That is the seed, not a gap: healing would read the whole
    // durable log from 0 for no reason.
    subscription.receiveDurable({ aggregateID: "s1", seq: 57, event: event("durable-57") })
    await settleSnapshotChain()

    expect(snapshotCalls).toEqual([])
    expect(snapshots).toEqual([])
    expect(subscription.resyncs).toBe(0)
    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-57"])
    expect(subscription.lastDurableSeq).toBe(57)

    // Once seeded, a contiguous commit continues without a resync.
    subscription.receiveDurable({ aggregateID: "s1", seq: 58, event: event("durable-58") })
    expect(subscription.resyncs).toBe(0)
    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-57", "durable-58"])

    // A real gap after the seed still heals at the missing position.
    subscription.receiveDurable({ aggregateID: "s1", seq: 60, event: event("durable-60") })
    await settleSnapshotChain()
    expect(snapshotCalls).toEqual([59])
    expect(subscription.resyncs).toBe(1)
  })

  test("a first observed delta above 0 seeds its subscription sequence, never a snapshot", async () => {
    const deltas: DeltaDelivery[] = []
    const snapshotCalls: number[] = []
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: (_aggregateID, seq) => {
        snapshotCalls.push(seq)
        return Promise.resolve([])
      },
      onDelta: (delta) => deltas.push(delta),
      onDurable: () => undefined,
    })

    // Deltas are sequenced per subscription, so a subscription attached after
    // its stream started sees its first delta at a non-zero local sequence.
    subscription.receiveDelta({ aggregateID: "s1", seq: 4, event: event("delta-4") })
    await settleSnapshotChain()

    expect(snapshotCalls).toEqual([])
    expect(subscription.resyncs).toBe(0)
    expect(deltas.map((delta) => delta.event.id)).toEqual(["delta-4"])
    expect(subscription.lastDeltaSeq).toBe(4)
  })

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

    subscription.receiveDurable({ aggregateID: "s1", seq: 0, event: event("durable-0") })
    subscription.receiveDelta({ aggregateID: "s1", seq: 0, event: event("d0") })
    // Delta seq 1 is lost in transit; the next delivered delta is seq 2.
    subscription.receiveDelta({ aggregateID: "s1", seq: 2, event: event("d2") })
    await settleSnapshotChain()

    expect(resyncs).toEqual([{ type: "resync", aggregateID: "s1", seq: 0 }])
    expect(snapshotCalls).toEqual([["s1", 0]])
    expect(snapshots).toHaveLength(1)
    expect(snapshots[0]?.events.map((entry) => entry.id)).toEqual(["snapshot"])
    expect(deltas.map((delta) => delta.event.id)).toEqual(["d0"])
    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-0"])

    // Sequence resumes after the gapped frame was observed.
    subscription.receiveDelta({ aggregateID: "s1", seq: 3, event: event("d3") })
    expect(deltas.map((delta) => delta.event.id)).toEqual(["d0", "d3"])
    expect(subscription.lastDeltaSeq).toBe(3)
    expect(subscription.lastDurableSeq).toBe(0)
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

    subscription.receiveDurable({ aggregateID: "s1", seq: 0, event: event("durable-0") })
    subscription.receiveDelta({ aggregateID: "s1", seq: 0, event: event("delta-0") })
    subscription.receiveDelta({ aggregateID: "s1", seq: 1, event: event("delta-1") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 1, event: event("durable-1") })
    // A replayed durable at or below the durable cursor is idempotent.
    subscription.receiveDurable({ aggregateID: "s1", seq: 1, event: event("durable-1-replay") })

    expect(deltas.map((delta) => delta.event.id)).toEqual(["delta-0", "delta-1"])
    expect(durables.map((durable) => durable.event.id)).toEqual(["durable-0", "durable-1"])
    expect(subscription.lastDeltaSeq).toBe(1)
    expect(subscription.lastDurableSeq).toBe(1)
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

    subscription.receiveDurable({ aggregateID: "s1", seq: 0, event: event("durable-0") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 1, event: event("durable-1") })
    // Durable 2 is lost; 3 and 4 arrive while the healing snapshot is in flight.
    subscription.receiveDurable({ aggregateID: "s1", seq: 3, event: event("durable-3") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 4, event: event("durable-4") })

    expect(delivered).toEqual(["durable-0", "durable-1"])
    await settleSnapshotChain()
    // Healing at `seq - 1` includes the skipped durable 2, not just the last
    // already-delivered position 1.
    expect(snapshotCalls).toEqual([2])

    releaseSnapshot()
    await settleSnapshotChain()
    expect(delivered).toEqual(["durable-0", "durable-1", "durable-3", "durable-4"])
  })

  test("a throwing durable handler does not poison later snapshots or deliveries", async () => {
    const delivered: string[] = []
    const snapshotCalls: number[] = []
    const errors: string[] = []
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: async (_aggregateID, seq) => {
        snapshotCalls.push(seq)
        return [event(`snapshot-${seq}`)]
      },
      onDelta: () => undefined,
      onDurable: (durable) => {
        delivered.push(durable.event.id)
        if (durable.event.id === "boom") throw new Error("durable handler exploded")
      },
      onError: (error) => errors.push(error.message),
    })

    // The first durable seeds the cursor, so it delivers without a snapshot and
    // its throwing handler is reported once.
    subscription.receiveDurable({ aggregateID: "s1", seq: 2, event: event("boom") })
    await settleSnapshotChain()
    expect(errors).toEqual(["durable handler exploded"])

    // A later gap must still heal: the failed link must not poison the chain.
    subscription.receiveDurable({ aggregateID: "s1", seq: 5, event: event("after") })
    await settleSnapshotChain()

    expect(snapshotCalls).toEqual([4])
    expect(delivered).toEqual(["boom", "after"])
    expect(subscription.healing).toBe(0)
  })

  test("a throwing snapshot callback still decrements `healing`", async () => {
    const subscription = createDeltaSubscription({
      aggregateID: "s1",
      snapshot: async () => {
        throw new Error("snapshot failed")
      },
      onDelta: () => undefined,
      onDurable: (durable) => {
        if (durable.event.id === "second") throw new Error("still alive")
      },
      onError: (error) => {
        throw new Error(`onError exploded: ${error.message}`)
      },
    })

    // Seed the cursor, then force a gap so a snapshot is actually requested.
    // The snapshot rejects and `onError` itself throws: neither may reject the
    // chain or leak the `healing` counter.
    subscription.receiveDurable({ aggregateID: "s1", seq: 0, event: event("seed") })
    subscription.receiveDurable({ aggregateID: "s1", seq: 3, event: event("first") })
    await settleSnapshotChain()
    expect(subscription.healing).toBe(0)
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

  test("reads the zero cursor as a real position", async () => {
    const calls: Array<readonly unknown[]> = []
    const reader = createEventLogSnapshotReader(async (_method, _sql, params) => {
      calls.push(params)
      return [{ id: "e0", type: "message.updated", data: { id: "e0" } }]
    })
    expect(await reader("s1", 0)).toEqual([{ id: "e0", type: "message.updated", properties: { id: "e0" } }])
    expect(calls).toEqual([["s1", 0]])
  })

  test("returns no rows for a before-first cursor without querying", async () => {
    let queries = 0
    const reader = createEventLogSnapshotReader(async () => {
      queries++
      return []
    })
    expect(await reader("s1", -1)).toEqual([])
    expect(queries).toBe(0)
  })
})
