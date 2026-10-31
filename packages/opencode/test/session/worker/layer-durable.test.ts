// Worker durable wakeups. The hub sends this worker only `{ aggregateID, seq }`
// position wakeups for its own session (no event bodies); the worker wakes the
// aggregate's `durable()` readers, which re-read SQLite, and keeps one gap
// tracker that heals a skipped sequence.

import { describe, expect, test } from "bun:test"
import { Effect, Layer, ManagedRuntime, Stream } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { durableLogStub } from "../../lib/durable-log-stub"
import { Database } from "@opencode-ai/core/database/database"
import { attachMeshWorker, createEgressMesh, type DurableWakeup, type MeshWorkerClient } from "@/session/egress/mesh"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { WORKER_DURABLE_WAKE_PATTERN, wireDurableWakeups } from "@/session/worker/layer"
import { pollUntil } from "../../lib/poll"

/** Publishing live-only never reads `db`, so a stub is enough to build the layer. */
const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(durableLogStub), Layer.provide(databaseStub))

const sessionID = "ses_durable_wake"

const wake = (seq: number): DurableWakeup => ({ aggregateID: sessionID, seq })

/** A recording stub for the seams a wakeup drives. */
function recordingEvents(): {
  readonly events: Pick<EventV2.Interface, "wakeDurable" | "durable">
  readonly woken: string[]
  readonly reads: Array<{ aggregateID: string; after?: number }>
} {
  const woken: string[] = []
  const reads: Array<{ aggregateID: string; after?: number }> = []
  return {
    events: {
      wakeDurable: (aggregateID) => Effect.sync(() => void woken.push(aggregateID)),
      durable: (input) => {
        reads.push({ aggregateID: input.aggregateID, ...(input.after === undefined ? {} : { after: input.after }) })
        return Stream.empty
      },
    },
    woken,
    reads,
  }
}

describe("wireDurableWakeups", () => {
  test("subscribes once and wakes the aggregate on each position wakeup", () => {
    const { events, woken } = recordingEvents()
    const subscriptions: string[] = []
    const handlers: ((wakeup: DurableWakeup) => void)[] = []
    let unsubscribed = false

    const unsubscribe = wireDurableWakeups(events, {
      // The bridge only reads the mesh's PRESENCE (a control plane exists), never
      // calls into it: the fake `subscribeDurable` below stands in for the real
      // control-plane client.
      mesh: {} as MeshWorkerClient,
      subscribeDurable: (subscription) => {
        subscriptions.push(subscription.pattern)
        handlers.push(subscription.onDurable)
        return () => {
          unsubscribed = true
        }
      },
    })

    expect(subscriptions).toEqual([WORKER_DURABLE_WAKE_PATTERN])
    expect(WORKER_DURABLE_WAKE_PATTERN).not.toBe("*")

    handlers[0](wake(1))
    expect(woken).toEqual([sessionID])

    unsubscribe()
    expect(unsubscribed).toBe(true)
  })

  test("a skipped sequence heals through the gap tracker", async () => {
    const { events, woken, reads } = recordingEvents()
    const handlers: ((wakeup: DurableWakeup) => void)[] = []
    wireDurableWakeups(events, {
      mesh: {} as MeshWorkerClient,
      subscribeDurable: (subscription) => {
        handlers.push(subscription.onDurable)
        return () => {}
      },
    })

    handlers[0](wake(1))
    handlers[0](wake(2))
    expect(woken).toHaveLength(2)

    handlers[0](wake(4))
    // Two wakeups plus the heal's wake for the skipped seq 3.
    expect(woken).toHaveLength(4)
    await pollUntil(() => reads.length === 1, { message: "gap never healed from the snapshot" })
    expect(reads[0]).toEqual({ aggregateID: sessionID, after: 2 })
  })

  test("a resync without a position wakes the tracked aggregate", () => {
    const { events, woken } = recordingEvents()
    const handlers: ((wakeup: DurableWakeup) => void)[] = []
    const resyncs: Array<(pattern: string, position?: DurableWakeup) => void> = []
    wireDurableWakeups(events, {
      mesh: {} as MeshWorkerClient,
      subscribeDurable: (subscription) => {
        handlers.push(subscription.onDurable)
        if (subscription.onResync) resyncs.push(subscription.onResync)
        return () => {}
      },
    })

    handlers[0](wake(1))
    woken.length = 0
    resyncs[0](WORKER_DURABLE_WAKE_PATTERN)
    expect(woken).toEqual([sessionID])
  })

  test("a reduced handle with no subscription surface is a no-op", () => {
    const { events } = recordingEvents()
    const unsubscribe = wireDurableWakeups(events, {})

    expect(unsubscribe).toBeInstanceOf(Function)
    expect(() => unsubscribe()).not.toThrow()
  })
})

describe("worker durable wakeups end-to-end (mesh → EventV2)", () => {
  test("a hub position wakeup wakes the worker's durable readers", async () => {
    const runtime = ManagedRuntime.make(eventLayer)
    const data = new MessageChannel()
    const control = new MessageChannel()
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    let disposeWakeups: (() => void) | undefined
    try {
      const events = runtime.runSync(EventV2.Service)
      const woken: string[] = []
      disposeWakeups = wireDurableWakeups({ ...events, wakeDurable: (id) => Effect.sync(() => void woken.push(id)) }, client)
      await pollUntil(() => mesh.subscriptions().length === 1, { message: "worker never subscribed to the mesh" })

      mesh.publish("w1", wake(1))

      await pollUntil(() => woken.length === 1, { message: "wakeup never reached the worker" })
      expect(woken).toEqual([sessionID])
      await pollUntil(() => mesh.outstanding("w1") === 0, { message: "wakeup was never acknowledged" })
    } finally {
      disposeWakeups?.()
      attachment.dispose()
      client.dispose()
      data.port1.close()
      data.port2.close()
      control.port1.close()
      control.port2.close()
      await runtime.dispose()
    }
  })
})
