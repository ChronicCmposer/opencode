// ws-hub item 5.9 — worker durable wakeups. A hub-delivered durable commit
// (carried over the egress control plane) is bridged into the worker's
// in-isolate EventV2 so its subscribers observe the cross-isolate wakeup.
//
// The commit already durably committed in the hub writer, so the bridge
// republishes it LIVE-ONLY (never re-commits) and verbatim (`{ id, type, data,
// durable }`, where `data` is the EventV2 `properties` reference and `durable`
// is the committed `{ aggregateID, seq, version }` identity).

import { describe, expect, test } from "bun:test"
import { Effect, Layer, ManagedRuntime } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { durableLogStub } from "../../lib/durable-log-stub"
import { Database } from "@opencode-ai/core/database/database"
import { SessionV1 } from "@opencode-ai/schema/session-v1"
import { attachMeshWorker, createEgressMesh, type DurableCommit, type MeshWorkerClient } from "@/session/egress/mesh"
import { createWorkerEgressClient } from "@/session/worker/egress-client"
import { DURABLE_WAKEUP_METADATA_KEY, createWorkerEventBridge } from "@/session/worker/event-bridge"
import {
  WORKER_DURABLE_WAKE_PATTERN,
  republishDurableCommit,
  wireDurableWakeups,
} from "@/session/worker/layer"
import { pollUntil } from "../../lib/poll"

/** Publishing live-only never reads `db`, so a stub is enough to build the layer. */
const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(durableLogStub), Layer.provide(databaseStub))

/** A real durable event type: `durable.aggregate === "sessionID"`. */
const DURABLE_TYPE = SessionV1.Event.MessageRemoved.type
const sessionID = "ses_durable_wake"

const commit = (seq: number, type: string = DURABLE_TYPE): DurableCommit => ({
  meta: { aggregateID: sessionID, seq },
  event: { id: `evt_${seq}`, type, properties: { sessionID, ordinal: seq } },
})

/** A recording stub for the two seams `republishDurableCommit` drives. */
function recordingEvents(): {
  readonly events: Pick<EventV2.Interface, "publishLive" | "wakeDurable">
  readonly published: EventV2.Payload[]
  readonly woken: string[]
} {
  const published: EventV2.Payload[] = []
  const woken: string[] = []
  return {
    events: {
      publishLive: (event) => Effect.sync(() => void published.push(event)),
      wakeDurable: (aggregateID) => Effect.sync(() => void woken.push(aggregateID)),
    },
    published,
    woken,
  }
}

describe("republishDurableCommit", () => {
  test("republishes the committed event live-only and wakes its committed aggregate", () => {
    const { events, published, woken } = recordingEvents()

    republishDurableCommit(events, commit(3))

    expect(published).toHaveLength(1)
    expect(published[0]).toMatchObject({ id: "evt_3", type: DURABLE_TYPE, data: { sessionID, ordinal: 3 } })
    // Live-only: the republish never re-commits, but the committed durable
    // identity crosses unchanged so the isolate can locate the exact position.
    expect(published[0].durable).toEqual({ aggregateID: sessionID, seq: 3, version: 1 })
    // The republish is marked so the worker bridge never re-forwards it (the mesh
    // delivers the worker its own commit back).
    expect(published[0].metadata).toEqual({ [DURABLE_WAKEUP_METADATA_KEY]: true })
    // The durable write already landed in the hub writer; the isolate's durable()
    // subscribers are woken for the committed aggregate.
    expect(woken).toEqual([sessionID])
  })

  test("a handle without the wake seam republishes live-only and skips the wake", () => {
    const published: EventV2.Payload[] = []

    republishDurableCommit({ publishLive: (event) => Effect.sync(() => void published.push(event)) }, commit(4))

    expect(published.map((event) => event.type)).toEqual([DURABLE_TYPE])
  })

  test("a commit whose type names no durable definition skips the wake", () => {
    const { events, published, woken } = recordingEvents()

    republishDurableCommit(events, commit(5, "not.a.durable.event"))

    // The event still crosses live-only; only the per-aggregate wake is skipped.
    expect(published.map((event) => event.type)).toEqual(["not.a.durable.event"])
    expect(woken).toEqual([])
  })

  test("a known durable commit missing its aggregate field fails loud", () => {
    const { events } = recordingEvents()
    const malformed: DurableCommit = {
      meta: { aggregateID: sessionID, seq: 6 },
      event: { id: "evt_6", type: DURABLE_TYPE, properties: { ordinal: 6 } },
    }

    expect(() => republishDurableCommit(events, malformed)).toThrow(/missing string aggregate field/)
  })
})

describe("wireDurableWakeups", () => {
  test("subscribes to the whole durable stream and republishes each delivery", () => {
    const { events, published, woken } = recordingEvents()
    const subscriptions: string[] = []
    const handlers: ((commit: DurableCommit) => void)[] = []
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
    expect(WORKER_DURABLE_WAKE_PATTERN).toBe("*")

    handlers[0](commit(1))
    expect(published.map((event) => event.type)).toEqual([DURABLE_TYPE])
    expect(woken).toEqual([sessionID])

    unsubscribe()
    expect(unsubscribed).toBe(true)
  })

  test("a reduced handle with no subscription surface is a no-op", () => {
    const { events } = recordingEvents()
    const unsubscribe = wireDurableWakeups(events, {})

    expect(unsubscribe).toBeInstanceOf(Function)
    expect(() => unsubscribe()).not.toThrow()
  })
})

describe("worker durable wakeups end-to-end (mesh → EventV2)", () => {
  test("a hub mesh durable commit reaches the worker's EventV2 subscribers", async () => {
    const runtime = ManagedRuntime.make(eventLayer)
    const data = new MessageChannel()
    const control = new MessageChannel()
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    let disposeWakeups: (() => void) | undefined
    try {
      const events = runtime.runSync(EventV2.Service)
      const received: string[] = []
      runtime.runSync(events.listen((event) => Effect.sync(() => void received.push(event.type))))

      disposeWakeups = wireDurableWakeups(events, client)
      await pollUntil(() => mesh.subscriptions().length === 1, { message: "worker never subscribed to the mesh" })

      mesh.publish(commit(1))

      await pollUntil(() => received.length === 1, { message: "durable commit never reached EventV2" })
      expect(received).toEqual([DURABLE_TYPE])
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

  // Regression for the flag-ON duplicate-event bug: the mesh delivers a worker its
  // OWN durable commit back (pattern `"*"`), `republishDurableCommit` republishes it
  // into the isolate, and the worker bridge must NOT re-forward it to the hub —
  // otherwise the hub live-republishes the same part/text update a second time.
  test("a wakeup republish reaches EventV2 but is never re-forwarded by the worker bridge", async () => {
    const runtime = ManagedRuntime.make(eventLayer)
    const data = new MessageChannel()
    const control = new MessageChannel()
    const client = createWorkerEgressClient(data.port1, { controlPort: control.port1 })
    const mesh = createEgressMesh()
    const attachment = attachMeshWorker(mesh, "w1", control.port2)
    let disposeWakeups: (() => void) | undefined
    let bridge: { readonly dispose: Effect.Effect<void> } | undefined
    try {
      const events = runtime.runSync(EventV2.Service)
      const received: string[] = []
      runtime.runSync(events.listen((event) => Effect.sync(() => void received.push(event.type))))
      const forwarded: string[] = []
      bridge = runtime.runSync(
        createWorkerEventBridge(events, { emit: (event) => void forwarded.push(event.type) }),
      )

      disposeWakeups = wireDurableWakeups(events, client)
      await pollUntil(() => mesh.subscriptions().length === 1, { message: "worker never subscribed to the mesh" })

      mesh.publish(commit(2))

      await pollUntil(() => received.length === 1, { message: "durable commit never reached EventV2" })
      expect(received).toEqual([DURABLE_TYPE])
      // The republish is marked, so it never crosses the egress bridge again.
      expect(forwarded).toEqual([])
    } finally {
      disposeWakeups?.()
      if (bridge !== undefined) runtime.runSync(bridge.dispose)
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
