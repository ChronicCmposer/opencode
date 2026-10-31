import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "../src/event-v2-bridge"
import { GlobalBus, type GlobalEvent } from "../src/bus/global"
import type { InstanceContext } from "../src/project/instance-context"
import { stringifySpliced } from "../src/util/lazy-json-splice"
import { CountingLazyJson } from "./lib/counting-lazy-json"
import { testEffect } from "./lib/effect"

const ctx = (directory: string) =>
  ({ directory, worktree: directory, project: { id: "proj" } }) as unknown as InstanceContext

describe("instanceLocation", () => {
  test("returns the cached Location.Info per instance context and workspace", () => {
    const a = ctx("/tmp/a")
    const first = EventV2Bridge.instanceLocation(a)
    expect(EventV2Bridge.instanceLocation(a)).toBe(first)
    const ws = "wrk_1" as never
    const scoped = EventV2Bridge.instanceLocation(a, ws)
    expect(scoped).not.toBe(first)
    expect(EventV2Bridge.instanceLocation(a, ws)).toBe(scoped)
    expect(EventV2Bridge.instanceLocation(ctx("/tmp/a"))).not.toBe(first)
  })

  test("returns undefined without a project", () => {
    expect(EventV2Bridge.instanceLocation(undefined)).toBeUndefined()
  })
})

const it = testEffect(Layer.mergeAll(LayerNode.compile(LayerNode.group([EventV2Bridge.node]))))

describe("bridge GlobalBus envelopes", () => {
  const seen: GlobalEvent[] = []
  const handler = (event: GlobalEvent) => void seen.push(event)
  afterEach(() => {
    GlobalBus.off("event", handler)
    seen.length = 0
  })

  it.live("forwards LazyJson data untouched and serialises like the plain path", () =>
    Effect.gen(function* () {
      const events = yield* EventV2Bridge.Service
      GlobalBus.on("event", handler)
      const data = { text: "é   $&", list: [1, { a: null }] }
      const lazy = new CountingLazyJson(data)
      const durable = { aggregateID: "agg", seq: 3, version: 1 }
      yield* events.publishLive({ id: EventV2.ID.create(), type: "test.lazy", data: lazy, durable })
      yield* events.publishLive({ id: EventV2.ID.create(), type: "test.lazy", data, durable })

      const mine = seen.filter((event) => event.payload.type === "test.lazy" || event.payload.type === "sync")
      expect(mine).toHaveLength(4)
      expect(mine[0].payload.properties).toBe(lazy)
      expect(mine[1].payload.syncEvent.data).toBe(lazy)
      expect(lazy.reads).toBe(0)

      const paths = [
        ["payload", "properties"],
        ["payload", "syncEvent", "data"],
      ]
      const strip = (event: GlobalEvent) => {
        const { id: _id, ...payload } = event.payload
        const { id: _sid, ...syncEvent } = payload.syncEvent ?? {}
        return { ...event, payload: { ...payload, ...(payload.syncEvent ? { syncEvent } : {}) } }
      }
      expect(JSON.parse(stringifySpliced(strip(mine[0]), paths))).toEqual(JSON.parse(JSON.stringify(strip(mine[2]))))
      expect(JSON.parse(stringifySpliced(strip(mine[1]), paths))).toEqual(JSON.parse(JSON.stringify(strip(mine[3]))))
      expect(lazy.reads).toBe(0)
      expect(lazy.serialised).toBe(0)
    }),
  )
})
