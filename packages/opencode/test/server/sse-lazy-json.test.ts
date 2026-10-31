import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer, Queue, Stream } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { GlobalBus } from "../../src/bus/global"
import { EventPaths } from "../../src/server/routes/instance/httpapi/groups/event"
import { GlobalPaths } from "../../src/server/routes/instance/httpapi/groups/global"
import { stringifySpliced } from "../../src/util/lazy-json-splice"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { CountingLazyJson } from "../lib/counting-lazy-json"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

const properties = { text: 'quote " \\ and   é $& $1', nested: { list: [1, 2.5, null, true], empty: {} } }

type Reader = { queue: Queue.Dequeue<string> }

const frames = (reader: Reader) =>
  Queue.take(reader.queue).pipe(
    Effect.timeoutOrElse({ duration: "5 seconds", orElse: () => Effect.fail(new Error("timed out")) }),
  )

const open = (path: string, directory: string) =>
  Effect.gen(function* () {
    const response = yield* requestInDirectory(path, directory)
    const queue = yield* Queue.unbounded<string>()
    let buffer = ""
    yield* response.stream.pipe(
      Stream.runForEach((value) =>
        Effect.gen(function* () {
          buffer += new TextDecoder().decode(value)
          const parts = buffer.split("\n\n")
          buffer = parts.pop() ?? ""
          for (const part of parts) yield* Queue.offer(queue, part.replace(/^data: /, ""))
        }),
      ),
      Effect.forkScoped,
    )
    return { queue }
  })

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

const it = testEffect(Layer.mergeAll(LayerNode.compile(LayerNode.group([EventV2Bridge.node])), httpApiLayer))

describe("stringifySpliced", () => {
  test("matches JSON.stringify for lazy and plain values", () => {
    const lazy = new CountingLazyJson(properties)
    const spliced = stringifySpliced({ a: 1, payload: { id: "x", properties: lazy, z: 2 } }, [["payload", "properties"]])
    expect(spliced).toBe(JSON.stringify({ a: 1, payload: { id: "x", properties, z: 2 } }))
    expect(lazy.reads + lazy.serialised).toBe(0)
    const plain = { payload: { properties } }
    expect(stringifySpliced(plain, [["payload", "properties"]])).toBe(JSON.stringify(plain))
  })

  test("splices every listed path and skips absent ones", () => {
    const a = new CountingLazyJson({ a: 1 })
    const b = new CountingLazyJson([2])
    const out = stringifySpliced({ payload: { properties: a, syncEvent: { data: b } } }, [
      ["payload", "properties"],
      ["payload", "syncEvent", "data"],
      ["missing", "path"],
    ])
    expect(JSON.parse(out)).toEqual({ payload: { properties: { a: 1 }, syncEvent: { data: [2] } } })
    expect(a.reads + b.reads).toBe(0)
  })

  test("a nested lazy outside the known paths still serialises as its value", () => {
    const lazy = new CountingLazyJson({ a: 1 })
    expect(stringifySpliced({ other: lazy }, [["properties"]])).toBe('{"other":{"a":1}}')
  })
})

describe("SSE with LazyJson data", () => {
  it.instance(
    "instance /event emits the same bytes as the plain path without reading value",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const events = yield* EventV2Bridge.Service
        const reader = yield* open(EventPaths.event, directory)
        yield* frames(reader)
        const location = new Location.Info({
          directory: AbsolutePath.make(directory),
          project: { id: "p" as never, directory: AbsolutePath.make(directory) },
        })
        const lazy = new CountingLazyJson(properties)
        yield* events.publishLive({ id: EventV2.ID.create(), type: "test.lazy", data: lazy, location })
        const lazyFrame = yield* frames(reader)
        yield* events.publishLive({ id: EventV2.ID.create(), type: "test.lazy", data: properties, location })
        const plainFrame = yield* frames(reader)
        const a = JSON.parse(lazyFrame)
        const b = JSON.parse(plainFrame)
        expect(a.properties).toEqual(b.properties)
        expect(a.properties).toEqual(properties)
        // Plugins with an event hook share one parse; the SSE path itself never reads or re-stringifies.
        expect(lazy.reads).toBeLessThanOrEqual(1)
        expect(lazy.serialised).toBe(0)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "/global/event splices properties and syncEvent.data",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const reader = yield* open(GlobalPaths.event, directory)
        yield* frames(reader)
        const props = new CountingLazyJson(properties)
        const data = new CountingLazyJson({ k: ["v"] })
        GlobalBus.emit("event", { directory, payload: { id: "e1", type: "test.lazy", properties: props } })
        GlobalBus.emit("event", {
          directory,
          payload: { id: "e2", type: "sync", syncEvent: { id: "e2", type: "t.1", seq: 1, aggregateID: "a", data } },
        })
        expect(JSON.parse(yield* frames(reader))).toEqual({
          directory,
          payload: { id: "e1", type: "test.lazy", properties },
        })
        expect(JSON.parse(yield* frames(reader))).toEqual({
          directory,
          payload: { id: "e2", type: "sync", syncEvent: { id: "e2", type: "t.1", seq: 1, aggregateID: "a", data: { k: ["v"] } } },
        })
        expect(props.reads + data.reads + props.serialised + data.serialised).toBe(0)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
