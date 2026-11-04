import { describe, expect, test } from "bun:test"
import { Cause, Context, Effect, Exit, Layer } from "effect"
import { makeGlobalNode, makeLocationNode, Node } from "@opencode-ai/core/effect/app-node"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Location } from "@opencode-ai/core/location"
import {
  buildLocationServiceMapFrom,
  buildLocationServiceMapNode,
  isWorkerLocationServiceMapNode,
  LocationServiceMap,
} from "@opencode-ai/core/location-services"
import { AbsolutePath } from "@opencode-ai/core/schema"

const Settings = Context.Service<{ readonly name: string }>("test/location-services-map/Settings")
const Greeting = Context.Service<{ readonly text: string }>("test/location-services-map/Greeting")
const Counter = Context.Service<{ readonly id: number }>("test/location-services-map/Counter")
const Provided = Context.Service<{ readonly name: string }>("test/location-services-map/Provided")
const Consumed = Context.Service<{ readonly text: string }>("test/location-services-map/Consumed")
const Other = Context.Service<{ readonly other: boolean }>("test/location-services-map/Other")

const settingsNode = makeGlobalNode({
  service: Settings,
  layer: Layer.succeed(Settings, { name: "original" }),
  deps: [],
})

const greetingNode = makeLocationNode({
  service: Greeting,
  layer: Layer.effect(
    Greeting,
    Effect.map(Settings, (settings) => ({ text: `hello ${settings.name}` })),
  ),
  deps: [settingsNode],
})

const ref = (directory: string) => Location.Ref.make({ directory: AbsolutePath.make(directory) })

type MapLayer = Layer.Layer<LocationServiceMap.Service>

const readGreeting = (map: MapLayer, directory: string): Effect.Effect<string> =>
  Effect.gen(function* () {
    const locations = yield* LocationServiceMap.Service
    return yield* Effect.gen(function* () {
      return (yield* Greeting).text
    }).pipe(Effect.provide(locations.get(ref(directory))), Effect.scoped)
  }).pipe(Effect.provide(map)) as unknown as Effect.Effect<string>

const greetingFailure = (map: MapLayer, directory: string): Effect.Effect<Exit.Exit<any, any>> =>
  Effect.gen(function* () {
    const locations = yield* LocationServiceMap.Service
    return yield* Effect.gen(function* () {
      return yield* Greeting
    }).pipe(Effect.provide(locations.get(ref(directory))), Effect.scoped)
  }).pipe(Effect.provide(map), Effect.exit) as unknown as Effect.Effect<Exit.Exit<any, any>>

const failureText = (exit: Exit.Exit<any, any>) =>
  Exit.isFailure(exit) ? Cause.prettyErrors(exit.cause).join("\n") : ""

describe("buildLocationServiceMapFrom", () => {
  test("builds a location map from a simple node group", async () => {
    const map = buildLocationServiceMapFrom([settingsNode, greetingNode])

    expect(await Effect.runPromise(readGreeting(map, "/tmp/location-services-map/simple"))).toBe("hello original")
  })

  test("uses a per-node replacement instead of the original", async () => {
    const map = buildLocationServiceMapFrom(
      [settingsNode, greetingNode],
      [[settingsNode, Layer.succeed(Settings, { name: "replaced" })]],
    )

    expect(await Effect.runPromise(readGreeting(map, "/tmp/location-services-map/replacement"))).toBe("hello replaced")
  })

  test("reuses services for structurally equal location refs", async () => {
    let builds = 0
    const counterNode = makeLocationNode({
      service: Counter,
      layer: Layer.effect(
        Counter,
        Effect.sync(() => {
          builds += 1
          return { id: builds }
        }),
      ),
      deps: [],
    })
    const map = buildLocationServiceMapFrom([counterNode])

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const locations = yield* LocationServiceMap.Service
        const first = (yield* locations.contextEffect(
          ref("/tmp/location-services-map/reuse"),
        )) as unknown as Context.Context<{ readonly id: number }>
        const second = (yield* locations.contextEffect(
          ref("/tmp/location-services-map/reuse"),
        )) as unknown as Context.Context<{ readonly id: number }>

        return {
          builds,
          firstID: Context.get(first, Counter).id,
          sameService: Context.get(first, Counter) === Context.get(second, Counter),
        }
      }).pipe(Effect.scoped, Effect.provide(map)) as unknown as Effect.Effect<{
        readonly builds: number
        readonly firstID: number
        readonly sameService: boolean
      }>,
    )

    expect(result).toEqual({ builds: 1, firstID: 1, sameService: true })
  })

  test("provides locationContext services to the location map", async () => {
    const consumedNode: LayerNode.Node<
      { readonly text: string },
      never,
      typeof Node.tags.values.location
    > = {
      kind: "layer",
      name: Consumed.key,
      service: Consumed,
      implementation: Layer.effect(
        Consumed,
        Effect.map(Provided, (provided) => ({ text: `from ${provided.name}` })),
      ),
      dependencies: [],
      tag: Node.tags.values.location,
    }
    const context = Layer.succeed(Provided, { name: "context" }) as Layer.Layer<unknown>
    const map = buildLocationServiceMapFrom([consumedNode], [], context)

    const text = await Effect.runPromise(
      Effect.gen(function* () {
        const locations = yield* LocationServiceMap.Service
        return yield* Effect.gen(function* () {
          return (yield* Consumed).text
        }).pipe(Effect.provide(locations.get(ref("/tmp/location-services-map/context"))), Effect.scoped)
      }).pipe(Effect.provide(map)) as unknown as Effect.Effect<string>,
    )

    expect(text).toBe("from context")
  })

  test("fails loudly when a replacement name does not match its source", async () => {
    const otherNode = makeGlobalNode({
      service: Other,
      layer: Layer.succeed(Other, { other: true }),
      deps: [],
    })
    const map = buildLocationServiceMapFrom([settingsNode, greetingNode], [[settingsNode, otherNode]])

    const exit = await Effect.runPromise(greetingFailure(map, "/tmp/location-services-map/name-mismatch"))

    expect(Exit.isFailure(exit)).toBe(true)
    expect(failureText(exit)).toContain("Cannot replace")
  })

  test("fails loudly when a replacement tag does not match its source", async () => {
    const wrongTagNode = makeLocationNode({
      service: Settings,
      layer: Layer.succeed(Settings, { name: "wrong tag" }),
      deps: [],
    })
    const map = buildLocationServiceMapFrom([settingsNode, greetingNode], [[settingsNode, wrongTagNode]])

    const exit = await Effect.runPromise(greetingFailure(map, "/tmp/location-services-map/tag-mismatch"))

    expect(Exit.isFailure(exit)).toBe(true)
    expect(failureText(exit)).toContain("Cannot replace")
  })
})

describe("buildLocationServiceMapNode", () => {
  test("exposes metadata and the worker brand", () => {
    const replacements: LayerNode.Replacements = [
      [settingsNode, Layer.succeed(Settings, { name: "replacement" })],
    ]
    const context = Layer.succeed(Provided, { name: "context" }) as Layer.Layer<unknown>
    const node = buildLocationServiceMapNode([settingsNode, greetingNode], replacements, context, {
      idleTimeToLive: "5 minutes",
    })

    expect(node.kind).toBe("layer")
    expect(node.name).toBe(LocationServiceMap.node.name)
    expect(node.service).toBe(LocationServiceMap.Service)
    expect(node.tag).toBe(LocationServiceMap.node.tag)
    expect(node.dependencies).toEqual([])
    expect(node.locationNodes).toEqual([settingsNode, greetingNode])
    expect(node.locationReplacements).toBe(replacements)
    expect(node.locationContext).toBe(context)
    expect(isWorkerLocationServiceMapNode(node)).toBe(true)

    expect(isWorkerLocationServiceMapNode(null)).toBe(false)
    expect(isWorkerLocationServiceMapNode(undefined)).toBe(false)
    expect(isWorkerLocationServiceMapNode({})).toBe(false)
    expect(isWorkerLocationServiceMapNode({ kind: "layer", name: node.name })).toBe(false)
  })
})
