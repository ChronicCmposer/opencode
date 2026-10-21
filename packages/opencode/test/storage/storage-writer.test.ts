// Phase 7.13 — `Storage` routes through the single writer when the writer port is
// present (plan task 7.13, decision line 38).
//
// The default (flag off / no port) path is unchanged and covered by
// `storage.test.ts`. Here we prove that, when a `Storage.WriterService` is
// present, every mutation and read proxies through it and never touches the
// filesystem directly. The port is provided at layer construction (the layer
// resolves it once), so the tests build the storage graph with it in place.

import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect, Layer } from "effect"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Storage } from "@/storage/storage"
import { testEffect } from "../lib/effect"

/** An in-memory `Storage.WriterPort` that records the calls it receives. */
function spyWriter() {
  const calls: string[] = []
  const store = new Map<string, unknown>()
  const port: Storage.WriterPort = {
    read: (key) =>
      Effect.sync(() => {
        const id = key.join("/")
        return store.has(id) ? { found: true, content: store.get(id) } : { found: false }
      }),
    update: <T>(key: ReadonlyArray<string>, fn: (draft: T) => void) =>
      Effect.sync(() => {
        const id = key.join("/")
        if (!store.has(id)) return { found: false as const }
        calls.push(`update:${id}`)
        const content = store.get(id) as T
        fn(content)
        return { found: true as const, content }
      }),
    write: (key, content) =>
      Effect.sync(() => {
        calls.push(`write:${key.join("/")}`)
        store.set(key.join("/"), content)
      }),
    remove: (key) =>
      Effect.sync(() => {
        calls.push(`remove:${key.join("/")}`)
        store.delete(key.join("/"))
      }),
    list: (prefix) =>
      Effect.sync(() =>
        [...store.keys()]
          .map((id) => id.split("/"))
          .filter((parts) => prefix.every((segment, index) => parts[index] === segment)),
      ),
  }
  const reset = () => {
    calls.length = 0
    store.clear()
  }
  return { port, calls, reset }
}

const spy = spyWriter()
const it = testEffect(
  LayerNode.compile(LayerNode.group([Storage.node, FSUtil.node, CrossSpawnSpawner.node])).pipe(
    Layer.provide(Layer.succeed(Storage.WriterService, spy.port)),
  ),
)

describe("Storage routes through the writer port (7.13)", () => {
  it.live("write/read/update/remove/list all proxy to the writer", () =>
    Effect.gen(function* () {
      spy.reset()
      const svc = yield* Storage.Service

      yield* svc.write(["session", "s1"], { count: 1 })
      const read = yield* svc.read<{ count: number }>(["session", "s1"])
      expect(read).toEqual({ count: 1 })

      const updated = yield* svc.update<{ count: number }>(["session", "s1"], (draft) => {
        draft.count += 1
      })
      expect(updated).toEqual({ count: 2 })

      expect(yield* svc.list(["session"])).toEqual([["session", "s1"]])

      yield* svc.remove(["session", "s1"])
      // `update` is a single atomic port call, not a read-then-write pair.
      expect(spy.calls).toEqual(["write:session/s1", "update:session/s1", "remove:session/s1"])
    }),
  )

  it.live("a missing key surfaces the same NotFoundError as the direct path", () =>
    Effect.gen(function* () {
      spy.reset()
      const svc = yield* Storage.Service
      const error = yield* svc.read(["missing", "key"]).pipe(Effect.flip)
      expect(Storage.NotFoundError.isInstance(error)).toBe(true)
    }),
  )
})
