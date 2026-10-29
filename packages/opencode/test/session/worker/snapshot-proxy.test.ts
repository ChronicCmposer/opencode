import { afterEach, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Effect, Layer } from "effect"
import path from "node:path"
import { Snapshot } from "@/snapshot"
import { buildHubProxyHandlers } from "@/session/worker/hub"
import { snapshotProxyLayer } from "@/session/worker/layer"
import { disposeAllInstances, testInstanceStoreLayer, TestInstance } from "../../fixture/fixture"
import { testEffect } from "../../lib/effect"
import { recordingHubAuthorities, serveHubProxyFor } from "../../lib/hub-proxy"

const it = testEffect(
  Layer.mergeAll(LayerNode.compile(LayerNode.group([Snapshot.node, FSUtil.node])), testInstanceStoreLayer),
)

afterEach(async () => {
  await disposeAllInstances()
})

const workerSnapshot = Effect.fn("SnapshotProxyTest.workerSnapshot")(function* () {
  const hub = yield* Snapshot.Service
  const context = yield* Effect.context<never>()
  const served = serveHubProxyFor(
    buildHubProxyHandlers({ ...recordingHubAuthorities().authorities, snapshot: hub }, (effect) =>
      Effect.runPromise(Effect.provide(effect, context)),
    ),
  )
  yield* Effect.addFinalizer(() => Effect.sync(() => served.dispose()))
  return yield* Effect.service(Snapshot.Service).pipe(Effect.provide(snapshotProxyLayer(served.client)))
})

it.instance(
  "worker snapshot proxy tracks, diffs and reverts through the hub",
  () =>
  Effect.gen(function* () {
    const tmp = yield* TestInstance
    const file = path.join(tmp.directory, "a.txt")
    const fs = yield* FSUtil.Service
    yield* fs.writeWithDirs(file, "before")
    const snapshot = yield* workerSnapshot()

    const before = yield* snapshot.track()
    expect(before).toBeTruthy()
    yield* fs.writeWithDirs(file, "after")

    const patch = yield* snapshot.patch(before!)
    expect(patch.files.map((item) => path.basename(item))).toEqual(["a.txt"])

    const after = yield* snapshot.track()
    const diffs = yield* snapshot.diffFull(before!, after!)
    expect(diffs.map((item) => item.file)).toEqual(["a.txt"])
    expect(diffs[0]?.additions).toBeGreaterThan(0)

    yield* snapshot.revert([patch])
    expect(yield* fs.readFileString(file)).toBe("before")
  }),
  { git: true },
)

it.instance(
  "concurrent worker snapshots in one project serialize through the hub",
  () =>
    Effect.gen(function* () {
      const tmp = yield* TestInstance
      const fs = yield* FSUtil.Service
      yield* fs.writeWithDirs(path.join(tmp.directory, "a.txt"), "one")
      const first = yield* workerSnapshot()
      const second = yield* workerSnapshot()
      const hashes = yield* Effect.all([first.track(), second.track(), first.track(), second.track()], {
        concurrency: "unbounded",
      })
      expect(hashes.every((hash) => hash !== undefined)).toBe(true)
      expect(new Set(hashes).size).toBe(1)
    }),
  { git: true },
)
