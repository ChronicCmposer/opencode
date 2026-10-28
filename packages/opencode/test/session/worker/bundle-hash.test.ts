// The build hash covers the EMITTED worker bundle/module graph, not
// the `session-worker.ts` source text. A change in a transitive import must
// change the hash; an unrelated plugin file must not.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { hashWorkerBundle } from "@/session/worker/build-hash"

const FIXTURE = path.join(import.meta.dir, "../../fixture/build-hash")

describe("hashWorkerBundle", () => {
  test("is deterministic across runs over the same emitted graph", async () => {
    const first = await hashWorkerBundle({ entrypoint: path.join(FIXTURE, "entry.ts") })
    const second = await hashWorkerBundle({ entrypoint: path.join(FIXTURE, "entry.ts") })
    expect(first).toMatch(/^[0-9a-f]{64}$/)
    expect(second).toBe(first)
  })

  test("a change in a transitive import changes the hash", async () => {
    const dep = path.join(FIXTURE, "dep.ts")
    const plugin = path.join(FIXTURE, "plugin.ts")
    const before = await hashWorkerBundle({ entrypoint: path.join(FIXTURE, "entry.ts") })
    const original = await Bun.file(dep).text()
    const pluginOriginal = await Bun.file(plugin).text()
    try {
      await Bun.write(dep, 'export const value = "v2"\n')
      const afterDep = await hashWorkerBundle({ entrypoint: path.join(FIXTURE, "entry.ts") })
      expect(afterDep).not.toBe(before)

      await Bun.write(plugin, 'export const plugin = "changed-but-not-imported"\n')
      const afterPlugin = await hashWorkerBundle({ entrypoint: path.join(FIXTURE, "entry.ts") })
      expect(afterPlugin).toBe(afterDep)
    } finally {
      await Bun.write(dep, original)
      await Bun.write(plugin, pluginOriginal)
    }
  })
})
