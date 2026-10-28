// The build hash covers the EMITTED worker bundle/module graph, not
// the `session-worker.ts` source text. A change in a transitive import must
// change the hash; an unrelated plugin file must not.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { computeBundleHash, type BundleArtifact } from "@/session/worker/build-hash"

const FIXTURE = path.join(import.meta.dir, "../../fixture/build-hash")

/** Emit the fixture graph the way `script/build.ts` does, then hash its bytes. */
async function emittedHash(entrypoint: string): Promise<string> {
  const config: Parameters<typeof Bun.build>[0] & { write?: boolean } = {
    entrypoints: [entrypoint],
    target: "bun",
    conditions: ["bun", "node"],
    format: "esm",
    splitting: true,
    minify: true,
    write: false,
  }
  const result = await Bun.build(config)
  expect(result.success).toBe(true)
  const artifacts: BundleArtifact[] = await Promise.all(
    result.outputs.map(async (output) => ({
      path: output.path,
      bytes: new Uint8Array(await output.arrayBuffer()),
    })),
  )
  return computeBundleHash(artifacts)
}

describe("computeBundleHash over the emitted worker graph", () => {
  test("is deterministic across runs over the same emitted graph", async () => {
    const first = await emittedHash(path.join(FIXTURE, "entry.ts"))
    const second = await emittedHash(path.join(FIXTURE, "entry.ts"))
    expect(first).toMatch(/^[0-9a-f]{64}$/)
    expect(second).toBe(first)
  })

  test("a change in a transitive import changes the hash", async () => {
    const dep = path.join(FIXTURE, "dep.ts")
    const plugin = path.join(FIXTURE, "plugin.ts")
    const before = await emittedHash(path.join(FIXTURE, "entry.ts"))
    const original = await Bun.file(dep).text()
    const pluginOriginal = await Bun.file(plugin).text()
    try {
      await Bun.write(dep, 'export const value = "v2"\n')
      const afterDep = await emittedHash(path.join(FIXTURE, "entry.ts"))
      expect(afterDep).not.toBe(before)

      await Bun.write(plugin, 'export const plugin = "changed-but-not-imported"\n')
      const afterPlugin = await emittedHash(path.join(FIXTURE, "entry.ts"))
      expect(afterPlugin).toBe(afterDep)
    } finally {
      await Bun.write(dep, original)
      await Bun.write(plugin, pluginOriginal)
    }
  })
})
