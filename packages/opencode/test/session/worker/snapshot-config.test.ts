// The worker's snapshot-backed Config must expose the hub's directory roots.
//
// Skill/agent/command discovery scans `config.directories()` (see
// `src/skill/index.ts`). Before this seam shipped the hub's roots, a worker's
// Config returned only `[directory]`, so discovery never saw
// `$OPENCODE_CONFIG_DIR` / `~/.config/opencode` and workers resolved only the
// built-in skills. These tests pin both halves of the contract.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Config } from "@/config/config"
import { snapshotConfigLayer, type WorkerSnapshot } from "@/session/worker/layer"

/** Minimal dependency-free snapshot; only the directories seam is under test. */
const snapshot = (
  overrides: Partial<WorkerSnapshot> & { directories?: readonly string[] } = {},
): WorkerSnapshot =>
  ({
    epoch: 1,
    directory: "/tmp/project-a",
    config: {},
    auth: {},
    modelsDev: {},
    flags: {},
    workerCount: 1,
    ...overrides,
  }) as WorkerSnapshot

const directoriesOf = (value: WorkerSnapshot): Promise<string[]> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const config = yield* Config.Service
      return yield* config.directories()
    }).pipe(Effect.provide(snapshotConfigLayer(value))),
  )

describe("snapshotConfigLayer directories", () => {
  test("serves the hub's shipped directories verbatim", async () => {
    const directories = await directoriesOf(
      snapshot({ directories: ["/cfg/opencode", "/proj/.opencode"] }),
    )
    expect(directories).toEqual(["/cfg/opencode", "/proj/.opencode"])
  })

  test("falls back to [directory] when the snapshot ships no directories", async () => {
    const directories = await directoriesOf(snapshot({ directory: "/tmp/project-a" }))
    expect(directories).toEqual(["/tmp/project-a"])
  })
})
