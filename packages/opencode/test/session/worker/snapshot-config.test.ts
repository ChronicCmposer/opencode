// The worker's snapshot-backed Config must expose the hub's directory roots.
//
// Skill/agent/command discovery scans `config.directories()` (see
// `src/skill/index.ts`). Before this seam shipped the hub's roots, a worker's
// Config returned only `[directory]`, so discovery never saw
// `$OPENCODE_CONFIG_DIR` / `~/.config/opencode` and workers resolved only the
// built-in skills. These tests pin both halves of the contract.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Project } from "@opencode-ai/schema/project"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import {
  boundaryReplacements,
  snapshotConfigLayer,
  snapshotRuntimeFlagsLayer,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import { makeInstanceContext } from "@/session/worker/snapshot"
import { workerInstanceContext } from "@/session/worker/session-worker"
import { fallbackInstanceContext, instanceFromSession } from "@/session/worker/hub"

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

/**
 * A hub-resolved flag set with `experimentalBackgroundSubagents` forced on, as the
 * hub would ship it: the override exists only on the hub side, so the isolate can
 * see it ONLY through the snapshot. `parseInfo` supplies the canonical defaults
 * for the remaining flags.
 */
const hubFlags = (): RuntimeFlags.Info =>
  RuntimeFlags.parseInfo({ experimentalBackgroundSubagents: true })

/** Resolve the worker-side `RuntimeFlags.Service` from a compiled layer. */
const flagsFrom = (layer: Layer.Layer<RuntimeFlags.Service>): Promise<RuntimeFlags.Info> =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* RuntimeFlags.Service
    }).pipe(Effect.provide(layer)),
  )

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

describe("worker runtime flags cross the snapshot boundary", () => {
  test("snapshotRuntimeFlagsLayer serves the hub's resolved flags verbatim", async () => {
    const flags = await flagsFrom(snapshotRuntimeFlagsLayer(snapshot({ flags: hubFlags() })))
    expect(flags.experimentalBackgroundSubagents).toBe(true)
    // A flag the hub left at its default stays at the default.
    expect(flags.autoShare).toBe(false)
  })

  test("the full session graph's boundary replacements honor the snapshot flags", async () => {
    const layer = LayerNode.compile(
      RuntimeFlags.node,
      boundaryReplacements(snapshot({ flags: hubFlags() })),
    )
    const flags = await flagsFrom(layer)
    expect(flags.experimentalBackgroundSubagents).toBe(true)
  })

  test("parseInfo recovers defaults for a missing or malformed snapshot", () => {
    expect(RuntimeFlags.parseInfo(undefined).autoShare).toBe(false)
    expect(RuntimeFlags.parseInfo({ experimentalBackgroundSubagents: true }).experimentalBackgroundSubagents).toBe(true)
    // A malformed value recovers to the default rather than being trusted.
    expect(RuntimeFlags.parseInfo({ autoShare: "yes" }).autoShare).toBe(false)
    // Optional numeric flags pass through when finite, recover otherwise.
    expect(RuntimeFlags.parseInfo({ outputTokenMax: 4096 }).outputTokenMax).toBe(4096)
    expect(RuntimeFlags.parseInfo({ outputTokenMax: "nope" }).outputTokenMax).toBeUndefined()
  })
})

describe("makeInstanceContext equivalence", () => {
  test("the snapshot-derived context equals the shared builder's output", () => {
    const built = workerInstanceContext({
      directory: "/work/a",
      worktree: "/work/a",
      projectId: "proj_a",
      projectVcs: "git",
    } as WorkerSnapshot)
    expect(built).toEqual(
      makeInstanceContext({ directory: "/work/a", projectID: "proj_a", projectVcs: "git" }),
    )
  })

  test("the session-derived and fallback contexts share one project shape", () => {
    expect(instanceFromSession({ directory: "/work/b", projectID: "proj_b" })).toEqual(
      makeInstanceContext({ directory: "/work/b", projectID: "proj_b" }),
    )
    expect(fallbackInstanceContext()).toEqual(
      makeInstanceContext({ directory: process.cwd(), projectID: Project.ID.global }),
    )
  })

  test("worktree defaults to the directory and the project carries a zero time span", () => {
    const ctx = makeInstanceContext({ directory: "/work/c", projectID: Project.ID.global })
    expect(ctx.worktree).toBe("/work/c")
    expect(ctx.project.worktree).toBe("/work/c")
    expect(ctx.project.time).toEqual({ created: 0, updated: 0 })
    expect(ctx.project.sandboxes).toEqual([])
  })
})
