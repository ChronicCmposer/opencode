// Worker skill discovery regression: a skill in a hub config directory must be
// discovered inside the worker context.
//
// `Skill` discovery scans `config.directories()` for `{skill,skills}/**/SKILL.md`
// (src/skill/index.ts:205-208). A worker's Config is `snapshotConfigLayer(snapshot)`;
// before `WorkerSnapshot.directories` shipped, `directories()` returned only
// `[snapshot.directory]`, so a worker scanned only its project dir and saw only
// the built-in `customize-opencode`. These tests boot the real worker `Skill`
// service against a snapshot-backed Config and pin both halves: a skill that
// lives only in a hub config directory is discovered, and a snapshot without
// `directories` still falls back to the project dir (the control).

import { describe, expect, test } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect } from "effect"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Skill } from "@/skill"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceRef } from "@/effect/instance-ref"
import { snapshotConfigLayer, type WorkerSnapshot } from "@/session/worker/layer"
import { workerInstanceContext } from "@/session/worker/session-worker"
import { workerSnapshot } from "../../lib/snapshot"

const BUILT_IN = "customize-opencode"
const HUB_SKILL = "code-philosophy"

const HUB_SKILL_MD = `---
name: ${HUB_SKILL}
description: A skill that lives only in a hub config directory.
---

# Code Philosophy

Proves a worker discovers a skill outside its project directory.
`

/**
 * Boot the real worker `Skill` service with `Config` supplied by the shipped
 * snapshot seam, then read the sorted available skill names. External
 * `.claude`/`.agents` scanning is disabled so the assertion isolates
 * `config.directories()`.
 */
const availableNames = (snapshot: WorkerSnapshot) =>
  Effect.gen(function* () {
    const skill = yield* Skill.Service
    const list = yield* skill.available()
    return list.map((item) => item.name)
  }).pipe(
    Effect.provideService(InstanceRef, workerInstanceContext(snapshot)),
    Effect.provide(
      LayerNode.compile(Skill.node, [
        [Config.node, snapshotConfigLayer(snapshot)],
        [RuntimeFlags.node, RuntimeFlags.layer({ disableExternalSkills: true, disableClaudeCodeSkills: true })],
      ]),
    ),
  )

/** A scoped temp root with a `project` dir and a separate `hub` config dir. */
const withDirs = <A, E, R>(fn: (dirs: { project: string; hub: string }) => Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const root = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "oc-skill-discovery-")))
    const project = path.join(root, "project")
    const hub = path.join(root, "hub")
    yield* Effect.promise(() =>
      Promise.all([fs.mkdir(project, { recursive: true }), fs.mkdir(hub, { recursive: true })]),
    )
    yield* Effect.addFinalizer(() => Effect.promise(() => fs.rm(root, { recursive: true, force: true })))
    return yield* fn({ project, hub })
  })

describe("worker skill discovery (snapshot directories)", () => {
  test("discovers a skill that lives only in a hub config directory", async () => {
    const names = await Effect.runPromise(
      Effect.scoped(
        withDirs(({ project, hub }) =>
          Effect.gen(function* () {
            yield* Effect.promise(() => Bun.write(path.join(hub, "skills", HUB_SKILL, "SKILL.md"), HUB_SKILL_MD))
            // The hub skill is NOT under `project`; only the shipped `directories`
            // seam can reveal it to the worker.
            return yield* availableNames(workerSnapshot({ directory: project, directories: [hub] }))
          }),
        ),
      ),
    )

    expect(names).toContain(HUB_SKILL)
    expect(names).toContain(BUILT_IN)
  })

  test("control: without shipped directories, only the built-in skill is seen", async () => {
    const names = await Effect.runPromise(
      Effect.scoped(
        withDirs(({ project, hub }) =>
          Effect.gen(function* () {
            yield* Effect.promise(() => Bun.write(path.join(hub, "skills", HUB_SKILL, "SKILL.md"), HUB_SKILL_MD))
            // No `directories`: the seam falls back to `[directory]`, which is the
            // project dir and holds no `{skill,skills}` tree.
            return yield* availableNames(workerSnapshot({ directory: project }))
          }),
        ),
      ),
    )

    expect(names).not.toContain(HUB_SKILL)
    expect(names).toEqual([BUILT_IN])
  })
})
