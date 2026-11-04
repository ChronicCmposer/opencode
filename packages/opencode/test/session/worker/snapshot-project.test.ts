// The worker's Project service is resolved from the hub snapshot, never by
// running `git rev-parse`. Only `resolve` is meaningful during boot; the other
// members fail loudly because nothing in the worker is allowed to reach them.

import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import {
  SnapshotProjectBoundaryError,
  snapshotProjectLayer,
  type SnapshotProjectInput,
} from "@/session/worker/snapshot-project"

const resolveWith = (input: SnapshotProjectInput): Promise<Project.Resolved> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const project = yield* Project.Service
      return yield* project.resolve(AbsolutePath.make("/ignored/input"))
    }).pipe(Effect.provide(snapshotProjectLayer(input))),
  )

const defectOf = async (
  run: (project: Project.Interface) => Effect.Effect<unknown>,
  input: SnapshotProjectInput,
): Promise<unknown> => {
  const exit = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const project = yield* Project.Service
      return yield* run(project)
    }).pipe(Effect.provide(snapshotProjectLayer(input))),
  )
  if (Exit.isSuccess(exit)) throw new Error("expected the boundary to fail loudly")
  return Cause.squash(exit.cause)
}

describe("snapshotProjectLayer", () => {
  test("resolves the snapshot project id and worktree", async () => {
    const resolved = await resolveWith({
      directory: "/work/checkout",
      worktree: "/work/repo",
      projectId: "proj_hub",
    })

    expect(String(resolved.id)).toBe("proj_hub")
    expect(String(resolved.directory)).toBe("/work/repo")
    expect(resolved.vcs).toBeUndefined()
  })

  test("defaults to the global project and the directory as worktree", async () => {
    const resolved = await resolveWith({ directory: "/work/solo" })

    expect(resolved.id).toBe(Project.ID.global)
    expect(String(resolved.directory)).toBe("/work/solo")
  })

  test("carries git vcs only when the snapshot says git and supplies a store", async () => {
    const resolved = await resolveWith({
      directory: "/work/checkout",
      projectId: "proj_hub",
      projectVcs: "git",
      projectStore: "/work/repo/.git",
    })

    expect(resolved.vcs?.type).toBe("git")
    expect(String(resolved.vcs?.store)).toBe("/work/repo/.git")
  })

  test("drops git vcs when the snapshot has no store", async () => {
    const resolved = await resolveWith({
      directory: "/work/checkout",
      projectVcs: "git",
    })

    expect(resolved.vcs).toBeUndefined()
  })

  test("directories fails loudly", async () => {
    const defect = await defectOf(
      (project) => project.directories({ projectID: Project.ID.global }),
      { directory: "/work/solo" },
    )

    expect(defect).toBeInstanceOf(SnapshotProjectBoundaryError)
    expect((defect as SnapshotProjectBoundaryError).method).toBe("directories")
  })

  test("commit fails loudly", async () => {
    const defect = await defectOf(
      (project) =>
        project.commit({
          store: AbsolutePath.make("/work/repo/.git"),
          id: Project.ID.make("proj_hub"),
        }),
      { directory: "/work/solo" },
    )

    expect(defect).toBeInstanceOf(SnapshotProjectBoundaryError)
    expect((defect as SnapshotProjectBoundaryError).method).toBe("commit")
  })
})
