import { Effect, Layer, Schema } from "effect"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Project } from "@opencode-ai/core/project"

export interface SnapshotProjectInput {
  readonly directory: string
  readonly worktree?: string
  readonly projectId?: string
  readonly projectVcs?: "git"
  readonly projectStore?: string
}

export class SnapshotProjectBoundaryError extends Schema.TaggedErrorClass<SnapshotProjectBoundaryError>()(
  "WorkerSnapshotProject.Boundary",
  {
    method: Schema.String,
  },
) {}

/**
 * The worker's Location/Reference graph must not run `git rev-parse`.
 * Resolve from the hub snapshot instead; real Git remains available to
 * RepositoryCache for configured git references only.
 */
export function snapshotProjectLayer(snapshot: SnapshotProjectInput): Layer.Layer<Project.Service> {
  const worktree = AbsolutePath.make(snapshot.worktree ?? snapshot.directory)
  const vcs =
    snapshot.projectVcs === "git" && snapshot.projectStore !== undefined
      ? {
          type: "git" as const,
          store: AbsolutePath.make(snapshot.projectStore),
        }
      : undefined

  return Layer.succeed(
    Project.Service,
    Project.Service.of({
      resolve: () =>
        Effect.succeed({
          id: snapshot.projectId === undefined ? Project.ID.global : Project.ID.make(snapshot.projectId),
          directory: worktree,
          ...(vcs === undefined ? {} : { vcs }),
        }),
      directories: () => Effect.die(new SnapshotProjectBoundaryError({ method: "directories" })),
      commit: () => Effect.die(new SnapshotProjectBoundaryError({ method: "commit" })),
    }),
  )
}
