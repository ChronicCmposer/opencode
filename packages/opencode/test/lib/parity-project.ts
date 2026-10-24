// The synthetic project an ambient parity instance resolves.
//
// A parity runner attaches no project loader, but `InstanceContext.project` is
// typed as a full `Project.Info`. Building the record from a plain literal, with
// the id branded through the schema's own constructor, keeps callers from
// forcing a partial past the type with a cast.

import type { InstanceContext } from "@/project/instance-context"
import { Project } from "@opencode-ai/schema/project"

/** Build a synthetic project record with the given id (no loader is attached). */
export function parityProject(id: string): InstanceContext["project"] {
  return {
    id: Project.ID.make(id),
    worktree: "/",
    time: { created: 0, updated: 0 },
    sandboxes: [],
  }
}
