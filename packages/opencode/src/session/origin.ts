// Who asked for a session dispatch.
//
// A user prompt is explicit: the user asked for the run, so crash supervision
// resets and the dispatcher respawns a fresh worker. Background / auto work is
// not explicit: once a session's worker has crashed repeatedly it fails fast
// instead of spawning forever (see `worker/supervision.ts`).
//
// Kept in the session domain (not the worker folder) so prompt/session/run-state
// can share the vocabulary without importing a worker module.

import { Schema } from "effect"

export const DispatchOrigin = Schema.Literals(["user", "auto"]).annotate({
  description: "Who asked for the dispatch: an explicit user prompt or background/auto work",
})
export type DispatchOrigin = Schema.Schema.Type<typeof DispatchOrigin>

export * as SessionOrigin from "./origin"
