// Shared session payload fixture for the worker/rollout suites.
//
// `withParts` builds the minimal valid `SessionV1.WithParts` the session-worker
// result path carries (`run-state.ts` decodes exactly this shape at the hub
// boundary). Suites only ever compare one `withParts(...)` against another, so
// the message fields are intentionally fixed: one definition, many callers
// (finding #182).
//
// The value is built through the canonical schemas (branded ids via `.make`)
// and merged with the caller's `Partial<User>` overrides, rather than cast past
// the types, so a field/brand change fails here loudly instead of producing a
// value the hub would reject.

import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Model } from "@opencode-ai/schema/model"
import { Provider } from "@opencode-ai/schema/provider"
import type { SessionID } from "@/session/schema"

/** A minimal valid `SessionV1.WithParts` (the worker result payload). */
export const withParts = (
  sessionID: SessionID,
  overrides: Partial<SessionV1.User> = {},
): SessionV1.WithParts => ({
  info: {
    id: SessionV1.MessageID.make("msg_test"),
    sessionID,
    role: "user",
    time: { created: 0 },
    agent: "build",
    model: { providerID: Provider.ID.make("test"), modelID: Model.ID.make("test") },
    ...overrides,
  },
  parts: [],
})
