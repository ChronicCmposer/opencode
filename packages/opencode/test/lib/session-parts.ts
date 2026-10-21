// Shared session payload fixture for the worker/rollout suites.
//
// `withParts` builds the minimal valid `SessionV1.WithParts` the session-worker
// result path carries (`run-state.ts` decodes exactly this shape at the hub
// boundary). Suites only ever compare one `withParts(...)` against another, so
// the message fields are intentionally fixed: one definition, many callers
// (finding #182).

import type { SessionV1 } from "@opencode-ai/core/v1/session"
import type { SessionID } from "@/session/schema"

/** A minimal valid `SessionV1.WithParts` (the worker result payload). */
export const withParts = (sessionID: SessionID): SessionV1.WithParts =>
  ({
    info: {
      id: "msg_test",
      sessionID,
      role: "user",
      time: { created: 0 },
      agent: "build",
      model: { providerID: "test", modelID: "test" },
    },
    parts: [],
  }) as unknown as SessionV1.WithParts
