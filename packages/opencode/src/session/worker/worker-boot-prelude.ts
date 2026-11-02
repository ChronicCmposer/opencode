import { syncTrace } from "@opencode-ai/core/observability/sync-trace"

/** Written when this module is evaluated; `session-worker.ts` reports the delta after its imports. */
export const PRELUDE_STARTED_AT = Date.now()

syncTrace({
  prefix: "session workers: ",
  level: "info",
  event: "worker module eval start",
  fields: { startedAt: PRELUDE_STARTED_AT },
})
