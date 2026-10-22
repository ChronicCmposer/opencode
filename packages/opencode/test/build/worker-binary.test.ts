// Build-verification gate: a packaged binary must boot the session worker and
// stream its events to `/global/event`.
//
// This test runs the *built* artifact through `script/smoke-worker-binary.ts`.
// It is the only tier that can catch a `session-worker.ts` that is missing from
// the compiled binary's entrypoints: in-process tests load the worker from
// source, so they stay green while `dist/<target>/bin/opencode` dies at isolate
// boot with `ModuleNotFound resolving "/$bunfs/root/session-worker.ts"` and the
// user sees a prompt that never streams.
//
// Running it:
//   cd packages/opencode
//   bun test test/build/worker-binary.test.ts          # skip if no binary built
//   bun run script/build.ts --single --skip-install --skip-embed-web-ui
//   bun test test/build/worker-binary.test.ts          # the real gate
//
// Or drive the same logic directly:
//   bun run script/smoke-worker-binary.ts
//
// Skip conditions (so an un-built checkout does not fail the unit suite):
//   * the native `dist/<target>/bin/opencode` does not exist, or
//   * `OPENCODE_SKIP_WORKER_BINARY_SMOKE=1` is set.
// Build verification must run it with the binary present and the skip unset.

import { describe, expect, test } from "bun:test"
import { existsSync } from "node:fs"
import { REQUIRED_EVENT_TYPES, runSmokeWorkerBinary, smokeBinaryPath } from "../../script/smoke-worker-binary"

const binary = smokeBinaryPath()
const skipped = !existsSync(binary) || process.env["OPENCODE_SKIP_WORKER_BINARY_SMOKE"] === "1"

function skipReason(): string {
  if (process.env["OPENCODE_SKIP_WORKER_BINARY_SMOKE"] === "1") return "OPENCODE_SKIP_WORKER_BINARY_SMOKE=1"
  return `packaged binary not built at ${binary}`
}

describe("packaged session-worker binary", () => {
  test.skipIf(skipped)(
    "boots the worker and streams message.part.* / session.status to /global/event",
    async () => {
      const result = await runSmokeWorkerBinary()
      if (!result.passed) {
        // Surface the packaged-worker boot signature so the failure is actionable
        // (the hub swallows the isolate's real cause as `worker N crashed`).
        throw new Error(
          [
            `packaged session worker did not stream to /global/event`,
            `missing: ${result.missing.join(", ")}`,
            `observed: ${result.observedTypes.join(", ") || "(none)"}`,
            `signatures: ${result.signatures.join(" | ") || "(none)"}`,
            `server exit: ${result.serverExitCode ?? "running"}`,
            `--- server log tail ---`,
            result.logTail,
          ].join("\n"),
        )
      }
      expect(result.missing).toEqual([])
      for (const type of REQUIRED_EVENT_TYPES) expect(result.observedTypes).toContain(type)
    },
    120_000,
  )

  test("documents the required streamed event types", () => {
    expect([...REQUIRED_EVENT_TYPES]).toEqual(["message.part.updated", "message.part.delta", "session.status"])
  })
})

if (skipped) {
  // eslint-disable-next-line no-console
  console.log(`[worker-binary.test] skipped: ${skipReason()}`)
}
