// Fix 2 — the `grep` tool must run inside a session worker.
//
// `GrepTool` depends on `Ripgrep.Service`; before Fix 2 the worker graph bound
// that to a fail-loud `ripgrepBoundaryLayer`, so the tool died with
// `SessionLayerBoundaryError`. This resolves the real tool effect against the
// production reduced graph and executes it on a fixture directory.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { GrepTool } from "@/tool/grep"
import { SessionID, MessageID } from "@/session/schema"
import type * as Tool from "@/tool/tool"
import { runWorker } from "../lib/worker-ripgrep-harness"

const ctx: Tool.Context = {
  sessionID: SessionID.make("ses_worker_grep"),
  messageID: MessageID.make("msg_worker_grep"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

function fixture(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "oc-grep-worker-"))
  fs.writeFileSync(path.join(dir, "alpha.txt"), "first line\nhello needle here\nlast line\n")
  fs.writeFileSync(path.join(dir, "beta.md"), "needle in markdown\n")
  return dir
}

const withFixture = <A>(use: (dir: string) => Promise<A>): Promise<A> => {
  const dir = fixture()
  return use(dir).finally(() => fs.rmSync(dir, { recursive: true, force: true }))
}

describe("worker GrepTool (Fix 2)", () => {
  test("executes in-worker and reports matches", () =>
    withFixture(async (dir) => {
      const result = await runWorker(
        Effect.gen(function* () {
          const info = yield* GrepTool
          const grep = yield* info.init()
          return yield* grep.execute({ pattern: "needle", path: dir, include: "*.txt" }, ctx)
        }),
        dir,
      )

      expect(result.metadata.matches).toBeGreaterThan(0)
      expect(result.output).toContain("Found")
      expect(result.output).toContain("needle")
      expect(result.output).toContain("Line 2: hello needle here")
    }))

  test("returns the empty result for a pattern with no matches", () =>
    withFixture(async (dir) => {
      const result = await runWorker(
        Effect.gen(function* () {
          const info = yield* GrepTool
          const grep = yield* info.init()
          return yield* grep.execute({ pattern: "zzz-no-such-token-zzz", path: dir }, ctx)
        }),
        dir,
      )

      expect(result.metadata.matches).toBe(0)
      expect(result.output).toBe("No files found")
    }))
})
