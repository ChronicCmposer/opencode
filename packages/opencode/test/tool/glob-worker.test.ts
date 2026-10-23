// Fix 2 — the `glob` tool must run inside a session worker.
//
// `GlobTool` depends on `Ripgrep.Service`; before Fix 2 the worker graph bound
// that to a fail-loud `ripgrepBoundaryLayer`, so the tool died with
// `SessionLayerBoundaryError`. This resolves the real tool effect against the
// production reduced graph and executes it on a fixture directory.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { GlobTool } from "@/tool/glob"
import { SessionID, MessageID } from "@/session/schema"
import type * as Tool from "@/tool/tool"
import { runWorker } from "../lib/worker-ripgrep-harness"

const ctx: Tool.Context = {
  sessionID: SessionID.make("ses_worker_glob"),
  messageID: MessageID.make("msg_worker_glob"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

function fixture(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "oc-glob-worker-"))
  fs.writeFileSync(path.join(dir, "a.ts"), "export const a = 1\n")
  fs.writeFileSync(path.join(dir, "b.txt"), "hello\n")
  fs.mkdirSync(path.join(dir, "nested"))
  fs.writeFileSync(path.join(dir, "nested", "c.ts"), "export const c = 1\n")
  return dir
}

const withFixture = <A>(use: (dir: string) => Promise<A>): Promise<A> => {
  const dir = fixture()
  return use(dir).finally(() => fs.rmSync(dir, { recursive: true, force: true }))
}

describe("worker GlobTool (Fix 2)", () => {
  test("executes in-worker and lists matching files", () =>
    withFixture(async (dir) => {
      const result = await runWorker(
        Effect.gen(function* () {
          const info = yield* GlobTool
          const glob = yield* info.init()
          return yield* glob.execute({ pattern: "**/*.ts", path: dir }, ctx)
        }),
        dir,
      )

      expect(result.metadata.count).toBe(2)
      expect(result.output).toContain(path.join(dir, "a.ts"))
      expect(result.output).toContain(path.join(dir, "nested", "c.ts"))
      expect(result.output).not.toContain(path.join(dir, "b.txt"))
    }))

  test("returns the empty result when nothing matches", () =>
    withFixture(async (dir) => {
      const result = await runWorker(
        Effect.gen(function* () {
          const info = yield* GlobTool
          const glob = yield* info.init()
          return yield* glob.execute({ pattern: "**/*.rs", path: dir }, ctx)
        }),
        dir,
      )

      expect(result.metadata.count).toBe(0)
      expect(result.output).toBe("No files found")
    }))
})
