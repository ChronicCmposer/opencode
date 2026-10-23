// Fix 2 — the `skill` tool must run inside a session worker.
//
// `SkillTool.execute` resolves the skill, then lists its files through
// `Ripgrep.Service.find({ signal: ctx.abort })`. Before Fix 2 that search died
// with `SessionLayerBoundaryError`. This builds the production reduced graph with
// the fixture directory as the snapshot's config root so the skill is discovered,
// then executes the real tool effect with an `AbortSignal`.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { SkillTool } from "@/tool/skill"
import { SessionID, MessageID } from "@/session/schema"
import type * as Tool from "@/tool/tool"
import { workerSnapshot } from "../lib/snapshot"
import { buildWorkerLayer, runWithLayer } from "../lib/worker-ripgrep-harness"

const SKILL_NAME = "worker-skill"

const ctx: Tool.Context = {
  sessionID: SessionID.make("ses_worker_skill"),
  messageID: MessageID.make("msg_worker_skill"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

interface Fixture {
  readonly dir: string
  readonly skillDir: string
  readonly demoFile: string
}

/** A project with a discoverable skill at `<dir>/skill/<name>/SKILL.md`. */
function fixture(): Fixture {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "oc-skill-worker-"))
  const skillDir = path.join(dir, "skill", SKILL_NAME)
  fs.mkdirSync(path.join(skillDir, "scripts"), { recursive: true })
  fs.writeFileSync(
    path.join(skillDir, "SKILL.md"),
    `---
name: ${SKILL_NAME}
description: Skill used by the worker SkillTool test.
---

# Worker Skill

Run the demo script.
`,
  )
  const demoFile = path.join(skillDir, "scripts", "demo.txt")
  fs.writeFileSync(demoFile, "demo\n")
  return { dir, skillDir, demoFile }
}

describe("worker SkillTool (Fix 2)", () => {
  test("executes in-worker and lists skill files through ripgrep.find", async () => {
    const { dir, skillDir, demoFile } = fixture()
    try {
      const layer = buildWorkerLayer({ snapshot: workerSnapshot({ directory: dir, directories: [dir] }) })
      const result = await runWithLayer(
        layer,
        Effect.gen(function* () {
          const info = yield* SkillTool
          const skill = yield* info.init()
          return yield* skill.execute({ name: SKILL_NAME }, ctx)
        }),
        dir,
      )

      expect(result.metadata.dir).toBe(skillDir)
      expect(result.output).toContain(`<skill_content name="${SKILL_NAME}">`)
      expect(result.output).toContain(`<file>${demoFile}</file>`)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})
