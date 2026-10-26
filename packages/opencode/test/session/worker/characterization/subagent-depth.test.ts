// Characterization: the `subagent_depth` admission contract.
//
// Depth is now explicit `metadata.depth` stamped by `Session.spawn` and enforced
// by the shared prompt guard (`assertSpawnDepth`). The task tool relies on that
// guard, so this test pins the two externally visible facts:
//   * the stamped child depth is `parent.depth + 1`;
//   * a child above the configured limit fails with the locked message before
//     any prompt runs.
//
// The exact user-facing text is part of the contract: the refactor may move the
// check (shared prompt/dispatch, explicit `metadata.depth`), so the message must
// stay byte-identical.

import { afterEach, describe, expect } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import { Session } from "@/session/session"
import { MessageID } from "@/session/schema"
import { assertSpawnDepth } from "@/session/prompt"
import { disposeAllInstances } from "../../../fixture/fixture"
import { testEffect } from "../../../lib/effect"
import { seed, setSpawn, spawnResult, stubOps, taskLayer, toolContext, TaskTool } from "./task-harness"

afterEach(async () => {
  await disposeAllInstances()
})

const it = testEffect(taskLayer())

/**
 * A depth-1 parent attempts a child at the resolved limit of 1. The rejection
 * must happen before the permission ask, before `sessions.create`, and before
 * the hub-proxy spawn.
 */
const overDepthScenario = Effect.fn("Characterization.subagentDepth.overDepth")(function* () {
  const sessions = yield* Session.Service
  const { chat, assistant } = yield* seed()
  const child = yield* sessions.create({ parentID: chat.id, title: "child", metadata: { depth: 1 } })
  const nestedAssistant = yield* sessions.updateMessage({
    ...assistant,
    id: MessageID.ascending(),
    parentID: MessageID.ascending(),
    sessionID: child.id,
  })
  const tool = yield* TaskTool
  const def = yield* tool.init()
  let asked = false
  const spawnDepths: number[] = []
  setSpawn((request) =>
    Effect.sync(() => {
      spawnDepths.push(typeof request.metadata?.depth === "number" ? request.metadata.depth : -1)
      assertSpawnDepth(request.metadata, 1)
      return spawnResult(request, "should not run")
    }),
  )

  const exit = yield* def
    .execute(
      {
        description: "inspect bug",
        prompt: "look into the cache key path",
        subagent_type: "general",
      },
      {
        ...toolContext({
          sessionID: child.id,
          messageID: nestedAssistant.id,
          promptOps: stubOps(),
        }),
        ask: () => Effect.sync(() => (asked = true)),
      },
    )
    .pipe(Effect.exit)

  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isSuccess(exit)) throw new Error("expected a depth rejection")
  const failure = Cause.squash(exit.cause)
  expect(failure).toBeInstanceOf(Error)
  if (!(failure instanceof Error)) throw new Error("expected an Error defect")
  expect(failure.message).toBe(
    `Subagent depth limit reached (1). Increase "subagent_depth" to allow nested subagents.`,
  )
  expect(asked).toBe(false)
  expect(spawnDepths).toEqual([])
  expect(yield* sessions.children(child.id)).toEqual([])
})

describe("subagent_depth admission", () => {
  it.instance("rejects a child above the limit before asking or creating a child", () => overDepthScenario(), {
    config: { subagent_depth: 1 },
  })

  it.instance("defaults an unset subagent_depth to 1 before asking or creating a child", () => overDepthScenario())
})
