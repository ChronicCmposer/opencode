// Characterization: the `task` tool's externally observable lifecycle.
//
// These tests PASS on the pre-refactor tree and pin the contract the per-session
// worker refactor must preserve:
//   * foreground `task` returns the child's final assistant text;
//   * background `task` starts a job and completes through `BackgroundJob`;
//   * aborting a foreground `task` cancels the child and settles the tool call.
//
// Deterministic: readiness and completion are Deferred/promise signals, never
// fixed sleeps. The child runs through the `Session.spawn` proxy seam (the
// per-session-worker path); the proxy double is the only test-owned behavior.

import { afterEach, describe, expect } from "bun:test"
import { Effect, Exit, Fiber } from "effect"
import { BackgroundJob } from "@/background/job"
import type { SessionID } from "@/session/schema"
import type { SessionPrompt } from "@/session/prompt"
import { disposeAllInstances } from "../../../fixture/fixture"
import { testEffect } from "../../../lib/effect"
import { defer, seed, setSpawn, spawnResult, stubOps, taskLayer, toolContext, TaskTool } from "./task-harness"

afterEach(async () => {
  await disposeAllInstances()
})

const it = testEffect(taskLayer())
const background = testEffect(taskLayer({ experimentalBackgroundSubagents: true }))

describe("task lifecycle — foreground", () => {
  it.instance("returns the child's final assistant text", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      setSpawn((request) => Effect.succeed(spawnResult(request, "child says hello")))

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
        },
        toolContext({
          sessionID: chat.id,
          messageID: assistant.id,
          promptOps: stubOps(),
        }),
      )

      expect(result.metadata.background).toBeUndefined()
      expect(result.output).toContain(`<task id="${result.metadata.sessionId}" state="completed">`)
      expect(result.output).toContain("<task_result>\nchild says hello\n</task_result>")
    }),
  )
})

describe("task lifecycle — background", () => {
  background.instance("starts a job and completes through the job service", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      setSpawn((request) => Effect.succeed(spawnResult(request, "background done")))

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          background: true,
        },
        toolContext({
          sessionID: chat.id,
          messageID: assistant.id,
          promptOps: stubOps(),
        }),
      )

      expect(result.metadata.background).toBe(true)
      expect(result.output).toContain('state="running"')

      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("completed")
      expect(waited.info?.output).toBe("background done")
    }),
  )
})

describe("task lifecycle — cancel", () => {
  it.instance("aborting a foreground run cancels the child and settles the call", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const cancelled = defer<SessionID>()
      const abort = new AbortController()
      const promptOps = {
        ...stubOps(),
        cancel: (sessionID: SessionID) =>
          Effect.sync(() => {
            cancelled.resolve(sessionID)
          }),
      }
      setSpawn((request) =>
        Effect.promise(() => {
          ready.resolve(request.prompt)
          return cancelled.promise
        }).pipe(Effect.as(spawnResult(request, "cancelled"))),
      )

      const fiber = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
          },
          toolContext({
            sessionID: chat.id,
            messageID: assistant.id,
            promptOps,
            abort: abort.signal,
          }),
        )
        .pipe(Effect.forkChild)

      const input = yield* Effect.promise(() => ready.promise)
      abort.abort()
      expect(yield* Effect.promise(() => cancelled.promise)).toBe(input.sessionID)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
    }),
  )
})
