import { afterEach, describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Agent } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { Session } from "@/session/session"
import type { SessionPrompt } from "../../src/session/prompt"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import {
  SessionSpawnProxyService,
  type SessionSpawnRequest,
  type SessionSpawnResult,
} from "../../src/session/worker/subagent"

import { TaskTool, type TaskPromptOps } from "../../src/tool/task"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

/** The spawn the current test controls; `Session.spawn` prefers the proxy. */
let onSpawn: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult> = (request) =>
  Effect.succeed(resultFor(request, "done"))
const spawnCalls: SessionSpawnRequest[] = []

const spawnProxyLayer = Layer.succeed(SessionSpawnProxyService, {
  spawn: (request) => {
    spawnCalls.push(request)
    return onSpawn(request)
  },
})

const layer = (flags: Partial<RuntimeFlags.Info> = {}) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [[RuntimeFlags.node, RuntimeFlags.layer(flags)]],
  ).pipe(Layer.provideMerge(spawnProxyLayer))

const it = testEffect(layer())
const background = testEffect(layer({ experimentalBackgroundSubagents: true }))

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function resultFor(
  request: SessionSpawnRequest,
  text: string,
  input?: {
    error?: NonNullable<SessionV1.Assistant["error"]>
    toolError?: string
  },
): SessionSpawnResult {
  const childID = request.childID ?? (MessageID.ascending() as unknown as SessionID)
  const id = MessageID.ascending()
  return {
    childID,
    result: {
      info: {
        id,
        role: "assistant",
        parentID: request.prompt.messageID ?? MessageID.ascending(),
        sessionID: childID,
        mode: request.prompt.agent ?? "general",
        agent: request.prompt.agent ?? "general",
        cost: 0,
        path: { cwd: "/tmp", root: "/tmp" },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: request.prompt.model?.modelID ?? ref.modelID,
        providerID: request.prompt.model?.providerID ?? ref.providerID,
        time: { created: Date.now() },
        finish: "stop",
        error: input?.error,
      },
      parts: [
        { id: PartID.ascending(), messageID: id, sessionID: childID, type: "text", text },
        ...(input?.toolError
          ? [
              {
                id: PartID.ascending(),
                messageID: id,
                sessionID: childID,
                type: "tool" as const,
                tool: "read",
                callID: "call-1",
                state: {
                  status: "error" as const,
                  input: { filePath: "/external" },
                  error: input.toolError,
                  time: { start: Date.now(), end: Date.now() },
                },
              },
            ]
          : []),
      ],
    },
  }
}

const seed = Effect.fn("TaskToolTest.seed")(function* (title = "Pinned") {
  const session = yield* Session.Service
  const chat = yield* session.create({ title })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    variant: "xhigh",
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  return { chat, assistant }
})

function stubOps(opts?: {
  onPrompt?: (input: SessionPrompt.PromptInput) => void
  onCancel?: (sessionID: SessionID) => void
}): TaskPromptOps {
  return {
    cancel: (sessionID) =>
      Effect.sync(() => {
        opts?.onCancel?.(sessionID)
      }),
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        opts?.onPrompt?.(input)
        const id = MessageID.ascending()
        return {
          info: {
            id,
            role: "assistant",
            parentID: input.messageID ?? MessageID.ascending(),
            sessionID: input.sessionID,
            mode: input.agent ?? "general",
            agent: input.agent ?? "general",
            cost: 0,
            path: { cwd: "/tmp", root: "/tmp" },
            tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
            modelID: ref.modelID,
            providerID: ref.providerID,
            time: { created: Date.now() },
            finish: "stop",
          },
          parts: [
            { id: PartID.ascending(), messageID: id, sessionID: input.sessionID, type: "text", text: "injected" },
          ],
        } satisfies SessionV1.WithParts
      }),
  }
}

const runContext = (chat: SessionID, assistant: MessageID, ops: TaskPromptOps, signal = new AbortController().signal) => ({
  sessionID: chat,
  messageID: assistant,
  agent: "build",
  abort: signal,
  extra: { promptOps: ops },
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

describe("tool.task", () => {
  it.instance("description sorts subagents by name and is stable across calls", () =>
    Effect.gen(function* () {
      const agent = yield* Agent.Service
      const build = yield* agent.get("build")
      const registry = yield* ToolRegistry.Service
      const get = Effect.fnUntraced(function* () {
        const tools = yield* registry.tools({ ...ref, agent: build })
        return tools.find((tool) => tool.id === TaskTool.id)?.description ?? ""
      })
      const first = yield* get()
      const second = yield* get()

      expect(first).toBe(second)
      const alpha = first.indexOf("- alpha: Alpha agent")
      const explore = first.indexOf("- explore:")
      const general = first.indexOf("- general:")
      const zebra = first.indexOf("- zebra: Zebra agent")
      expect(alpha).toBeGreaterThan(-1)
      expect(explore).toBeGreaterThan(alpha)
      expect(general).toBeGreaterThan(explore)
      expect(zebra).toBeGreaterThan(general)
    }),
    {
      config: {
        agent: {
          zebra: { description: "Zebra agent", mode: "subagent" },
          alpha: { description: "Alpha agent", mode: "subagent" },
        },
      },
    },
  )

  it.instance("description hides denied subagents for the caller", () =>
    Effect.gen(function* () {
      const agent = yield* Agent.Service
      const build = yield* agent.get("build")
      const registry = yield* ToolRegistry.Service
      const description =
        (yield* registry.tools({ ...ref, agent: build })).find((tool) => tool.id === TaskTool.id)?.description ?? ""
      expect(description).toContain("- alpha: Alpha agent")
      expect(description).not.toContain("- zebra: Zebra agent")
    }),
    {
      config: {
        permission: { task: { "*": "allow", zebra: "deny" } },
        agent: { zebra: { description: "Zebra agent", mode: "subagent" }, alpha: { description: "Alpha agent", mode: "subagent" } },
      },
    },
  )

  it.instance("foreground spawns the child and never runs it inline", () =>
    Effect.gen(function* () {
      spawnCalls.length = 0
      onSpawn = (request) => Effect.succeed(resultFor(request, "spawned output"))
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const parentPrompts: SessionPrompt.PromptInput[] = []

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "look into the cache key path", subagent_type: "general" },
        runContext(chat.id, assistant.id, stubOps({ onPrompt: (input) => parentPrompts.push(input) })),
      )

      expect(spawnCalls).toHaveLength(1)
      expect(spawnCalls[0]?.parentID).toBe(chat.id)
      expect(spawnCalls[0]?.childID).toBe(result.metadata.sessionId)
      expect(spawnCalls[0]?.type).toBe("task")
      expect(spawnCalls[0]?.prompt.sessionID).toBe(result.metadata.sessionId)
      expect(spawnCalls[0]?.prompt.agent).toBe("general")
      expect(spawnCalls[0]?.prompt.parts).toEqual([{ type: "text", text: "look into the cache key path" }])
      expect(spawnCalls[0]?.prompt.model).toEqual({ modelID: ref.modelID, providerID: ref.providerID })
      expect(spawnCalls[0]?.prompt.variant).toBe("xhigh")
      expect(parentPrompts).toHaveLength(0)
      expect(result.output).toContain(`<task id="${result.metadata.sessionId}" state="completed">`)
      expect(result.output).toContain("spawned output")
      expect((yield* sessions.get(result.metadata.sessionId)).metadata?.depth).toBe(1)
    }),
  )

  it.instance("resumes an existing task session and spawns with that childID", () =>
    Effect.gen(function* () {
      spawnCalls.length = 0
      onSpawn = (request) => Effect.succeed(resultFor(request, "resumed"))
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "Existing child" })
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "continue", subagent_type: "general", task_id: child.id },
        runContext(chat.id, assistant.id, stubOps()),
      )

      expect(spawnCalls[0]?.childID).toBe(child.id)
      expect(result.metadata.sessionId).toBe(child.id)
      expect(yield* sessions.children(chat.id)).toHaveLength(1)
    }),
  )

  it.instance("creates a child when task_id does not exist", () =>
    Effect.gen(function* () {
      spawnCalls.length = 0
      onSpawn = (request) => Effect.succeed(resultFor(request, "created"))
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "go", subagent_type: "general", task_id: "ses_missing" },
        runContext(chat.id, assistant.id, stubOps()),
      )

      const kids = yield* sessions.children(chat.id)
      expect(kids).toHaveLength(1)
      expect(kids[0]?.id).toBe(result.metadata.sessionId)
      expect(result.metadata.sessionId).not.toBe("ses_missing")
    }),
  )

  it.instance("surfaces child assistant errors", () =>
    Effect.gen(function* () {
      onSpawn = (request) =>
        Effect.succeed(
          resultFor(request, "", {
            error: new SessionV1.APIError({ message: "Network connection lost", isRetryable: false }).toObject(),
          }),
        )
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const exit = yield* def
        .execute(
          { description: "inspect bug", prompt: "go", subagent_type: "general" },
          runContext(chat.id, assistant.id, stubOps()),
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected task failure")
      const child = (yield* sessions.children(chat.id))[0]
      const failure = Cause.squash(exit.cause)
      expect(failure).toBeInstanceOf(Error)
      if (!(failure instanceof Error)) throw new Error("expected Error defect")
      expect(failure.message).toBe(`Subagent failed (task_id: ${child?.id}): Network connection lost`)
    }),
  )

  it.instance("surfaces terminal child tool errors", () =>
    Effect.gen(function* () {
      onSpawn = (request) =>
        Effect.succeed(resultFor(request, "I will inspect.", { toolError: "The user rejected permission." }))
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const exit = yield* def
        .execute(
          { description: "inspect bug", prompt: "go", subagent_type: "general" },
          runContext(chat.id, assistant.id, stubOps()),
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected task failure")
      const child = (yield* sessions.children(chat.id))[0]
      const failure = Cause.squash(exit.cause)
      if (!(failure instanceof Error)) throw new Error("expected Error defect")
      expect(failure.message).toBe(`Subagent failed (task_id: ${child?.id}): The user rejected permission.`)
    }),
  )

  it.instance("abort cancels the spawned child", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = yield* Deferred.make<SessionID>()
      const cancelled = yield* Deferred.make<SessionID>()
      const abort = new AbortController()

      onSpawn = (request) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(ready, request.childID ?? ("" as SessionID))
          const id = yield* Deferred.await(cancelled)
          return resultFor(request, `cancelled:${id}`)
        })
      const ops = stubOps({
        onCancel: (sessionID) => {
          Deferred.doneUnsafe(cancelled, Effect.succeed(sessionID))
        },
      })

      const fiber = yield* def
        .execute(
          { description: "inspect bug", prompt: "go", subagent_type: "general" },
          runContext(chat.id, assistant.id, ops, abort.signal),
        )
        .pipe(Effect.forkChild)

      const childID = yield* Deferred.await(ready)
      abort.abort()
      expect(yield* Deferred.await(cancelled)).toBe(childID)
      expect(Exit.isSuccess(yield* Fiber.await(fiber))).toBe(true)
    }),
  )

  it.instance("execute shapes child permissions for task, todowrite, and primary tools", () =>
    Effect.gen(function* () {
      onSpawn = (request) => Effect.succeed(resultFor(request, "ok"))
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "go", subagent_type: "reviewer" },
        runContext(chat.id, assistant.id, stubOps()),
      )

      const child = yield* sessions.get(result.metadata.sessionId)
      expect(child.parentID).toBe(chat.id)
      expect(child.agent).toBe("reviewer")
      expect(child.permission).toEqual([
        { permission: "todowrite", pattern: "*", action: "deny" },
        { permission: "bash", pattern: "*", action: "deny" },
        { permission: "read", pattern: "*", action: "deny" },
      ])
    }),
    {
      config: {
        agent: { reviewer: { mode: "subagent", permission: { task: "allow" } } },
        experimental: { primary_tools: ["bash", "read"] },
      },
    },
  )

  it.instance("a foreground spawn carries the user dispatch origin", () =>
    Effect.gen(function* () {
      spawnCalls.length = 0
      onSpawn = (request) => Effect.succeed(resultFor(request, "ok"))
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()

      yield* def.execute(
        { description: "inspect bug", prompt: "go", subagent_type: "general" },
        runContext(chat.id, assistant.id, stubOps()),
      )

      expect(spawnCalls).toHaveLength(1)
      expect(spawnCalls[0]?.prompt.origin).toBe("user")
    }),
  )

  it.instance("rejects background execution when the experiment is disabled", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const exit = yield* def
        .execute(
          { description: "inspect bug", prompt: "go", subagent_type: "general", background: true },
          runContext(chat.id, assistant.id, stubOps()),
        )
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )
})

describe("tool.task background", () => {
  background.instance("forks the spawn into a local background job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      onSpawn = (request) => Effect.succeed(resultFor(request, "background done"))

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "go", subagent_type: "general", background: true },
        runContext(chat.id, assistant.id, stubOps()),
      )

      expect(result.metadata.background).toBe(true)
      expect(result.output).toContain(`state="running"`)
      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("completed")
      expect(waited.info?.output).toBe("background done")
      // Background work is auto-originated: a dead child worker fails fast rather
      // than resetting crash supervision.
      expect(spawnCalls.at(-1)?.prompt.origin).toBe("auto")
    }),
  )

  background.instance("a running background spawn stays cancellable through the job", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const started = yield* Deferred.make<void>()
      const cancelled = yield* Deferred.make<SessionID>()
      const abort = new AbortController()
      onSpawn = () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          yield* Effect.never
          return resultFor({} as SessionSpawnRequest, "unreachable")
        })

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "go", subagent_type: "general", background: true },
        runContext(
          chat.id,
          assistant.id,
          stubOps({ onCancel: (sessionID) => Deferred.doneUnsafe(cancelled, Effect.succeed(sessionID)) }),
          abort.signal,
        ),
      )
      yield* Deferred.await(started)
      expect((yield* jobs.get(result.metadata.sessionId))?.status).toBe("running")

      abort.abort()
      expect(yield* Deferred.await(cancelled)).toBe(result.metadata.sessionId)
      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.info?.status).toBe("cancelled")
    }),
  )

  background.instance("removing the parent session cancels a running background task", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      onSpawn = () => Effect.never

      const result = yield* def.execute(
        { description: "inspect bug", prompt: "go", subagent_type: "general", background: true },
        runContext(chat.id, assistant.id, stubOps()),
      )

      yield* sessions.remove(chat.id)
      const waited = yield* jobs.wait({ id: result.metadata.sessionId, timeout: 1_000 })
      expect(waited.timedOut).toBe(false)
      expect(waited.info?.status).toBe("cancelled")
    }),
  )
})
