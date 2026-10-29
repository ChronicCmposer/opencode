import { describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Cause, Effect, Exit, Layer } from "effect"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { Session } from "@/session/session"
import { SessionDepth } from "@/session/depth"
import { SessionPrompt } from "@/session/prompt"
import { SessionPromptService } from "@/session/prompt-service"
import { spawnChildOnHub } from "@/session/worker/hub"
import type { SessionSpawnRequest } from "@/session/worker/subagent"
import { MessageID, PartID } from "@/session/schema"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { testEffect } from "../lib/effect"

const promptCalls: SessionPrompt.PromptInput[] = []

function reply(input: SessionPrompt.PromptInput, text: string): SessionV1.WithParts {
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
      modelID: ModelV2.ID.make("test-model"),
      providerID: ProviderV2.ID.make("test"),
      time: { created: Date.now() },
      finish: "stop",
    },
    parts: [{ id: PartID.ascending(), messageID: id, sessionID: input.sessionID, type: "text", text }],
  }
}

const promptLayer = Layer.mock(SessionPromptService.Service, {
  prompt: (input) =>
    Effect.sync(() => {
      promptCalls.push(input)
      return reply(input, `done:${input.sessionID}`)
    }),
})

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Session.node,
      EventV2Bridge.node,
      SessionProjector.node,
      CrossSpawnSpawner.node,
      InstanceStore.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalWorkspaces: false })],
      [
        InstanceBootstrap.node,
        Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void })),
      ],
    ],
  ).pipe(Layer.provideMerge(promptLayer)),
)

const promptInput = (text: string): SessionPrompt.PromptInput => ({
  messageID: MessageID.ascending(),
  sessionID: "ses_ignored" as never,
  agent: "general",
  parts: [{ type: "text", text }],
})

describe("Session.spawn", () => {
  it.instance("creates a child with metadata.depth = parent + 1 and prompts it", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "parent" })

      const spawned = yield* sessions.spawn({
        parentID: parent.id,
        prompt: promptInput("inspect"),
        type: "task",
        title: "child task",
      })

      expect(spawned.result.parts[0]).toMatchObject({ type: "text", text: `done:${spawned.childID}` })
      expect(promptCalls).toHaveLength(1)
      expect(promptCalls[0]?.sessionID).toBe(spawned.childID)
      expect(promptCalls[0]?.parts).toEqual([{ type: "text", text: "inspect" }])

      const child = yield* sessions.get(spawned.childID)
      expect(child.parentID).toBe(parent.id)
      expect(child.title).toBe("child task")
      expect(child.metadata?.depth).toBe(1)
    }),
  )

  it.instance("advances depth for each task generation", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const root = yield* sessions.create({ title: "root" })
      const first = yield* sessions.spawn({
        parentID: root.id,
        prompt: promptInput("one"),
        type: "task",
      })
      const second = yield* sessions.spawn({
        parentID: first.childID,
        prompt: promptInput("two"),
        type: "task",
      })
      const third = yield* sessions.spawn({
        parentID: second.childID,
        prompt: promptInput("three"),
        type: "task",
      })

      expect((yield* sessions.get(first.childID)).metadata?.depth).toBe(1)
      expect((yield* sessions.get(second.childID)).metadata?.depth).toBe(2)
      expect((yield* sessions.get(third.childID)).metadata?.depth).toBe(3)
    }),
  )

  it.instance("an existing child's stale depth is replaced by parent + 1", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "parent" })
      const child = yield* sessions.create({ parentID: parent.id, metadata: { depth: 9, keep: true } })

      yield* sessions.spawn({
        parentID: parent.id,
        childID: child.id,
        prompt: promptInput("rebase"),
        type: "task",
      })

      expect((yield* sessions.get(child.id)).metadata).toMatchObject({ depth: 1, keep: true })
    }),
  )

  it.instance("reuses an explicit childID and stamps its depth", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "parent" })
      const child = yield* sessions.create({ parentID: parent.id, title: "existing" })

      const spawned = yield* sessions.spawn({
        parentID: parent.id,
        childID: child.id,
        prompt: promptInput("resume"),
        type: "task",
      })

      expect(spawned.childID).toBe(child.id)
      expect(promptCalls[0]?.sessionID).toBe(child.id)
      expect((yield* sessions.get(child.id)).metadata?.depth).toBe(1)
    }),
  )

  it.instance("rejects an over-depth spawn without creating a child", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "deep parent", metadata: { depth: 1 } })

      const exit = yield* sessions
        .spawn({
          parentID: parent.id,
          prompt: promptInput("nested"),
          type: "task",
          depthLimit: 1,
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected depth rejection")
      const failure = Cause.squash(exit.cause)
      expect(failure).toBeInstanceOf(Error)
      if (!(failure instanceof Error)) throw new Error("expected Error defect")
      expect(failure.message).toBe(SessionDepth.subagentDepthMessage(1))
      expect(yield* sessions.children(parent.id)).toEqual([])
      expect(promptCalls).toHaveLength(0)
    }),
  )

  it.instance("allows a child whose depth equals the limit", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "parent at the boundary", metadata: { depth: 1 } })

      const spawned = yield* sessions.spawn({
        parentID: parent.id,
        prompt: promptInput("equal depth"),
        type: "task",
        depthLimit: 2,
      })

      expect((yield* sessions.get(spawned.childID)).metadata?.depth).toBe(2)
      expect(yield* sessions.children(parent.id)).toHaveLength(1)
    }),
  )

  it.instance("rejects an existing child before stamping its depth", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "deep parent", metadata: { depth: 1 } })
      const child = yield* sessions.create({ parentID: parent.id, metadata: { keep: true } })

      const exit = yield* sessions
        .spawn({
          parentID: parent.id,
          childID: child.id,
          prompt: promptInput("nested"),
          type: "task",
          depthLimit: 1,
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected depth rejection")
      expect((Cause.squash(exit.cause) as Error).message).toBe(SessionDepth.subagentDepthMessage(1))
      expect((yield* sessions.get(child.id)).metadata).toMatchObject({ keep: true })
      expect((yield* sessions.get(child.id)).metadata?.depth).toBeUndefined()
      expect(promptCalls).toHaveLength(0)
    }),
  )
})

describe("hub session.spawn authority", () => {
  it.instance("rejects an over-depth spawn with the locked message without creating a child", () =>
    Effect.gen(function* () {
      promptCalls.length = 0
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "deep parent", metadata: { depth: 1 } })
      const request: SessionSpawnRequest = {
        parentID: parent.id,
        type: "task",
        prompt: promptInput("nested"),
      }

      const exit = yield* spawnChildOnHub(sessions, 1, request).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected depth rejection")
      expect((Cause.squash(exit.cause) as Error).message).toBe(SessionDepth.subagentDepthMessage(1))
      expect(yield* sessions.children(parent.id)).toEqual([])
      expect(promptCalls).toHaveLength(0)
    }),
  )
})
