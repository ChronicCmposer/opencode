import { describe, expect, test } from "bun:test"
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
import { SessionPrompt, assertSpawnDepth } from "@/session/prompt"
import { MessageID, PartID } from "@/session/schema"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { testEffect } from "../../lib/effect"

function reply(input: SessionPrompt.PromptInput): SessionV1.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: input.messageID ?? MessageID.ascending(),
      sessionID: input.sessionID,
      mode: "general",
      agent: "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: ModelV2.ID.make("test-model"),
      providerID: ProviderV2.ID.make("test"),
      time: { created: Date.now() },
      finish: "stop",
    },
    parts: [{ id: PartID.ascending(), messageID: id, sessionID: input.sessionID, type: "text", text: "ok" }],
  }
}

const promptLayer = Layer.mock(SessionPrompt.Service, {
  prompt: (input) => Effect.sync(() => reply(input)),
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

describe("explicit metadata.depth", () => {
  it.instance("implicit children (create + prompt) carry no depth and stay at 0", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "root" })
      const implicit = yield* sessions.create({ parentID: parent.id, metadata: { delegated: true } })

      expect(implicit.metadata?.depth).toBeUndefined()
      expect(SessionDepth.spawnDepth(implicit.metadata)).toBe(0)
    }),
  )

  it.instance("a delegate-style child (create + prompt) is depth 0 and passes a limit of 0", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const parent = yield* sessions.create({ title: "root" })
      const delegated = yield* sessions.create({ parentID: parent.id, metadata: { delegated: true } })

      expect(SessionDepth.spawnDepth(delegated.metadata)).toBe(0)
      expect(() => assertSpawnDepth(delegated.metadata, 0)).not.toThrow()
      // Lineage still records the edge for cascade cancellation.
      expect((yield* sessions.children(parent.id)).map((child) => child.id)).toContain(delegated.id)
    }),
  )

  it.instance("task children advance 1, 2, 3 from the parent metadata", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const root = yield* sessions.create({ title: "root" })
      const spawn = (parentID: Session.Info["id"]) =>
        sessions.spawn({
          parentID,
          type: "task",
          prompt: { sessionID: "ses_ignored" as never, parts: [{ type: "text", text: "go" }] },
        })
      const first = yield* spawn(root.id)
      const second = yield* spawn(first.childID)
      const third = yield* spawn(second.childID)

      expect(SessionDepth.spawnDepth((yield* sessions.get(first.childID)).metadata)).toBe(1)
      expect(SessionDepth.spawnDepth((yield* sessions.get(second.childID)).metadata)).toBe(2)
      expect(SessionDepth.spawnDepth((yield* sessions.get(third.childID)).metadata)).toBe(3)
    }),
  )
})

describe("subagent_depth enforcement", () => {
  test("depth 0 is allowed at limit 0 (equal depth is not exceeded)", () => {
    expect(() => assertSpawnDepth({}, 0)).not.toThrow()
    expect(() => assertSpawnDepth({ depth: 0 }, 0)).not.toThrow()
  })

  test("a child above the limit fails with the exact task-tool message", () => {
    expect(() => assertSpawnDepth({ depth: 1 }, 0)).toThrow(
      `Subagent depth limit reached (0). Increase "subagent_depth" to allow nested subagents.`,
    )
    expect(() => assertSpawnDepth({ depth: 2 }, 1)).toThrow(
      `Subagent depth limit reached (1). Increase "subagent_depth" to allow nested subagents.`,
    )
    expect(() => assertSpawnDepth({ depth: 4 }, 3)).toThrow(
      `Subagent depth limit reached (3). Increase "subagent_depth" to allow nested subagents.`,
    )
  })

  test("depth equal to the limit is accepted", () => {
    expect(() => assertSpawnDepth({ depth: 1 }, 1)).not.toThrow()
    expect(() => assertSpawnDepth({ depth: 3 }, 3)).not.toThrow()
  })

  it.instance("an over-depth task spawn persists no child and leaves delegates at depth 0", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const root = yield* sessions.create({ title: "root" })
      const delegate = yield* sessions.create({ parentID: root.id, metadata: { delegated: true } })
      const deep = yield* sessions.create({ title: "deep", metadata: { depth: 1 } })

      const exit = yield* sessions
        .spawn({
          parentID: deep.id,
          prompt: { sessionID: "ses_ignored" as never, parts: [{ type: "text", text: "go" }] },
          type: "task",
          depthLimit: 1,
        })
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isSuccess(exit)) throw new Error("expected depth rejection")
      expect((Cause.squash(exit.cause) as Error).message).toBe(SessionDepth.subagentDepthMessage(1))
      expect(yield* sessions.children(deep.id)).toEqual([])
      expect((yield* sessions.get(delegate.id)).metadata?.depth).toBeUndefined()
      expect(SessionDepth.spawnDepth((yield* sessions.get(delegate.id)).metadata)).toBe(0)
      expect((yield* sessions.children(root.id)).map((child) => child.id)).toContain(delegate.id)
    }),
  )
})
