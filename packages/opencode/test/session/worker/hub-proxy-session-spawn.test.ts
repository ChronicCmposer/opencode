import { describe, expect, test } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect } from "effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/hub-proxy-wire"
import { sessionSpawnProxyLayer } from "@/session/worker/layer"
import { SessionSpawnProxyService, type SessionSpawnRequest } from "@/session/worker/subagent"
import { MessageID, PartID } from "@/session/schema"

function reply(childID: SessionSpawnRequest["parentID"], text: string): SessionV1.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: MessageID.ascending(),
      sessionID: childID,
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
    parts: [{ id: PartID.ascending(), messageID: id, sessionID: childID, type: "text", text }],
  }
}

const request: SessionSpawnRequest = {
  parentID: "ses_parent" as never,
  childID: "ses_child" as never,
  type: "task",
  title: "child",
  metadata: { parentSessionId: "ses_parent" },
  prompt: {
    messageID: MessageID.ascending(),
    sessionID: "ses_child" as never,
    agent: "general",
    parts: [{ type: "text", text: "do the thing" }],
  },
}

const spawnWith = (client: ReturnType<typeof createHubProxyClient>) =>
  Effect.gen(function* () {
    const proxy = yield* SessionSpawnProxyService
    return yield* proxy.spawn(request)
  }).pipe(Effect.provide(sessionSpawnProxyLayer(client)))

describe("hub-proxy session.spawn", () => {
  test("a worker spawn crosses a real MessageChannel and returns the child result", async () => {
    const channel = new MessageChannel()
    const seen: SessionSpawnRequest[] = []
    const disposeServer = serveHubProxy(
      {
        "session.spawn": (value) => {
          const spawn = value as SessionSpawnRequest
          seen.push(spawn)
          return { childID: spawn.childID, result: reply(spawn.childID!, "spawned over the wire") }
        },
      },
      channel.port2,
    )
    const client = createHubProxyClient(channel.port1)
    try {
      const spawned = await Effect.runPromise(spawnWith(client))
      expect(seen).toHaveLength(1)
      expect(seen[0]?.parentID).toBe(request.parentID)
      expect(seen[0]?.childID).toBe(request.childID)
      expect(seen[0]?.prompt.parts).toEqual([{ type: "text", text: "do the thing" }])
      expect(spawned.childID).toBe(request.childID!)
      expect(spawned.result.parts[0]).toMatchObject({ type: "text", text: "spawned over the wire" })
    } finally {
      client.dispose()
      disposeServer()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a missing spawn authority fails loud (never an unknown-method silence)", async () => {
    const channel = new MessageChannel()
    const disposeServer = serveHubProxy({}, channel.port2)
    const client = createHubProxyClient(channel.port1)
    try {
      const exit = await Effect.runPromise(spawnWith(client).pipe(Effect.exit))
      expect(exit._tag).toBe("Failure")
      if (exit._tag !== "Failure") return
      expect(String(exit.cause)).toContain('unknown hub-proxy method "session.spawn"')
    } finally {
      client.dispose()
      disposeServer()
      channel.port1.close()
      channel.port2.close()
    }
  })

  test("a disposed transport rejects loudly", async () => {
    const channel = new MessageChannel()
    const disposeServer = serveHubProxy(
      { "session.spawn": (value) => ({ childID: (value as SessionSpawnRequest).childID, result: reply("ses_child" as never, "late") }) },
      channel.port2,
    )
    const client = createHubProxyClient(channel.port1)
    client.dispose()
    try {
      const exit = await Effect.runPromise(spawnWith(client).pipe(Effect.exit))
      expect(exit._tag).toBe("Failure")
      if (exit._tag !== "Failure") return
      expect(String(exit.cause)).toContain("hub proxy client disposed")
    } finally {
      disposeServer()
      channel.port1.close()
      channel.port2.close()
    }
  })
})
