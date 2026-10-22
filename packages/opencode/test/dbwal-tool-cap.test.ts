import { describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Effect, Layer, Schema } from "effect"
import { Agent } from "@/agent/agent"
import { MessageID, SessionID } from "@/session/schema"
import { Tool } from "@/tool/tool"
import { Truncate } from "@/tool/truncate"
import { testEffect } from "./lib/effect"

const fakeAgent = Layer.mock(Agent.Service, {
  get: () => Effect.succeed({ permission: [] } as unknown as Agent.Info),
})

const it = testEffect(
  LayerNode.compile(LayerNode.group([Truncate.node, FSUtil.node, Agent.node]), [[Agent.node, fakeAgent]]),
)

function makeCtx(): Tool.Context {
  return {
    sessionID: SessionID.descending(),
    messageID: MessageID.ascending(),
    agent: "build",
    abort: new AbortController().signal,
    messages: [],
    metadata() {
      return Effect.void
    },
    ask() {
      return Effect.void
    },
  }
}

const oversized = "x".repeat(Truncate.MAX_TOOL_OUTPUT_BYTES + 1)

describe("dbwal tool output cap", () => {
  it.live("externalizes an oversized tool result and references it by path", () =>
    Effect.gen(function* () {
      const info = yield* Tool.define(
        "dbwal-big",
        Effect.succeed({
          description: "big",
          parameters: Schema.Struct({}),
          execute: () =>
            Effect.succeed({
              title: "big",
              output: "done",
              metadata: { diff: oversized } as Record<string, any>,
            }),
        }),
      )
      const tool = yield* info.init()
      const execute = tool.execute as unknown as (args: unknown, ctx: Tool.Context) => ReturnType<typeof tool.execute>
      const result = yield* execute({}, makeCtx())

      // Persisted result is bounded and carries a reference to the full payload.
      expect(result.metadata.diff).toBeUndefined()
      expect(result.metadata.truncated).toBe(true)
      const outputPath = result.metadata.outputPath
      expect(typeof outputPath).toBe("string")
      expect(outputPath).toContain("tool_")

      // The full payload is retained on disk, parseable, and complete.
      const fs = yield* FSUtil.Service
      const written = JSON.parse(yield* fs.readFileString(outputPath as string))
      expect(written.metadata.diff).toBe(oversized)
    }),
  )

  it.live("passes a small tool result through unchanged", () =>
    Effect.gen(function* () {
      const info = yield* Tool.define(
        "dbwal-small",
        Effect.succeed({
          description: "small",
          parameters: Schema.Struct({}),
          execute: () =>
            Effect.succeed({
              title: "small",
              output: "hello",
              metadata: { diff: "small" } as Record<string, any>,
            }),
        }),
      )
      const tool = yield* info.init()
      const execute = tool.execute as unknown as (args: unknown, ctx: Tool.Context) => ReturnType<typeof tool.execute>
      const result = yield* execute({}, makeCtx())

      expect(result.output).toBe("hello")
      expect(result.metadata.diff).toBe("small")
      expect(result.metadata.outputPath).toBeUndefined()
      expect(result.metadata.truncated).toBe(false)
    }),
  )

  it.live("boundResult keeps small results byte-identical and externalizes large ones", () =>
    Effect.gen(function* () {
      const svc = yield* Truncate.Service
      const fs = yield* FSUtil.Service

      const small = yield* Truncate.boundResult(svc, { output: "ok", metadata: { a: 1 } })
      expect(small.output).toBe("ok")
      expect(small.metadata).toEqual({ a: 1, truncated: false })

      const large = yield* Truncate.boundResult(svc, { output: "ok", metadata: { diff: oversized } })
      expect(large.metadata.truncated).toBe(true)
      expect(typeof large.metadata.outputPath).toBe("string")
      const written = JSON.parse(yield* fs.readFileString(large.metadata.outputPath as string))
      expect(written.metadata.diff).toBe(oversized)
    }),
  )
})
