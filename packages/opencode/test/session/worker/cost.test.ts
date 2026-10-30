// Worker-mode priced-cost integration tests.
//
// The worker's reduced provider bootstrap is the only worker-specific cost
// input: `WorkerModelConfig.cost` is serialized into the reduced `Provider.Model`
// and then handed to the SAME `Session.getUsage` arithmetic the in-process path
// uses. These tests therefore prove the worker turn stores exactly what that
// shared arithmetic computes on the assistant message, the step-finish part, and
// the session total.
//
// Harness: the established `createE2EContext` worker E2E fixture plus the live
// `startMockProvider` SSE server, modelled on `layer-session.test.ts`'s
// "full-graph prompt path" suite.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
import { Usage } from "@opencode-ai/llm"
import { MessageID, PartID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionPromptService } from "@/session/prompt-service"
import { InstanceRef } from "@/effect/instance-ref"
import { Provider } from "@/provider/provider"
import type { WorkerModelConfig } from "@/session/worker/layer"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { workerSnapshot } from "../../lib/snapshot"
import { startMockProvider, type MockProviderUsage } from "../../lib/mock-provider"

type ModelCost = Provider.Model["cost"]

const modelConfig = (provider: { model: string; baseUrl: string; apiKey: string }, cost: ModelCost): WorkerModelConfig => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
  cost,
  // Large enough that the 250K-token case prices via `experimentalOver200K`
  // instead of tripping the reduced model's default 128K overflow compaction.
  limit: { context: 1_000_000, output: 0 },
})

/** Seed a session + one user text part through the worker's own services. */
const seed = (ctx: E2EContext, modelID: string, prompt: string) =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "priced-cost",
      agent: "build",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const user = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: user.id,
      sessionID: session.id,
      type: "text",
      text: prompt,
    } as never)
    return session.id
  }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance))

/** Drizzle stores row payloads as JSON text; parse once at this boundary. */
const rowData = (row: Record<string, unknown>): Record<string, unknown> =>
  typeof row.data === "string" ? (JSON.parse(row.data) as Record<string, unknown>) : (row.data as Record<string, unknown>)

/** The in-process reference: `getUsage` only reads the model's `cost`. */
const expectedFor = (cost: ModelCost, usage: MockProviderUsage) =>
  Session.getUsage({
    model: { cost } as unknown as Provider.Model,
    usage: new Usage({
      inputTokens: usage.input,
      outputTokens: usage.output,
      totalTokens: usage.input + usage.output,
      ...(usage.cacheRead === undefined ? {} : { cacheReadInputTokens: usage.cacheRead }),
    }),
  })

/**
 * Run one priced worker turn against a fresh temp directory + context, then
 * assert the three durable cost surfaces all match the shared `getUsage` result.
 */
async function runPricedTurn(cost: ModelCost, usage: MockProviderUsage): Promise<ReturnType<typeof expectedFor>> {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-cost-"))
  await using provider = startMockProvider({ chunks: ["ok"], usage })
  const ctx = createE2EContext({
    snapshot: workerSnapshot({ directory }),
    modelConfig: modelConfig(provider, cost),
  })
  try {
    const sessionID = await Effect.runPromise(seed(ctx, provider.model, "hi"))
    await Effect.runPromise(
      Effect.gen(function* () {
        const prompt = yield* SessionPromptService.Service
        yield* prompt.loop({ sessionID })
      }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance)),
    )

    const expected = expectedFor(cost, usage)

    const assistant = ctx.rows("message").map(rowData).find((message) => message.role === "assistant")
    expect(assistant).toBeDefined()
    expect(assistant!.cost as number).toBeCloseTo(expected.cost, 12)

    const stepFinish = ctx.rows("part").map(rowData).find((part) => part.type === "step-finish")
    expect(stepFinish).toBeDefined()
    expect(stepFinish!.cost as number).toBeCloseTo(expected.cost, 12)
    const stepTokens = stepFinish!.tokens as { cache: { read: number } }
    expect(stepTokens.cache.read).toBe(expected.tokens.cache.read)

    const sessionRow = ctx.rows("session")[0]
    expect(sessionRow).toBeDefined()
    expect(sessionRow!.cost as number).toBeCloseTo(expected.cost, 12)

    return expected
  } finally {
    await ctx.dispose()
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

describe("worker priced cost", () => {
  test("plain pricing stores the priced cost on message, step-finish and session", async () => {
    const expected = await runPricedTurn(
      { input: 3, output: 15, cache: { read: 0.3, write: 3.75 } },
      { input: 1000, output: 100, cacheRead: 400 },
    )
    expect(expected.cost).toBeGreaterThan(0)
  }, 60_000)

  test("tiered pricing uses the matching context tier", async () => {
    const expected = await runPricedTurn(
      {
        input: 3,
        output: 15,
        cache: { read: 0.3, write: 3.75 },
        tiers: [{ input: 6, output: 30, cache: { read: 0.6, write: 7.5 }, tier: { type: "context", size: 10 } }],
      },
      { input: 100, output: 10 },
    )
    expect(expected.cost).toBeGreaterThan(0)
  }, 60_000)

  test("over-200K pricing uses the over-200K price", async () => {
    const expected = await runPricedTurn(
      {
        input: 3,
        output: 15,
        cache: { read: 0.3, write: 3.75 },
        experimentalOver200K: { input: 4.5, output: 22.5, cache: { read: 0.45, write: 5.625 } },
      },
      { input: 250_000, output: 100 },
    )
    expect(expected.cost).toBeGreaterThan(0)
  }, 60_000)

  test("an unpriced model stays zero everywhere", async () => {
    const expected = await runPricedTurn(
      { input: 0, output: 0, cache: { read: 0, write: 0 } },
      { input: 10, output: 5 },
    )
    expect(expected.cost).toBe(0)
  }, 60_000)
})
