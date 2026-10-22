import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Provider } from "@/provider/provider"
import { ModelClient, llmModelClientLayer, reducedLlmLayer, reducedModelProviderLayer } from "@/session/worker/layer"
import type { WorkerModelConfig } from "@/session/worker/layer"
import { startMockProvider } from "./mock-provider"
import { workerSnapshot } from "./snapshot"
import {
  assertRequestParity,
  captureRequest,
  compareRequestParams,
  preparedRequestRun,
  requestParityFixtures,
} from "./request-parity"

const fixtures = requestParityFixtures

describe("request parity harness", () => {
  test("identical request params compare equal", async () => {
    for (const [name, model] of Object.entries(fixtures)) {
      const off = await captureRequest({ model, run: preparedRequestRun(model) })
      const on = await captureRequest({ model, run: preparedRequestRun(model) })
      expect(compareRequestParams(off, on)).toEqual([])
      expect(name.length).toBeGreaterThan(0)
    }
  })

  test("captures the real sampling params for every target model", async () => {
    const glm = await captureRequest({ model: fixtures["glm-4.7"], run: preparedRequestRun(fixtures["glm-4.7"]) })
    expect(glm.temperature).toBe(1)
    expect(glm.topP).toBeNull()
    expect(glm.topK).toBeNull()
    expect(glm.maxOutputTokens).toBe(32_000)
    expect(glm.providerOptions).toHaveProperty("zhipuai.thinking")

    const minimax = await captureRequest({
      model: fixtures["minimax-m2"],
      run: preparedRequestRun(fixtures["minimax-m2"]),
    })
    expect(minimax.temperature).toBe(1)
    expect(minimax.topP).toBe(0.95)
    expect(minimax.topK).toBe(20)

    const kimi = await captureRequest({
      model: fixtures["kimi-k2-thinking"],
      run: preparedRequestRun(fixtures["kimi-k2-thinking"]),
    })
    expect(kimi.temperature).toBe(1)
    expect(kimi.topP).toBeNull()
    expect(kimi.topK).toBeNull()

    const gemini = await captureRequest({
      model: fixtures["gemini-2.5"],
      run: preparedRequestRun(fixtures["gemini-2.5"]),
    })
    expect(gemini.temperature).toBe(1)
    expect(gemini.topP).toBe(0.95)
    expect(gemini.topK).toBe(64)
    expect(gemini.providerOptions).toHaveProperty("google.thinkingConfig")

    const deepseek = await captureRequest({ model: fixtures.deepseek, run: preparedRequestRun(fixtures.deepseek) })
    expect(deepseek.temperature).toBeNull()
    expect(deepseek.topP).toBeNull()
  })

  test("detects a planted temperature divergence with the exact field", async () => {
    const model = fixtures.deepseek
    const off = await captureRequest({ model, run: preparedRequestRun(model) })
    const on = await captureRequest({
      model,
      run: preparedRequestRun(model, { override: (request) => ({ ...request, temperature: 1.0 }) }),
    })

    const diffs = compareRequestParams(off, on)
    expect(diffs).toEqual([{ field: "temperature", off: null, on: 1 }])
    expect(() => assertRequestParity(off, on, "deepseek")).toThrow(/temperature/)
  })

  test("detects a planted providerOptions divergence", async () => {
    const model = fixtures["gemini-2.5"]
    const off = await captureRequest({ model, run: preparedRequestRun(model) })
    const on = await captureRequest({
      model,
      run: preparedRequestRun(model, { override: (request) => ({ ...request, providerOptions: {} }) }),
    })

    const diffs = compareRequestParams(off, on)
    expect(diffs.some((diff) => diff.field.startsWith("providerOptions.google"))).toBe(true)
  })

  test("assertRequestParity is silent when params are equal", async () => {
    const model = fixtures["glm-4.7"]
    const off = await captureRequest({ model, run: preparedRequestRun(model) })
    expect(() => assertRequestParity(off, off)).not.toThrow()
  })

  test("captureRequest fails loudly when run never issues a request", async () => {
    await expect(captureRequest({ model: fixtures["glm-4.7"], run: () => {} })).rejects.toThrow(/never issued/)
  })
})

// ---------------------------------------------------------------------------
// Phase 8.1 — the worker (flag-ON) reduced model must send the SAME request
// params as flag-OFF. The "gibberish loop" came from the worker's reduced
// provider inventing its own descriptor (`capabilities.temperature: true`, a
// hardcoded 4096 output cap, dropped reasoning/interleaved/variants/options), so
// the worker forced `temperature: 1.0` where flag-OFF omitted it and truncated
// turns to 4096 tokens. Both describe the same real model, so their outgoing
// request params must be identical.
//
// The worker side is the REAL production descriptor: the `Provider.Model` the
// worker's `reducedModelProviderLayer` serves from the serialized
// `WorkerModelConfig`. Feeding it through the same recording seam the flag-OFF
// baseline uses (`preparedRequestRun`) makes the two sides directly comparable.
// A final test drives the full worker `LLM` graph (`reducedLlmLayer`) against the
// mock provider, proving the identical params reach the wire.
// ---------------------------------------------------------------------------

/** Build the worker's serialized descriptor from a flag-OFF fixture model. */
const workerConfigFor = (model: Provider.Model): WorkerModelConfig => ({
  providerID: model.providerID,
  modelID: model.id,
  apiId: model.api.id,
  npm: model.api.npm,
  baseUrl: model.api.url,
  apiKey: "test-key",
  headers: { ...model.headers },
  capabilities: model.capabilities,
  limit: model.limit,
  options: model.options,
  variants: model.variants,
})

/** The exact `Provider.Model` the worker's reduced provider serves for `config`. */
const workerReducedModel = (config: WorkerModelConfig): Promise<Provider.Model> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const provider = yield* Provider.Service
      return yield* provider.getModel(config.providerID as never, config.modelID as never)
    }).pipe(Effect.provide(reducedModelProviderLayer(config))),
  )

describe("Phase 8.1 — worker (flag-ON) request parity with flag-OFF", () => {
  test("the worker reduced model sends identical params for every target model", async () => {
    for (const [name, model] of Object.entries(fixtures)) {
      const reduced = await workerReducedModel(workerConfigFor(model))
      const off = await captureRequest({ model, run: preparedRequestRun(model) })
      const on = await captureRequest({ model: reduced, run: preparedRequestRun(reduced) })
      // Fail with the exact divergent field(s) when the worker descriptor drifts.
      assertRequestParity(off, on, name)
      expect(compareRequestParams(off, on)).toEqual([])
    }
  })

  test("temperature is omitted for a temperature-less descriptor (deepseek)", async () => {
    const model = fixtures.deepseek
    const reduced = await workerReducedModel(workerConfigFor(model))
    const off = await captureRequest({ model, run: preparedRequestRun(model) })
    const on = await captureRequest({ model: reduced, run: preparedRequestRun(reduced) })

    expect(model.capabilities.temperature).toBe(false)
    // The old reduced provider forced `temperature: true`, so `transform.temperature`
    // produced a value; flag-OFF omits it. The fix keeps it omitted.
    expect(off.temperature).toBeNull()
    expect(on.temperature).toBeNull()
  })

  test("maxOutputTokens uses the descriptor limit, not the synthesized 4096 cap", async () => {
    const model = fixtures["glm-4.7"]
    const reduced = await workerReducedModel(workerConfigFor(model))
    const off = await captureRequest({ model, run: preparedRequestRun(model) })
    const on = await captureRequest({ model: reduced, run: preparedRequestRun(reduced) })

    // min(limit.output 131072, OUTPUT_TOKEN_MAX 32000) — the descriptor limit wins.
    expect(on.maxOutputTokens).toBe(32_000)
    expect(on.maxOutputTokens).not.toBe(4_096)
    expect(on.maxOutputTokens).toBe(off.maxOutputTokens)
  })

  test("reasoning/thinking options survive the descriptor round-trip", async () => {
    const glm = fixtures["glm-4.7"]
    const glmReduced = await workerReducedModel(workerConfigFor(glm))
    const glmOn = await captureRequest({ model: glmReduced, run: preparedRequestRun(glmReduced) })
    expect(glmOn.providerOptions).toHaveProperty("zhipuai.thinking")

    const gemini = fixtures["gemini-2.5"]
    const geminiReduced = await workerReducedModel(workerConfigFor(gemini))
    const geminiOn = await captureRequest({ model: geminiReduced, run: preparedRequestRun(geminiReduced) })
    expect(geminiOn.providerOptions).toHaveProperty("google.thinkingConfig")
  })

  test("the real worker LLM graph sends the same params on the wire (mock provider)", async () => {
    await using provider = startMockProvider({ text: "ok" })
    const model = fixtures.deepseek
    const config: WorkerModelConfig = {
      ...workerConfigFor(model),
      baseUrl: provider.baseUrl,
      apiKey: provider.apiKey,
    }

    // Drive the production worker graph: `reducedLlmLayer` is the real reduced
    // `LLM` node (real `LLMRequestPrep.prepare` over the reduced model).
    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* ModelClient
        return yield* client.complete({ sessionID: "session", messages: [{ role: "user", content: "hi" }] })
      }).pipe(
        Effect.provide(llmModelClientLayer(config).pipe(Layer.provide(reducedLlmLayer(workerSnapshot(), config)))),
      ),
    )

    const off = await captureRequest({ model, run: preparedRequestRun(model) })
    const body = provider.requests[0]?.body as Record<string, unknown> | undefined
    expect(body).toBeDefined()
    // Omitted temperature on the wire (flag-OFF omits it) and the descriptor's
    // output limit (not the old 4096 cap).
    expect(body?.temperature).toBe(off.temperature ?? undefined)
    expect(body?.max_tokens ?? body?.max_completion_tokens).toBe(off.maxOutputTokens)
    expect(body?.max_tokens ?? body?.max_completion_tokens).not.toBe(4_096)
  })
})
