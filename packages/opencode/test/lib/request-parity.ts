// Phase 0 — request parity harness.
//
// Captures the exact parameters that reach the AI SDK language-model call for a
// run, so the worker (flag-ON) request can be asserted equal to the flag-OFF
// request for the same model. The capture seam is a recording `LanguageModelV3`:
// `captureRequest` hands the runner a model whose `doGenerate` / `doStream`
// record the `LanguageModelV3CallOptions` before returning an empty result. That
// is the same boundary `streamText` / `generateText` cross in production, so the
// snapshot is the outgoing request rather than an approximation of it.
//
// The harness is a pure instrument: it does not know how opencode builds a
// request. `preparedRequest` / `preparedRequestRun` are provided so callers can
// drive the real flag-OFF composition (`LLMRequestPrep.prepare` +
// `ProviderTransform.providerOptions`) through the recorder, and plant
// divergences with `PreparedRequestRunOptions.override`.

import type {
  LanguageModelV3,
  LanguageModelV3CallOptions,
  LanguageModelV3GenerateResult,
  LanguageModelV3StreamPart,
} from "@ai-sdk/provider"
import { streamText } from "ai"
import { Effect } from "effect"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import type { Agent } from "@/agent/agent"
import type { Plugin } from "@/plugin"
import type { Provider } from "@/provider/provider"
import type { RuntimeFlags } from "@/effect/runtime-flags"
import { LLMRequestPrep } from "@/session/llm/request"
import { ProviderTransform } from "@/provider/transform"

/** The normalized request parameters that reach the provider. */
export interface RequestSnapshot {
  readonly temperature: number | null
  readonly topP: number | null
  readonly topK: number | null
  readonly maxOutputTokens: number | null
  readonly providerOptions: Record<string, unknown>
  readonly headers: Record<string, string | null>
}

/** One leaf difference between two snapshots, named by its dotted path. */
export interface RequestDiff {
  readonly field: string
  readonly off: unknown
  readonly on: unknown
}

/** Issues a request against the recording language model the harness supplies. */
export type RequestRun = (language: LanguageModelV3) => Promise<void> | void

export interface RequestCaptureInput {
  readonly model: Provider.Model
  readonly run: RequestRun
}

/**
 * Capture the outgoing request parameters for one run of `model`.
 *
 * Fails loud when `run` never reaches the language model: an empty capture is a
 * broken instrument, not a passing parity check.
 */
export async function captureRequest(input: RequestCaptureInput): Promise<RequestSnapshot> {
  const recorder = createRequestRecorder()
  await input.run(recorder.model)
  const captured = recorder.captured()
  if (captured === undefined) {
    throw new Error(
      `request-parity: run() never issued a language-model request for ${input.model.providerID}/${input.model.id}`,
    )
  }
  return normalizeSnapshot(captured)
}

/** Every leaf where `off` and `on` differ, keyed by dotted field path. */
export function compareRequestParams(off: RequestSnapshot, on: RequestSnapshot): RequestDiff[] {
  const diffs: RequestDiff[] = []
  collectDiffs("", off, on, diffs)
  return diffs
}

/** Render a diff list as a human-readable failure message. */
export function formatRequestDiff(diffs: ReadonlyArray<RequestDiff>, label = "request params"): string {
  const lines = diffs.map((diff) => `  - ${diff.field}: off=${stringify(diff.off)} on=${stringify(diff.on)}`)
  return `request parity failed for ${label}:\n${lines.join("\n")}`
}

/** Throw with the exact divergent fields when two snapshots differ. */
export function assertRequestParity(off: RequestSnapshot, on: RequestSnapshot, label = "request params"): void {
  const diffs = compareRequestParams(off, on)
  if (diffs.length > 0) throw new Error(formatRequestDiff(diffs, label))
}

// --- Recording seam ---------------------------------------------------------

interface RequestRecorder {
  readonly model: LanguageModelV3
  readonly captured: () => LanguageModelV3CallOptions | undefined
}

const emptyUsage = {
  inputTokens: { total: 0, noCache: 0, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 0, text: 0, reasoning: 0 },
}

function emptyGenerateResult(): LanguageModelV3GenerateResult {
  return {
    content: [{ type: "text", text: "ok" }],
    finishReason: { unified: "stop", raw: "stop" },
    usage: emptyUsage,
    warnings: [],
  }
}

function emptyStream(): ReadableStream<LanguageModelV3StreamPart> {
  return new ReadableStream<LanguageModelV3StreamPart>({
    start(controller) {
      controller.enqueue({ type: "stream-start", warnings: [] })
      controller.enqueue({ type: "text-start", id: "0" })
      controller.enqueue({ type: "text-delta", id: "0", delta: "ok" })
      controller.enqueue({ type: "text-end", id: "0" })
      controller.enqueue({ type: "finish", finishReason: { unified: "stop", raw: "stop" }, usage: emptyUsage })
      controller.close()
    },
  })
}

function createRequestRecorder(): RequestRecorder {
  let captured: LanguageModelV3CallOptions | undefined
  const model: LanguageModelV3 = {
    specificationVersion: "v3",
    provider: "request-parity",
    modelId: "request-parity",
    supportedUrls: {},
    async doGenerate(options) {
      captured = options
      return emptyGenerateResult()
    },
    async doStream(options) {
      captured = options
      return { stream: emptyStream() }
    },
  }
  return { model, captured: () => captured }
}

// --- Normalization + diffing ------------------------------------------------

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function sortDeep(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortDeep)
  if (!isPlainObject(value)) return value
  return Object.fromEntries(Object.keys(value).toSorted().map((key) => [key, sortDeep(value[key])]))
}

function normalizeHeaders(headers: LanguageModelV3CallOptions["headers"]): Record<string, string | null> {
  if (headers === undefined) return {}
  return Object.fromEntries(Object.keys(headers).toSorted().map((key) => [key, headers[key] ?? null]))
}

function normalizeSnapshot(call: LanguageModelV3CallOptions): RequestSnapshot {
  return {
    temperature: call.temperature ?? null,
    topP: call.topP ?? null,
    topK: call.topK ?? null,
    maxOutputTokens: call.maxOutputTokens ?? null,
    providerOptions: (sortDeep(call.providerOptions ?? {}) ?? {}) as Record<string, unknown>,
    headers: normalizeHeaders(call.headers),
  }
}

function deepEqual(off: unknown, on: unknown): boolean {
  if (Object.is(off, on)) return true
  if (Array.isArray(off) && Array.isArray(on)) {
    return off.length === on.length && off.every((value, index) => deepEqual(value, on[index]))
  }
  if (isPlainObject(off) && isPlainObject(on)) {
    const offKeys = Object.keys(off)
    const onKeys = Object.keys(on)
    return offKeys.length === onKeys.length && offKeys.every((key) => key in on && deepEqual(off[key], on[key]))
  }
  return false
}

function collectDiffs(path: string, off: unknown, on: unknown, out: RequestDiff[]): void {
  if (isPlainObject(off) && isPlainObject(on)) {
    const keys = [...new Set([...Object.keys(off), ...Object.keys(on)])].toSorted()
    for (const key of keys) collectDiffs(path ? `${path}.${key}` : key, off[key], on[key], out)
    return
  }
  if (!deepEqual(off, on)) out.push({ field: path, off, on })
}

function stringify(value: unknown): string {
  return value === undefined ? "undefined" : JSON.stringify(value)
}

// --- Flag-OFF request composition -------------------------------------------

/** The AI-SDK-shaped request options flag-OFF would send for a model. */
export interface PreparedRequest {
  readonly temperature?: number
  readonly topP?: number
  readonly topK?: number
  readonly maxOutputTokens?: number
  readonly providerOptions: Record<string, unknown>
  readonly headers: Record<string, string>
}

export interface PreparedRequestRunOptions {
  readonly agent?: Partial<Pick<Agent.Info, "temperature" | "topP" | "options" | "prompt">>
  readonly flags?: Partial<RuntimeFlags.Info>
  /** Applied after production params are computed; used to plant divergences. */
  readonly override?: (request: PreparedRequest) => PreparedRequest
}

const noopPlugin = {
  trigger: (_name: string, _input: unknown, output: unknown) => Effect.succeed(output),
  list: () => Effect.succeed([]),
  init: () => Effect.void,
} as unknown as Plugin.Interface

/** Compute the flag-OFF request options by running the real `prepare` path. */
export async function preparedRequest(
  model: Provider.Model,
  options: PreparedRequestRunOptions = {},
): Promise<PreparedRequest> {
  const prepared = await Effect.runPromise(
    LLMRequestPrep.prepare({
      user: { id: "request-parity", role: "user", model: { variant: undefined } } as unknown as SessionV1.User,
      sessionID: "request-parity-session",
      model,
      agent: { name: "request-parity", mode: "primary", ...options.agent } as unknown as Agent.Info,
      system: [],
      messages: [{ role: "user", content: "parity" }],
      tools: {},
      provider: {
        id: model.providerID,
        name: model.providerID,
        source: "config",
        env: [],
        options: {},
        models: { [model.id]: model },
      } as Provider.Info,
      auth: undefined,
      plugin: noopPlugin,
      flags: { ...options.flags } as RuntimeFlags.Info,
      isWorkflow: false,
    }),
  )

  return {
    temperature: prepared.params.temperature,
    topP: prepared.params.topP,
    topK: prepared.params.topK,
    maxOutputTokens: prepared.params.maxOutputTokens,
    providerOptions: ProviderTransform.providerOptions(model, prepared.params.options),
    headers: prepared.headers,
  }
}

/** A `RequestRun` that issues the flag-OFF request through the AI SDK. */
export function preparedRequestRun(model: Provider.Model, options: PreparedRequestRunOptions = {}): RequestRun {
  return async (language) => {
    const base = await preparedRequest(model, options)
    const request = options.override ? options.override(base) : base
    await streamText({
      model: language,
      prompt: "parity",
      temperature: request.temperature,
      topP: request.topP,
      topK: request.topK,
      maxOutputTokens: request.maxOutputTokens,
      providerOptions: request.providerOptions as never,
      headers: request.headers,
    }).consumeStream()
  }
}

// --- Offline model fixtures (real models.dev capabilities/limits/options) ----

const textOnly = { text: true, audio: false, image: false, video: false, pdf: false }

interface FixtureInput {
  readonly providerID: string
  readonly apiId: string
  readonly npm: string
  readonly url: string
  readonly name: string
  readonly capabilities: Provider.Model["capabilities"]
  readonly limit: { readonly context: number; readonly output: number }
  readonly cost: { readonly input: number; readonly output: number; readonly cacheRead?: number }
  readonly releaseDate: string
}

function modelFixture(input: FixtureInput): Provider.Model {
  return {
    id: ModelV2.ID.make(input.apiId),
    providerID: ProviderV2.ID.make(input.providerID),
    api: { id: input.apiId, url: input.url, npm: input.npm },
    name: input.name,
    capabilities: input.capabilities,
    cost: {
      input: input.cost.input,
      output: input.cost.output,
      cache: { read: input.cost.cacheRead ?? 0, write: 0 },
    },
    limit: { context: input.limit.context, output: input.limit.output },
    status: "active",
    options: {},
    headers: {},
    release_date: input.releaseDate,
    variants: {},
  }
}

/**
 * The target models the worker fix must preserve, with their real catalog
 * capabilities/limits. `deepseek` carries `capabilities.temperature: false`
 * because its reasoner rejects sampling controls, so flag-OFF must omit
 * temperature — the exact field the worker bug forced to `1.0`.
 */
export const requestParityFixtures: Record<string, Provider.Model> = {
  "glm-4.7": modelFixture({
    providerID: "zhipuai",
    apiId: "glm-4.7",
    npm: "@ai-sdk/openai-compatible",
    url: "https://open.bigmodel.cn/api/paas/v4",
    name: "GLM-4.7",
    capabilities: {
      temperature: true,
      reasoning: true,
      attachment: false,
      toolcall: true,
      input: textOnly,
      output: textOnly,
      interleaved: { field: "reasoning_content" },
    },
    limit: { context: 204_800, output: 131_072 },
    cost: { input: 0.6, output: 2.2 },
    releaseDate: "2025-12-22",
  }),
  "minimax-m2": modelFixture({
    providerID: "minimax",
    apiId: "MiniMax-M2",
    npm: "@ai-sdk/anthropic",
    url: "https://api.minimax.io/anthropic/v1",
    name: "MiniMax M2",
    capabilities: {
      temperature: true,
      reasoning: true,
      attachment: false,
      toolcall: true,
      input: textOnly,
      output: textOnly,
      interleaved: false,
    },
    limit: { context: 196_608, output: 128_000 },
    cost: { input: 0.3, output: 1.2 },
    releaseDate: "2025-10-27",
  }),
  "kimi-k2-thinking": modelFixture({
    providerID: "moonshotai",
    apiId: "kimi-k2-thinking",
    npm: "@ai-sdk/openai-compatible",
    url: "https://api.moonshot.ai/v1",
    name: "Kimi K2 Thinking",
    capabilities: {
      temperature: true,
      reasoning: true,
      attachment: false,
      toolcall: true,
      input: textOnly,
      output: textOnly,
      interleaved: { field: "reasoning_content" },
    },
    limit: { context: 262_144, output: 262_144 },
    cost: { input: 0.6, output: 2.5, cacheRead: 0.15 },
    releaseDate: "2025-11-06",
  }),
  "gemini-2.5": modelFixture({
    providerID: "google",
    apiId: "gemini-2.5-pro",
    npm: "@ai-sdk/google",
    url: "https://generativelanguage.googleapis.com/v1beta",
    name: "Gemini 2.5 Pro",
    capabilities: {
      temperature: true,
      reasoning: true,
      attachment: true,
      toolcall: true,
      input: { text: true, audio: true, image: true, video: true, pdf: true },
      output: textOnly,
      interleaved: false,
    },
    limit: { context: 1_048_576, output: 65_536 },
    cost: { input: 1.25, output: 10, cacheRead: 0.31 },
    releaseDate: "2025-06-17",
  }),
  deepseek: modelFixture({
    providerID: "deepseek",
    apiId: "deepseek-reasoner",
    npm: "@ai-sdk/openai-compatible",
    url: "https://api.deepseek.com",
    name: "DeepSeek Reasoner",
    capabilities: {
      temperature: false,
      reasoning: true,
      attachment: false,
      toolcall: true,
      input: textOnly,
      output: textOnly,
      interleaved: { field: "reasoning_content" },
    },
    limit: { context: 1_000_000, output: 384_000 },
    cost: { input: 0.14, output: 0.28, cacheRead: 0.0028 },
    releaseDate: "2026-04-24",
  }),
}
