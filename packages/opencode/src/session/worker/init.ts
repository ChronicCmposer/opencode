// Production producer for the session-worker `workerData` payload
// (`{ snapshot, env, model }`) and the serializable pieces it is built from.
//
// The hub ships one structured-cloned `SessionWorkerInit` per worker generation:
//   * `snapshot` — {@link buildWorkerSnapshot} (plan decisions 67/68);
//   * `env` — an immutable overlay merged with `broker/env.ts` (decision 68);
//   * `model` — a plain-data model bootstrap ({@link resolveWorkerModel}).
//
// `workerData` crosses an isolate boundary, so it must be structured-clone
// friendly: functions (custom `fetch`, provider loaders, header callbacks) can
// never cross. {@link resolveWorkerModel} rejects them with a typed error instead
// of letting them silently vanish.

import { Effect, Schema } from "effect"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"

import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { Provider, parseModel } from "@/provider/provider"
import { mergeEnv } from "@/session/broker/env"

import type {
  SerializedModelCapabilities,
  SerializedModelLimit,
  SessionWorkerCapabilities,
  SessionWorkerInit,
  SessionWorkerModel,
} from "./session-worker"
import type { WorkerEnv } from "./layer"
import type {
  ChatHeadersOutput,
  ChatParamsOutput,
  ResolvedChatHeaders,
  ResolvedChatParams,
  WorkerPlugin,
  WorkerPluginHooks,
} from "./plugin-host"
import { buildWorkerSnapshot, type BuildWorkerSnapshotOptions } from "./snapshot"

// ---------------------------------------------------------------------------
// Environment overlay (step 7)
// ---------------------------------------------------------------------------

export interface BuildWorkerEnvOptions {
  /** Inherited environment; defaults to `process.env`. */
  readonly base?: Record<string, string | undefined>
  /** Overlay applied on top; `undefined` deletes an inherited key. */
  readonly overlay?: Record<string, string | undefined>
}

/**
 * Build the immutable worker environment. A thin, typed wrapper over the shared
 * {@link mergeEnv} policy: never mutates `process.env`, and `undefined` overlay
 * values delete inherited keys so a caller can scrub secrets.
 */
export const buildWorkerEnv = (options: BuildWorkerEnvOptions = {}): WorkerEnv => ({
  values: mergeEnv(options.overlay, options.base ?? process.env),
})

// ---------------------------------------------------------------------------
// Serializable model bootstrap (step 8)
// ---------------------------------------------------------------------------

/**
 * The plain-data model the hub ships to a worker (no functions, no class
 * instances). It carries the real descriptor; see {@link SessionWorkerModel}.
 */
export type WorkerModel = SessionWorkerModel

/** Raised when provider/model data cannot cross the isolate boundary. */
export class WorkerModelNotSerializableError extends Schema.TaggedErrorClass<WorkerModelNotSerializableError>()(
  "WorkerModelNotSerializableError",
  {
    providerID: Schema.String,
    modelID: Schema.String,
    field: Schema.String,
    /** The offending value's kind (`function`, `symbol`, …). */
    kind: Schema.optional(Schema.String),
  },
) {
  override get message() {
    return `cannot ship ${this.providerID}/${this.modelID} to a session worker: "${this.field}" is a ${this.kind ?? "value"} (not structured-cloneable)`
  }
}

/** A session's provider/model pair, as persisted on the session. */
export interface WorkerModelSelection {
  readonly providerID: string
  readonly modelID: string
}

export interface ResolveWorkerModelOptions {
  /**
   * The dispatching session's model. When present it is authoritative: a worker
   * is built for the model the session actually runs, never the hub's
   * config/default. A mismatch with the session then cannot arise by
   * construction.
   */
  readonly session?: WorkerModelSelection
  /** Explicit provider selection; defaults to config/default model. */
  readonly providerID?: string
  readonly modelID?: string
  /** Environment source for `${VAR}` base-URL substitution; defaults to `process.env`. */
  readonly env?: Record<string, string | undefined>
}

type NonCloneableKind = "function" | "symbol"

interface NonCloneablePath {
  readonly field: string
  readonly kind: NonCloneableKind
}

/** The reason a value cannot cross the isolate boundary, if any. */
function nonCloneableKind(value: unknown): NonCloneableKind | undefined {
  if (typeof value === "function") return "function"
  if (typeof value === "symbol") return "symbol"
  return undefined
}

/** Iterate a container's children, including `Map` values and `Set` members. */
function entriesOf(value: object): ReadonlyArray<readonly [string, unknown]> {
  if (value instanceof Map) return [...value.entries()].map(([key, nested]) => [String(key), nested] as const)
  if (value instanceof Set) return [...value].map((nested, index) => [String(index), nested] as const)
  return Object.entries(value as Record<string, unknown>)
}

/** Depth-first search for the first non-cloneable value, returning its path + kind. */
function findNonCloneablePath(value: unknown, path: string, seen: WeakSet<object>): NonCloneablePath | undefined {
  const kind = nonCloneableKind(value)
  if (kind !== undefined) return { field: path, kind }
  if (value === null || typeof value !== "object") return undefined
  if (seen.has(value)) return undefined
  seen.add(value)
  for (const [key, nested] of entriesOf(value)) {
    const found = findNonCloneablePath(nested, path === "" ? key : `${path}.${key}`, seen)
    if (found !== undefined) return found
  }
  return undefined
}

const firstNonCloneablePath = (value: unknown, path = ""): NonCloneablePath | undefined =>
  findNonCloneablePath(value, path, new WeakSet())

/** Resolve the base URL the same way `provider.ts`'s `resolveSDK` does. */
function resolveBaseUrl(
  options: Readonly<Record<string, unknown>>,
  fallback: string,
  env: Readonly<Record<string, string | undefined>>,
): string {
  const configured = options["baseURL"]
  const url = typeof configured === "string" && configured !== "" ? configured : fallback
  return url.replace(/\$\{([^}]+)\}/g, (match, key) => env[String(key)] ?? match)
}

function stringHeaders(value: unknown): Record<string, string> {
  if (value === null || typeof value !== "object") return {}
  const headers: Record<string, string> = {}
  for (const [key, header] of Object.entries(value as Record<string, unknown>)) {
    if (typeof header === "string") headers[key] = header
  }
  return headers
}

/**
 * Serialize the real capabilities. `temperature`/`reasoning` are normalized to
 * booleans because the reduced provider gates sampling on them; every other key
 * (including the real `interleaved` object, e.g. `{ field: "reasoning_content" }`)
 * is carried through so the worker can replay reasoning.
 */
function serializeCapabilities(capabilities: Provider.Model["capabilities"]): SerializedModelCapabilities {
  return {
    ...capabilities,
    temperature: capabilities.temperature === true,
    reasoning: capabilities.reasoning === true,
    // `ProviderInterleaved` is `boolean | { field }`; the frozen wire type names
    // `boolean`, but the index signature carries the real object unchanged.
    interleaved: capabilities.interleaved as unknown as boolean | undefined,
  }
}

function serializeLimit(limit: Provider.Model["limit"]): SerializedModelLimit {
  return { context: limit.context, output: limit.output }
}

function credentialKey(credential: Auth.Info | undefined): string | undefined {
  if (!credential) return undefined
  if (credential.type === "api" || credential.type === "wellknown") return credential.key
  return undefined
}

function resolveApiKey(info: Provider.Info, credential: Auth.Info | undefined): string {
  const fromOptions = info.options["apiKey"]
  if (typeof fromOptions === "string" && fromOptions !== "") return fromOptions
  if (typeof info.key === "string" && info.key !== "") return info.key
  return credentialKey(credential) ?? ""
}

const selectModel = Effect.fnUntraced(function* (
  provider: Provider.Interface,
  config: Config.Interface,
  options: ResolveWorkerModelOptions,
) {
  // The session's model is the authority: a worker must serve exactly the model
  // the session runs, not whatever the hub happens to default to.
  if (options.session !== undefined) {
    return {
      providerID: ProviderV2.ID.make(options.session.providerID),
      modelID: ModelV2.ID.make(options.session.modelID),
    }
  }
  if (options.providerID && options.modelID) {
    return { providerID: ProviderV2.ID.make(options.providerID), modelID: ModelV2.ID.make(options.modelID) }
  }
  const cfg = yield* config.get()
  if (cfg.model) return parseModel(cfg.model)
  return yield* provider.defaultModel()
})

/**
 * Resolve the serializable model bootstrap for the worker: the provider/model
 * identity, transport (base URL, API key, headers) and the REAL descriptor
 * (capabilities, limit, options, variants, provider options). Rejects
 * non-cloneable provider/model/model-descriptor values with
 * {@link WorkerModelNotSerializableError}, at hub build rather than at spawn.
 * A supplied session model wins over config/default.
 */
export const resolveWorkerModel = Effect.fn("WorkerModel.resolve")(function* (options: ResolveWorkerModelOptions = {}) {
  const provider = yield* Provider.Service
  const auth = yield* Auth.Service
  const config = yield* Config.Service

  const selection = yield* selectModel(provider, config, options)
  const info = yield* provider.getProvider(selection.providerID)
  const model = yield* provider.getModel(selection.providerID, selection.modelID)
  const credential = yield* auth.get(selection.providerID)

  // Every field that is spread into the serialized bootstrap is scanned, so a
  // non-cloneable value is diagnosed here at hub build rather than making
  // `workerData` throw at spawn. `variants`/`capabilities` are newly shipped, so
  // they are scanned under their wire names; provider/model options keep their
  // historical unprefixed paths.
  const nonCloneable =
    firstNonCloneablePath(info.options) ??
    firstNonCloneablePath(model.options) ??
    firstNonCloneablePath(model.variants, "variants") ??
    firstNonCloneablePath(model.capabilities, "capabilities") ??
    firstNonCloneablePath(model.limit, "limit") ??
    firstNonCloneablePath(model.headers, "headers")
  if (nonCloneable !== undefined) {
    return yield* new WorkerModelNotSerializableError({
      providerID: selection.providerID,
      modelID: selection.modelID,
      field: nonCloneable.field,
      kind: nonCloneable.kind,
    })
  }

  const resolved: WorkerModel = {
    providerID: selection.providerID,
    modelID: selection.modelID,
    apiId: model.api.id,
    npm: model.api.npm,
    baseUrl: resolveBaseUrl(info.options, model.api.url, options.env ?? process.env),
    apiKey: resolveApiKey(info, credential),
    headers: { ...stringHeaders(info.options["headers"]), ...model.headers },
    capabilities: serializeCapabilities(model.capabilities),
    limit: serializeLimit(model.limit),
    options: model.options,
    ...(model.variants === undefined ? {} : { variants: model.variants }),
    providerOptions: info.options,
  }
  return resolved
})

// ---------------------------------------------------------------------------
// Plugin effect resolution (Phase 6.3 / plan decision 70)
// ---------------------------------------------------------------------------

/**
 * Raised when a hub plugin hook cannot be resolved into structuredClone-safe
 * data for the isolate. A hook function can never cross `workerData`, so the hub
 * resolves the hook's *effect* and ships it as data; a hook that throws, or whose
 * resolved effect is not cloneable, fails loud here instead of silently no-oping.
 */
export class WorkerPluginNotSerializableError extends Schema.TaggedErrorClass<WorkerPluginNotSerializableError>()(
  "WorkerPluginNotSerializableError",
  {
    plugin: Schema.String,
    hook: Schema.String,
    reason: Schema.String,
    /** The non-cloneable value's path, when the failure is a cloneability one. */
    field: Schema.optional(Schema.String),
    kind: Schema.optional(Schema.String),
  },
) {
  override get message() {
    return `cannot ship plugin "${this.plugin}" hook "${this.hook}" to a session worker: ${this.reason}`
  }
}

/** One hub plugin's model-facing hooks, before resolution (functions allowed). */
export interface WorkerPluginSource {
  readonly name: string
  readonly hooks: WorkerPluginHooks
}

export interface ResolveWorkerPluginsOptions {
  /** The serialized model the worker will run; hooks see a model of this shape. */
  readonly model: WorkerModel
  readonly sources: readonly WorkerPluginSource[]
  /**
   * The dispatching session id, so a hook that keys on `input.sessionID` (e.g. a
   * `session-id` header) resolves against the real session.
   */
  readonly sessionID?: string
  /**
   * The dispatching session's agent (its last user message's agent, falling back
   * to the session's agent). Per-request, so it is resolved as late as the hub
   * can — at worker bind — and defaults to `""` only when genuinely absent.
   */
  readonly agent?: string
  /**
   * The user message being processed (`UserMessage`), so a hook that keys on
   * `input.message` sees the real value instead of an empty stub. Resolved at
   * worker bind from the session's last user message; a later turn on the same
   * worker stays approximate until the worker rebinds to another session.
   */
  readonly message?: unknown
}

/** The hook input the hub synthesises from the worker's serialized model. */
function pluginHookInput(
  model: WorkerModel,
  sessionID: string,
  agent: string,
  message: unknown,
): Record<string, unknown> {
  return {
    sessionID,
    agent,
    model: {
      id: ModelV2.ID.make(model.modelID),
      providerID: ProviderV2.ID.make(model.providerID),
      api: { id: model.apiId, url: model.baseUrl, npm: model.npm ?? "@ai-sdk/openai-compatible" },
    },
    provider: {
      id: ProviderV2.ID.make(model.providerID),
      name: model.providerID,
      source: "config",
      env: [],
      key: model.apiKey,
      options: { ...(model.providerOptions ?? {}) },
      models: {},
    },
    ...(message === undefined ? {} : { message }),
  }
}

/**
 * Sentinel baseline value for `maxOutputTokens`. The hub only ships the fields a
 * plugin changed; a plugin that *clears* the cap (`output.maxOutputTokens =
 * undefined`) must still be detected, so the baseline uses an impossible value
 * (never a real token cap) and `undefined` after the hook ships as `null`.
 */
const MAX_OUTPUT_TOKENS_SENTINEL = -1

/** The baseline `chat.params` output the hub resolves a hook against. */
function pluginParamsBaseline(model: WorkerModel): ChatParamsOutput {
  return {
    // The worker recomputes these from its own model/agent at request time; the
    // hub only ships the *diff* the plugin introduces, so the baseline is
    // deliberately unset (the plugin's absolute values still win).
    temperature: undefined as unknown as number,
    topP: undefined as unknown as number,
    topK: undefined as unknown as number,
    maxOutputTokens: MAX_OUTPUT_TOKENS_SENTINEL,
    options: { ...(model.options ?? {}) },
  }
}

/** The fields a `chat.params` hook changed, as cloneable data (`null` = cleared). */
function diffResolvedParams(before: ChatParamsOutput, after: ChatParamsOutput): ResolvedChatParams {
  const diff: Record<string, unknown> = {}
  for (const key of ["temperature", "topP", "topK", "maxOutputTokens"] as const) {
    const next = (after as unknown as Record<string, unknown>)[key]
    const previous = (before as unknown as Record<string, unknown>)[key]
    if (next !== previous) diff[key] = next ?? null
  }
  const options: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(after.options)) {
    if (before.options[key] !== value) options[key] = value
  }
  if (Object.keys(options).length > 0) diff.options = options
  return diff as ResolvedChatParams
}

/** Run one hook against the resolved input, mapping a throw to a typed failure. */
function runPluginHook<T>(
  plugin: string,
  hook: string,
  invoke: () => T | Promise<T>,
): Effect.Effect<T, WorkerPluginNotSerializableError> {
  return Effect.tryPromise({
    try: () => Promise.resolve(invoke()),
    catch: (error) =>
      new WorkerPluginNotSerializableError({
        plugin,
        hook,
        reason: `hook threw: ${error instanceof Error ? error.message : String(error)}`,
      }),
  })
}

/**
 * Resolve the hub's plugin hooks for a worker's model into structuredClone-safe
 * {@link WorkerPlugin} effects. `chat.params` / `chat.headers` run on the hub
 * against the serialized model and a baseline; only the resulting diff crosses
 * the isolate. A hook whose resolved effect contains a function/symbol fails loud
 * with {@link WorkerPluginNotSerializableError}.
 */
export const resolveWorkerPlugins = Effect.fn("WorkerPlugins.resolve")(function* (
  options: ResolveWorkerPluginsOptions,
) {
  const input = pluginHookInput(options.model, options.sessionID ?? "", options.agent ?? "", options.message)
  const plugins: WorkerPlugin[] = []
  for (const source of options.sources) {
    const resolved: { "chat.params"?: ResolvedChatParams; "chat.headers"?: ResolvedChatHeaders } = {}
    const paramsHook = source.hooks["chat.params"]
    if (paramsHook !== undefined) {
      const output = pluginParamsBaseline(options.model)
      // Snapshot BEFORE the hook mutates its output, so the diff compares the
      // plugin's effect rather than the output against itself.
      const before = structuredClone(output)
      const after = yield* runPluginHook(source.name, "chat.params", async () => {
        await paramsHook(input, output)
        return output
      })
      resolved["chat.params"] = diffResolvedParams(before, after)
    }
    const headersHook = source.hooks["chat.headers"]
    if (headersHook !== undefined) {
      const output: ChatHeadersOutput = { headers: {} }
      const after = yield* runPluginHook(source.name, "chat.headers", async () => {
        await headersHook(input, output)
        return output
      })
      const headers: ResolvedChatHeaders = { headers: { ...after.headers } }
      resolved["chat.headers"] = headers
    }
    const nonCloneable = firstNonCloneablePath(resolved)
    if (nonCloneable !== undefined) {
      return yield* new WorkerPluginNotSerializableError({
        plugin: source.name,
        hook: "resolved",
        reason: `the resolved effect is a ${nonCloneable.kind} (not structured-cloneable)`,
        field: nonCloneable.field,
        kind: nonCloneable.kind,
      })
    }
    plugins.push({ name: source.name, hooks: {}, resolved })
  }
  return plugins
})

// ---------------------------------------------------------------------------
// Init assembly (step 9)
// ---------------------------------------------------------------------------

export interface BuildSessionWorkerInitOptions {
  readonly snapshot?: BuildWorkerSnapshotOptions
  readonly env?: BuildWorkerEnvOptions
  readonly model?: ResolveWorkerModelOptions
  /**
   * The worker's pool slot id, when the caller knows it. The production pool
   * stamps the real id per spawn (it is the only layer that owns the slot), so
   * this is for callers that build an init for a specific worker.
   */
  readonly workerID?: number
  /**
   * Capabilities the hub advertises for this worker generation. Set
   * `{ sessionWorkerPorts: true }` once the hub serves the writer / egress /
   * broker ports, so the entry transfers them instead of using its in-worker
   * fallback.
   */
  readonly capabilities?: SessionWorkerCapabilities
  /**
   * The worker-hosted plugin hooks. Either ready (in-process) or a resolver
   * called with the resolved model (the production hub's path: resolve the
   * plugin effect against the model the worker will run).
   */
  readonly plugins?:
    | readonly WorkerPlugin[]
    | ((model: WorkerModel) => Effect.Effect<readonly WorkerPlugin[], WorkerPluginNotSerializableError>)
}

/**
 * Assemble the `workerData` payload from the snapshot, environment and model
 * producers. The result is structured-clone friendly and accepted by the
 * production entry's `parseSessionWorkerInit`.
 */
export const buildSessionWorkerInit = Effect.fn("SessionWorkerInit.build")(function* (
  options: BuildSessionWorkerInitOptions = {},
) {
  const snapshot = yield* buildWorkerSnapshot(options.snapshot)
  const env = buildWorkerEnv(options.env)
  const model = yield* resolveWorkerModel(options.model)
  // The hub resolves plugin hooks against the model the worker will run, then
  // ships the resolved effect as data (a hook function cannot cross the isolate).
  const plugins =
    typeof options.plugins === "function" ? yield* options.plugins(model) : options.plugins
  return {
    snapshot,
    env,
    // The full descriptor crosses verbatim, so the reduced provider reproduces
    // flag-OFF sampling (capabilities/limits/options/variants/provider options).
    model,
    ...(options.workerID === undefined ? {} : { workerID: options.workerID }),
    ...(options.capabilities === undefined ? {} : { capabilities: options.capabilities }),
    ...(plugins === undefined ? {} : { plugins }),
  } satisfies SessionWorkerInit
})

export * as WorkerInitModule from "./init"
