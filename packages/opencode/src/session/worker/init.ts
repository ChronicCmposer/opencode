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

import type { SessionWorkerCapabilities, SessionWorkerInit, SessionWorkerModel } from "./session-worker"
import type { WorkerEnv } from "./layer"
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

/** The plain-data model the hub ships to a worker (no functions, no class instances). */
export interface WorkerModel {
  readonly providerID: string
  readonly modelID: string
  readonly apiId: string
  readonly npm: string
  readonly baseUrl: string
  readonly apiKey: string
  readonly headers: Readonly<Record<string, string>>
}

/** Raised when a provider option cannot cross the isolate boundary. */
export class WorkerModelNotSerializableError extends Schema.TaggedErrorClass<WorkerModelNotSerializableError>()(
  "WorkerModelNotSerializableError",
  {
    providerID: Schema.String,
    modelID: Schema.String,
    field: Schema.String,
  },
) {
  override get message() {
    return `cannot ship ${this.providerID}/${this.modelID} to a session worker: "${this.field}" is a function (not structured-cloneable)`
  }
}

export interface ResolveWorkerModelOptions {
  /** Explicit provider selection; defaults to config/default model. */
  readonly providerID?: string
  readonly modelID?: string
  /** Environment source for `${VAR}` base-URL substitution; defaults to `process.env`. */
  readonly env?: Record<string, string | undefined>
}

/** Depth-first search for the first function-valued option, returning its path. */
function findFunctionPath(value: unknown, path: string, seen: WeakSet<object>): string | undefined {
  if (typeof value === "function") return path
  if (value === null || typeof value !== "object") return undefined
  if (seen.has(value)) return undefined
  seen.add(value)
  for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
    const found = findFunctionPath(nested, path === "" ? key : `${path}.${key}`, seen)
    if (found !== undefined) return found
  }
  return undefined
}

const firstFunctionPath = (options: Readonly<Record<string, unknown>> | undefined): string | undefined =>
  options === undefined ? undefined : findFunctionPath(options, "", new WeakSet())

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
  if (options.providerID && options.modelID) {
    return { providerID: ProviderV2.ID.make(options.providerID), modelID: ModelV2.ID.make(options.modelID) }
  }
  const cfg = yield* config.get()
  if (cfg.model) return parseModel(cfg.model)
  return yield* provider.defaultModel()
})

/**
 * Resolve the serializable model bootstrap for the worker: the provider/model
 * identity, API descriptor, and a plain-data base URL, API key and headers.
 * Rejects function-bearing provider options with
 * {@link WorkerModelNotSerializableError}.
 */
export const resolveWorkerModel = Effect.fn("WorkerModel.resolve")(function* (
  options: ResolveWorkerModelOptions = {},
) {
  const provider = yield* Provider.Service
  const auth = yield* Auth.Service
  const config = yield* Config.Service

  const selection = yield* selectModel(provider, config, options)
  const info = yield* provider.getProvider(selection.providerID)
  const model = yield* provider.getModel(selection.providerID, selection.modelID)
  const credential = yield* auth.get(selection.providerID)

  const field = firstFunctionPath(info.options) ?? firstFunctionPath(model.options)
  if (field !== undefined) {
    return yield* new WorkerModelNotSerializableError({
      providerID: selection.providerID,
      modelID: selection.modelID,
      field,
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
  }
  return resolved
})

// ---------------------------------------------------------------------------
// Init assembly (step 9)
// ---------------------------------------------------------------------------

export interface BuildSessionWorkerInitOptions {
  readonly snapshot?: BuildWorkerSnapshotOptions
  readonly env?: BuildWorkerEnvOptions
  readonly model?: ResolveWorkerModelOptions
  /**
   * Capabilities the hub advertises for this worker generation. Set
   * `{ sessionWorkerPorts: true }` once the hub serves the writer / egress /
   * broker ports, so the entry transfers them instead of using its in-worker
   * fallback.
   */
  readonly capabilities?: SessionWorkerCapabilities
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
  return {
    snapshot,
    env,
    // Carry the full serializable model identity so the reduced provider is
    // faithful (the entry falls back to `model` for any field omitted).
    model: {
      baseUrl: model.baseUrl,
      apiKey: model.apiKey,
      model: model.modelID,
      providerID: model.providerID,
      apiId: model.apiId,
      npm: model.npm,
      headers: model.headers,
    } satisfies SessionWorkerModel,
    ...(options.capabilities === undefined ? {} : { capabilities: options.capabilities }),
  } satisfies SessionWorkerInit
})

export * as WorkerInitModule from "./init"
