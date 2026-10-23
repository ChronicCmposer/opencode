import { Config, ConfigProvider, Context, Effect, Layer, Option } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ConfigService } from "@/effect/config-service"
import { isPositiveInt } from "@/util/parse"

const bool = (name: string) => Config.boolean(name).pipe(Config.withDefault(false))
const positiveInteger = (name: string) =>
  Config.number(name).pipe(
    Config.map((value) => (isPositiveInt(value) ? value : undefined)),
    Config.orElse(() => Config.succeed(undefined)),
  )
const experimental = bool("OPENCODE_EXPERIMENTAL")
const enabledByExperimental = (name: string) =>
  Config.all({ experimental, enabled: Config.boolean(name).pipe(Config.option) }).pipe(
    Config.map((flags) => Option.getOrElse(flags.enabled, () => flags.experimental)),
  )

const TRUE_VALUES = new Set(["true", "yes", "on", "1", "y"])
const FALSE_VALUES = new Set(["false", "no", "off", "0", "n"])

const parseBoolean = (value: string | undefined): boolean | undefined => {
  if (value === undefined) return undefined
  if (TRUE_VALUES.has(value)) return true
  if (FALSE_VALUES.has(value)) return false
  return undefined
}

const SESSION_WORKERS_ENV = "OPENCODE_EXPERIMENTAL_SESSION_WORKERS"
const UMBRELLA_ENV = "OPENCODE_EXPERIMENTAL"

/**
 * The canonical parser for the session-worker rollout flag. The dedicated flag
 * wins when it parses as a boolean; otherwise it falls back to the umbrella
 * `OPENCODE_EXPERIMENTAL`, which itself defaults to `false`. Shared by the
 * graph decision (`SessionWorkerNodes.sessionWorkersEnabled`) and the runtime
 * gate below, so the two cannot disagree on how a value is interpreted.
 */
export const experimentalSessionWorkersFromEnv = (env: Record<string, string | undefined> = process.env): boolean =>
  parseBoolean(env[SESSION_WORKERS_ENV]) ?? parseBoolean(env[UMBRELLA_ENV]) ?? false

/**
 * Read an optional string from the ConfigProvider. A missing *or* unparseable
 * value yields `None`, so the flag recovers to its fallback instead of dying on
 * a malformed variable.
 */
const optionalString = (name: string): Config.Config<Option.Option<string>> =>
  Config.string(name).pipe(
    Config.option,
    Config.orElse(() => Config.succeed(Option.none<string>())),
  )

/**
 * The session-worker rollout flag as a `Config`. It resolves through the ambient
 * ConfigProvider but parses through {@link experimentalSessionWorkersFromEnv},
 * so the Config and the graph predicate stay in lockstep — including malformed
 * values, where both recover rather than the Config path failing.
 */
const experimentalSessionWorkersConfig: Config.Config<boolean> = Config.all({
  dedicated: optionalString(SESSION_WORKERS_ENV),
  umbrella: optionalString(UMBRELLA_ENV),
}).pipe(
  Config.map((values) =>
    experimentalSessionWorkersFromEnv({
      [SESSION_WORKERS_ENV]: Option.getOrUndefined(values.dedicated),
      [UMBRELLA_ENV]: Option.getOrUndefined(values.umbrella),
    }),
  ),
)

export class Service extends ConfigService.Service<Service>()("@opencode/RuntimeFlags", {
  autoShare: bool("OPENCODE_AUTO_SHARE"),
  pure: bool("OPENCODE_PURE"),
  disableDefaultPlugins: bool("OPENCODE_DISABLE_DEFAULT_PLUGINS"),
  disableEmbeddedWebUi: bool("OPENCODE_DISABLE_EMBEDDED_WEB_UI"),
  disableExternalSkills: bool("OPENCODE_DISABLE_EXTERNAL_SKILLS"),
  disableLspDownload: bool("OPENCODE_DISABLE_LSP_DOWNLOAD"),
  disableClaudeCodePrompt: Config.all({
    broad: bool("OPENCODE_DISABLE_CLAUDE_CODE"),
    direct: bool("OPENCODE_DISABLE_CLAUDE_CODE_PROMPT"),
  }).pipe(Config.map((flags) => flags.broad || flags.direct)),
  disableClaudeCodeSkills: Config.all({
    broad: bool("OPENCODE_DISABLE_CLAUDE_CODE"),
    direct: bool("OPENCODE_DISABLE_CLAUDE_CODE_SKILLS"),
  }).pipe(Config.map((flags) => flags.broad || flags.direct)),
  enableExa: Config.all({
    experimental,
    enabled: bool("OPENCODE_ENABLE_EXA"),
    legacy: bool("OPENCODE_EXPERIMENTAL_EXA"),
  }).pipe(Config.map((flags) => flags.experimental || flags.enabled || flags.legacy)),
  enableParallel: Config.all({
    enabled: bool("OPENCODE_ENABLE_PARALLEL"),
    legacy: bool("OPENCODE_EXPERIMENTAL_PARALLEL"),
  }).pipe(Config.map((flags) => flags.enabled || flags.legacy)),
  enableExperimentalModels: bool("OPENCODE_ENABLE_EXPERIMENTAL_MODELS"),
  enableQuestionTool: bool("OPENCODE_ENABLE_QUESTION_TOOL"),
  experimentalReferences: enabledByExperimental("OPENCODE_EXPERIMENTAL_REFERENCES"),
  experimentalBackgroundSubagents: enabledByExperimental("OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS"),
  experimentalLspTy: bool("OPENCODE_EXPERIMENTAL_LSP_TY"),
  experimentalLspTool: enabledByExperimental("OPENCODE_EXPERIMENTAL_LSP_TOOL"),
  experimentalOxfmt: enabledByExperimental("OPENCODE_EXPERIMENTAL_OXFMT"),
  experimentalPlanMode: enabledByExperimental("OPENCODE_EXPERIMENTAL_PLAN_MODE"),
  experimentalCodeMode: enabledByExperimental("OPENCODE_EXPERIMENTAL_CODE_MODE"),
  experimentalEventSystem: enabledByExperimental("OPENCODE_EXPERIMENTAL_EVENT_SYSTEM"),
  experimentalSessionWorkers: experimentalSessionWorkersConfig,
  experimentalWorkspaces: enabledByExperimental("OPENCODE_EXPERIMENTAL_WORKSPACES"),
  experimentalIconDiscovery: enabledByExperimental("OPENCODE_EXPERIMENTAL_ICON_DISCOVERY"),
  outputTokenMax: positiveInteger("OPENCODE_EXPERIMENTAL_OUTPUT_TOKEN_MAX"),
  bashDefaultTimeoutMs: positiveInteger("OPENCODE_EXPERIMENTAL_BASH_DEFAULT_TIMEOUT_MS"),
  experimentalNativeLlm: bool("OPENCODE_EXPERIMENTAL_NATIVE_LLM"),
  experimentalWebSockets: bool("OPENCODE_EXPERIMENTAL_WEBSOCKETS"),
  client: Config.string("OPENCODE_CLIENT").pipe(Config.withDefault("cli")),
}) {}

export type Info = Context.Service.Shape<typeof Service>

const emptyConfigLayer = Service.layer.pipe(
  Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
  Layer.orDie,
)

export const layer = (overrides: Partial<Info> = {}) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const flags = yield* Service
      return Service.of({ ...flags, ...overrides })
    }),
  ).pipe(Layer.provide(emptyConfigLayer))

export const node = LayerNode.make({ service: Service, layer: Service.layer.pipe(Layer.orDie), deps: [] })

/**
 * The all-defaults flag set, resolved once from an empty `ConfigProvider`.
 *
 * Used as the recovery baseline when a hub-serialized snapshot omits a flag (a
 * version-skewed worker), so parsing a snapshot never falls back to the
 * isolate's `process.env`.
 */
let cachedDefaults: Info | undefined
const defaults = (): Info => {
  cachedDefaults ??= Effect.runSync(
    Effect.gen(function* () {
      return yield* Service
    }).pipe(Effect.provide(emptyConfigLayer)),
  )
  return cachedDefaults
}

/**
 * Parse a hub-serialized `RuntimeFlags` snapshot at the worker boundary.
 *
 * The hub resolves every flag once (including any per-layer override) and freezes
 * the result into the worker snapshot; the isolate must trust that value rather
 * than re-deriving it from its own `ConfigProvider`/`process.env`. This parses the
 * serialized record against the canonical schema's defaults: a known flag with the
 * right runtime type is carried through, and a missing or malformed entry recovers
 * to its default. The result is a trusted {@link Info} for the rest of the isolate.
 */
export const parseInfo = (input: unknown): Info => {
  const fallback = defaults() as Record<string, unknown>
  if (typeof input !== "object" || input === null) return fallback as Info
  const source = input as Record<string, unknown>
  const parsed: Record<string, unknown> = { ...fallback }
  for (const key of Object.keys(fallback)) {
    const expected = fallback[key]
    const value = source[key]
    // An optional numeric flag (`outputTokenMax`, `bashDefaultTimeoutMs`) defaults
    // to `undefined`; accept any finite number.
    if (expected === undefined) {
      if (value === undefined || (typeof value === "number" && Number.isFinite(value))) parsed[key] = value
      continue
    }
    if (typeof value === typeof expected) parsed[key] = value
  }
  return parsed as Info
}

export * as RuntimeFlags from "./runtime-flags"
