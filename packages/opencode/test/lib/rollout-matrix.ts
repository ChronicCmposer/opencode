// Rollout/flag contract used by the matrix tests.
//
// The production implementation may lag. These helpers define the two-state
// contract, status projection validation, startup plugin validation, kill-switch
// admission, and the typed infrastructure failures the runtime must surface.

/** There are exactly two flag states. */
export const ROLLOUT_STATES = ["off", "on"] as const
export type RolloutState = (typeof ROLLOUT_STATES)[number]

/** Parse a serialized state, failing loud on anything other than `off`/`on`. */
export function parseRolloutState(value: unknown): RolloutState {
  if (value === "off" || value === "on") return value
  throw new Error(`invalid rollout state ${JSON.stringify(value)} (expected "off" or "on")`)
}

/** Project the resolved boolean onto its state name. */
export function rolloutStateFromFlag(enabled: boolean): RolloutState {
  return enabled ? "on" : "off"
}

/** The status contract D25 requires: mode, disabled reason, and protocol schema version. */
export interface RolloutStatus {
  readonly mode: RolloutState
  /** Non-empty when `mode` is `off`; `null` when enabled. */
  readonly disabledReason: string | null
  readonly protocolSchemaVersion: number
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function readString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`${label} must be a non-empty string`)
  return value
}

/** Parse an untrusted status payload, failing loud on the first contract breach. */
export function parseRolloutStatus(value: unknown, path = "<rollout-status>"): RolloutStatus {
  if (!isRecord(value)) throw new Error(`${path} must be an object`)
  const mode = parseRolloutState(value.mode)
  const protocolSchemaVersion = value.protocolSchemaVersion
  if (typeof protocolSchemaVersion !== "number" || !Number.isInteger(protocolSchemaVersion) || protocolSchemaVersion < 0) {
    throw new Error(`${path}.protocolSchemaVersion must be a non-negative integer`)
  }

  if (mode === "on") {
    if (value.disabledReason !== null) throw new Error(`${path}.disabledReason must be null when the mode is on`)
    return { mode, disabledReason: null, protocolSchemaVersion }
  }

  const disabledReason = readString(value.disabledReason, `${path}.disabledReason`)
  return { mode, disabledReason, protocolSchemaVersion }
}

/** Build the status projection from the flag state. */
export function createRolloutStatus(input: {
  readonly enabled: boolean
  readonly disabledReason?: string | null
  readonly protocolSchemaVersion: number
}): RolloutStatus {
  return parseRolloutStatus({
    mode: rolloutStateFromFlag(input.enabled),
    disabledReason: input.enabled ? null : input.disabledReason,
    protocolSchemaVersion: input.protocolSchemaVersion,
  })
}

export type KillSwitchDecision =
  | { readonly admit: true; readonly reason: "allowed" | "drain-existing" }
  | { readonly admit: false; readonly reason: "kill-switch-new-session" }

/**
 * D25 kill-switch admission: new Sessions are blocked, existing Sessions drain.
 * The session set is explicit so an "existing" answer cannot be guessed.
 */
export function decideKillSwitchAdmission(input: {
  readonly killSwitchEnabled: boolean
  readonly existingSessionIDs: ReadonlySet<string>
  readonly sessionID: string
}): KillSwitchDecision {
  if (input.sessionID.trim() === "") throw new Error("kill-switch admission needs a non-empty sessionID")
  if (!input.killSwitchEnabled) return { admit: true, reason: "allowed" }
  if (input.existingSessionIDs.has(input.sessionID)) return { admit: true, reason: "drain-existing" }
  return { admit: false, reason: "kill-switch-new-session" }
}

/** Hooks declared for the plugin API but never routed through the worker table. */
export const DECLARED_ONLY_HOOKS: ReadonlySet<string> = new Set(["permission.ask"])

/** A configured hook the worker cannot route: fatal at startup, never silent. */
export class UnsupportedConfiguredHookError extends Error {
  readonly hooks: ReadonlyArray<string>

  constructor(hooks: ReadonlyArray<string>) {
    super(`configured plugin hook(s) have no worker route: ${hooks.join(", ")}`)
    this.name = "UnsupportedConfiguredHookError"
    this.hooks = hooks
  }
}

/**
 * D19 startup validation: a configured hook without a route is fatal. A
 * declared-only hook is allowed to be configured but is not considered routed.
 */
export function assertConfiguredHooksSupported(input: {
  readonly configured: ReadonlyArray<string>
  readonly supported: ReadonlySet<string>
}): void {
  const unsupported = [...new Set(input.configured)].filter(
    (hook) => !input.supported.has(hook) && !DECLARED_ONLY_HOOKS.has(hook),
  )
  if (unsupported.length > 0) throw new UnsupportedConfiguredHookError(unsupported.sort())
}

/** Infrastructure failures that must stay typed; there is no silent fallback. */
export const ROLLOUT_INFRA_FAILURES = ["SchemaMismatch", "BusyExhausted"] as const
export type RolloutInfraFailure = (typeof ROLLOUT_INFRA_FAILURES)[number]

/** Read the schema/Effect error tag without trusting the value shape. */
export function errorTagOf(error: unknown): string | undefined {
  if (!isRecord(error)) return undefined
  const tag = error._tag
  return typeof tag === "string" ? tag : undefined
}

/** True when `error` carries one of the typed infrastructure tags. */
export function isRolloutInfraFailure(error: unknown, expected: RolloutInfraFailure): boolean {
  const tag = errorTagOf(error)
  if (tag === undefined) return false
  return tag === expected || tag.endsWith(`.${expected}`)
}
