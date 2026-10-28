import { isPositiveInt } from "@/util/parse"

export const SESSION_WORKER_IDLE_TIMEOUT_ENV = "OPENCODE_SESSION_WORKER_IDLE_TIMEOUT_MS"
export const REAP_MS_ENV = "OPENCODE_SESSION_WORKER_REAP_MS"
export const DEFAULT_REAP_MS = 300_000

export type Env = Record<string, string | undefined>

export interface IdleTimeoutOptions {
  /** Environment source for the override; defaults to `process.env`. */
  readonly env?: Env
}

/**
 * Resolve the `run`/`startShell` reply liveness budget. A positive integer
 * enables the worker watchdog; anything else (absent, empty, zero, negative or
 * non-numeric) disables it (`0`). Unlike pool sizing, a malformed value is NOT
 * fatal: the watchdog is a resilience feature, so a typo must fall back to the
 * safe default — current unbounded behavior — rather than fail hub startup.
 */
export function resolveSessionWorkerIdleTimeoutMs(options: IdleTimeoutOptions = {}): number {
  const env = options.env ?? process.env
  return parseIdleTimeout(env[SESSION_WORKER_IDLE_TIMEOUT_ENV])
}

/** A positive integer, or `0` for "disabled" on any other input. */
function parseIdleTimeout(raw: string | undefined): number {
  if (raw === undefined || raw.trim() === "") return 0
  const parsed = Number(raw)
  return isPositiveInt(parsed) ? parsed : 0
}

/**
 * Resolve the idle window after which a session worker is reaped. A positive
 * integer wins; an absent value uses the five-minute default. Unlike pool
 * sizing, a malformed value is NOT fatal: reaping is a resilience feature, so a
 * typo falls back to the default and {@link reapMsWarning} reports why.
 */
export function resolveReapMs(env: Env = process.env): number {
  const parsed = parseReapMs(env[REAP_MS_ENV])
  return parsed.kind === "value" ? parsed.ms : DEFAULT_REAP_MS
}

/**
 * The reason `resolveReapMs` fell back to the default, or `undefined` when the
 * knob is absent or valid. Callers emit this at WARN at startup.
 */
export function reapMsWarning(env: Env = process.env): string | undefined {
  const parsed = parseReapMs(env[REAP_MS_ENV])
  if (parsed.kind === "absent" || parsed.kind === "value") return undefined
  if (parsed.kind === "empty") return `${REAP_MS_ENV} is empty; using default ${DEFAULT_REAP_MS} ms`
  return `invalid ${REAP_MS_ENV}: ${parsed.raw} (expected a positive integer); using default ${DEFAULT_REAP_MS} ms`
}

type ParsedReapMs =
  | { readonly kind: "absent" }
  | { readonly kind: "empty" }
  | { readonly kind: "invalid"; readonly raw: string }
  | { readonly kind: "value"; readonly ms: number }

function parseReapMs(raw: string | undefined): ParsedReapMs {
  if (raw === undefined) return { kind: "absent" }
  if (raw.trim() === "") return { kind: "empty" }
  const parsed = Number(raw)
  if (!isPositiveInt(parsed)) return { kind: "invalid", raw }
  return { kind: "value", ms: parsed }
}

export * as WorkerPoolModule from "./pool"
