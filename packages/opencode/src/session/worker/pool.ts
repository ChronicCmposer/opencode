// Session worker pool sizing.
//
// The hub sizes its production pool by oversubscribing the machine's cores:
// `nproc * DEFAULT_OVERSUBSCRIPTION_FACTOR` (2), floor 1, so the hub, the single
// writer and the broker keep their cores while several sessions still run in
// parallel (plan decision line 22). Retune the factor with
// `OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION`, or pin the count absolutely with
// `OPENCODE_SESSION_WORKER_COUNT`.
//
// Dispatch/scheduling and the worker lifecycle live in `execute.ts`/`hub.ts`;
// this module owns only sizing.

import os from "node:os"
import { isPositiveInt, parsePositiveInt } from "@/util/parse"

export const WORKER_COUNT_ENV = "OPENCODE_SESSION_WORKER_COUNT"
export const OVERSUBSCRIPTION_ENV = "OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION"
export const SESSION_WORKER_IDLE_TIMEOUT_ENV = "OPENCODE_SESSION_WORKER_IDLE_TIMEOUT_MS"
export const DEFAULT_OVERSUBSCRIPTION_FACTOR = 2

export type Env = Record<string, string | undefined>

export interface WorkerCountOptions {
  /** Injected parallelism; defaults to `os.availableParallelism`. */
  readonly availableParallelism?: () => number
  /** Environment source for the override; defaults to `process.env`. */
  readonly env?: Env
}

/**
 * Resolve the pool size. `OPENCODE_SESSION_WORKER_COUNT` (a positive integer)
 * wins outright; otherwise `OPENCODE_SESSION_WORKER_OVERSUBSCRIPTION` tunes the
 * oversubscription factor applied to `availableParallelism()`.
 */
export function resolveWorkerCount(options: WorkerCountOptions = {}): number {
  const env = options.env ?? process.env
  const override = parseWorkerCountOverride(env[WORKER_COUNT_ENV])
  if (override !== undefined) return override
  const factor = parseOversubscriptionFactor(env[OVERSUBSCRIPTION_ENV])
  return deriveWorkerCount(readParallelism(options.availableParallelism), factor)
}

/** Oversubscribe `cores` by `factor`, floor 1. */
export function deriveWorkerCount(cores: number, factor: number = DEFAULT_OVERSUBSCRIPTION_FACTOR): number {
  parsePositiveInt(cores, "availableParallelism")
  return Math.max(1, Math.floor(cores * factor))
}

function parseWorkerCountOverride(raw: string | undefined): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined
  return parsePositiveInt(raw, WORKER_COUNT_ENV)
}

function parseOversubscriptionFactor(raw: string | undefined): number {
  if (raw === undefined || raw.trim() === "") return DEFAULT_OVERSUBSCRIPTION_FACTOR
  const factor = Number(raw)
  if (!Number.isFinite(factor) || factor <= 0) {
    throw new Error(`invalid ${OVERSUBSCRIPTION_ENV}: ${raw} (expected a positive number)`)
  }
  return factor
}

function readParallelism(availableParallelism?: () => number): number {
  const detected = (availableParallelism ?? os.availableParallelism.bind(os))()
  return parsePositiveInt(detected, "availableParallelism")
}

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

export * as WorkerPoolModule from "./pool"
