// Session worker pool sizing.
//
// The hub sizes its production pool from a single core count: one worker below
// four cores, otherwise `availableParallelism() − 2`, so the hub, the single
// writer and the broker keep their cores (plan decision line 22). The count is
// overridable with `OPENCODE_SESSION_WORKER_COUNT`.
//
// Dispatch/scheduling and the worker lifecycle live in `execute.ts`/`hub.ts`;
// this module owns only sizing.

import os from "node:os"
import { parsePositiveInt } from "@/util/parse"

export const WORKER_COUNT_ENV = "OPENCODE_SESSION_WORKER_COUNT"

const CORES_REQUIRED_FOR_MULTIPLE_WORKERS = 4
const RESERVED_CORES = 2

export type Env = Record<string, string | undefined>

export interface WorkerCountOptions {
  /** Injected parallelism; defaults to `os.availableParallelism`. */
  readonly availableParallelism?: () => number
  /** Environment source for the override; defaults to `process.env`. */
  readonly env?: Env
}

/**
 * Resolve the pool size: `OPENCODE_SESSION_WORKER_COUNT` when set (positive
 * integer), otherwise `deriveWorkerCount(availableParallelism())`.
 */
export function resolveWorkerCount(options: WorkerCountOptions = {}): number {
  const override = parseWorkerCountOverride((options.env ?? process.env)[WORKER_COUNT_ENV])
  if (override !== undefined) return override
  return deriveWorkerCount(readParallelism(options.availableParallelism))
}

/** A single worker below 4 cores, otherwise `cores − 2`. */
export function deriveWorkerCount(cores: number): number {
  parsePositiveInt(cores, "availableParallelism")
  if (cores < CORES_REQUIRED_FOR_MULTIPLE_WORKERS) return 1
  return Math.max(1, cores - RESERVED_CORES)
}

function parseWorkerCountOverride(raw: string | undefined): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined
  return parsePositiveInt(raw, WORKER_COUNT_ENV)
}

function readParallelism(availableParallelism?: () => number): number {
  const detected = (availableParallelism ?? os.availableParallelism.bind(os))()
  return parsePositiveInt(detected, "availableParallelism")
}

export * as WorkerPoolModule from "./pool"
