// Normalization for parity comparisons.
//
// Generated ids and wall-clock fields differ between two runs of the same
// scenario, so a raw deep-equal would fail for reasons that carry no behavioral
// meaning. This module projects a captured value onto its stable shape exactly
// once, at the comparison boundary; everything downstream trusts the result.

import type { ParityDurableRow } from "./parity-session"

/** Generated identifier families whose concrete values differ per run. */
const TRACE_ID_PATTERNS: ReadonlyArray<readonly [RegExp, string]> = [
  [/\bses_[A-Za-z0-9_-]+/g, "ses_<id>"],
  [/\bmsg_[A-Za-z0-9_-]+/g, "msg_<id>"],
  [/\bprt_[A-Za-z0-9_-]+/g, "prt_<id>"],
  [/\bcall_[A-Za-z0-9_-]+/g, "call_<id>"],
  [/\bjob_[A-Za-z0-9_-]+/g, "job_<id>"],
  [/\bevt_[A-Za-z0-9_-]+/g, "evt_<id>"],
]

/** Fields carrying wall-clock values that differ between two runs. */
const VOLATILE_TIME_KEYS: ReadonlySet<string> = new Set([
  "time",
  "created",
  "updated",
  "started_at",
  "completed_at",
])

/**
 * Project a captured value to its comparable shape: generated ids collapse to a
 * single placeholder and wall-clock fields become `0`, recursively. Non-string
 * leaves and deliberate enum values pass through unchanged, so a divergence in
 * behavior still surfaces.
 */
export function normalizeTrace(value: unknown): unknown {
  if (typeof value === "string") {
    return TRACE_ID_PATTERNS.reduce((text, [pattern, replacement]) => text.replace(pattern, replacement), value)
  }
  if (Array.isArray(value)) return value.map(normalizeTrace)
  if (value === null || typeof value !== "object") return value

  const record = value as Record<string, unknown>
  return Object.fromEntries(
    Object.entries(record).map(([key, entry]) => [
      key,
      VOLATILE_TIME_KEYS.has(key) && typeof entry === "number" ? 0 : normalizeTrace(entry),
    ]),
  )
}

/** Project one durable row onto its stable `{ table, row }` shape. */
export function normalizeDurableRow(row: ParityDurableRow): unknown {
  return normalizeTrace({ table: row.table, row: row.row })
}
