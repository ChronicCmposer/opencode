// Golden-capture helper (plan task 3.12).
//
// Records the durable event stream a session pipeline commits so two runs — for
// example rollout flag off vs flag on — can be compared for FULL-row equivalence.
// A golden row is the whole durable row: `(aggregate_id, seq, type)` plus `data`
// and `owner_id` (the latter lives on `event_sequence`; see core/src/event/sql.ts).
//
// Design: composed on top of the 3.3 capture timeline, so a golden capture is also
// an ordered, clock-injected, immutable record. Rows are parsed at the boundary and
// compared canonically (aggregates sorted, rows ordered by seq) so that record
// order and interleaving differences between runs do not create false mismatches.

import { isDeepStrictEqual } from "node:util"
import { createCapture, type CaptureHarness } from "./capture"
import { deepFreeze } from "./deep-freeze"

export interface DurableRow {
  readonly aggregate_id: string
  readonly seq: number
  readonly type: string
  readonly data: unknown
  readonly owner_id?: string | null
}

export interface GoldenRow {
  readonly aggregate_id: string
  readonly seq: number
  readonly type: string
  readonly data: unknown
  readonly owner_id: string | null
}

export interface GoldenSequenceEntry {
  readonly aggregate_id: string
  readonly seq: number
  readonly type: string
}

export interface GoldenAggregate {
  readonly aggregate_id: string
  readonly rows: ReadonlyArray<GoldenRow>
}

export interface GoldenSnapshot {
  /** Canonical durable `(aggregate_id, seq, type)` sequence. */
  readonly sequence: ReadonlyArray<GoldenSequenceEntry>
  readonly aggregates: ReadonlyArray<GoldenAggregate>
}

export interface GoldenCaptureOptions {
  /** Injectable clock, forwarded to the underlying capture. */
  readonly clock?: () => number
  /** Reuse an existing capture timeline instead of creating one. */
  readonly capture?: CaptureHarness
}

export interface GoldenCapture {
  /** The underlying 3.3 capture timeline this helper records onto. */
  readonly capture: CaptureHarness
  /** Parse and record one durable row; throws on an invalid row. */
  durable(row: DurableRow): GoldenRow
  readonly rows: ReadonlyArray<GoldenRow>
  /** Observed durable sequence in record order. */
  sequence(): ReadonlyArray<GoldenSequenceEntry>
  /** Canonical, frozen projection for equivalence comparison. */
  snapshot(): GoldenSnapshot
  reset(): void
}

export type GoldenDifference =
  | { readonly kind: "missing-aggregate"; readonly aggregate_id: string }
  | { readonly kind: "extra-aggregate"; readonly aggregate_id: string }
  | { readonly kind: "missing-row"; readonly aggregate_id: string; readonly seq: number }
  | { readonly kind: "extra-row"; readonly aggregate_id: string; readonly seq: number }
  | {
      readonly kind: "field-mismatch"
      readonly aggregate_id: string
      readonly seq: number
      readonly field: "type" | "data" | "owner_id"
      readonly expected: unknown
      readonly actual: unknown
    }

const DURABLE_KIND = "durable"
const COMPARED_FIELDS = ["type", "data", "owner_id"] as const

/** Parse an untrusted durable row into a trusted `GoldenRow`, failing loudly. */
function parseDurableRow(row: DurableRow): GoldenRow {
  if (typeof row.aggregate_id !== "string" || row.aggregate_id.length === 0) {
    throw new Error(`invalid durable row: aggregate_id must be a non-empty string (got ${JSON.stringify(row.aggregate_id)})`)
  }
  if (!Number.isInteger(row.seq) || row.seq < 0) {
    throw new Error(`invalid durable row: seq must be a non-negative integer (got ${JSON.stringify(row.seq)})`)
  }
  if (typeof row.type !== "string" || row.type.length === 0) {
    throw new Error(`invalid durable row: type must be a non-empty string (got ${JSON.stringify(row.type)})`)
  }
  if (row.data === undefined) {
    throw new Error("invalid durable row: data is required")
  }
  if (row.owner_id !== undefined && row.owner_id !== null && typeof row.owner_id !== "string") {
    throw new Error(`invalid durable row: owner_id must be a string or null (got ${JSON.stringify(row.owner_id)})`)
  }
  return {
    aggregate_id: row.aggregate_id,
    seq: row.seq,
    type: row.type,
    data: row.data,
    owner_id: row.owner_id ?? null,
  }
}

/** Group by aggregate and order rows by seq; the canonical comparison value. */
function canonicalize(rows: ReadonlyArray<GoldenRow>): GoldenSnapshot {
  const byAggregate = new Map<string, GoldenRow[]>()
  for (const row of rows) {
    const bucket = byAggregate.get(row.aggregate_id)
    if (bucket === undefined) byAggregate.set(row.aggregate_id, [row])
    else bucket.push(row)
  }

  const aggregates: GoldenAggregate[] = [...byAggregate.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([aggregate_id, bucket]) => ({
      aggregate_id,
      rows: bucket.slice().sort((left, right) => left.seq - right.seq),
    }))

  const sequence: GoldenSequenceEntry[] = aggregates.flatMap((aggregate) =>
    aggregate.rows.map((row) => ({ aggregate_id: aggregate.aggregate_id, seq: row.seq, type: row.type })),
  )

  return deepFreeze({ sequence, aggregates })
}

export function createGoldenCapture(options: GoldenCaptureOptions = {}): GoldenCapture {
  const capture = options.capture ?? createCapture(options.clock ? { clock: options.clock } : {})

  const durable = (row: DurableRow): GoldenRow => {
    const parsed = parseDurableRow(row)
    capture.dbWrite(DURABLE_KIND, parsed)
    return parsed
  }

  const recordedRows = (): GoldenRow[] =>
    capture
      .dbWrites()
      .filter((record) => record.kind === DURABLE_KIND)
      // Records are parsed, not cast: a raw `db` record written directly onto
      // the shared timeline cannot smuggle an invalid row past `rows`/`snapshot`.
      .map((record) => parseDurableRow(record.data as DurableRow))

  return {
    capture,
    durable,
    get rows() {
      return recordedRows()
    },
    sequence: () => recordedRows().map((row) => ({ aggregate_id: row.aggregate_id, seq: row.seq, type: row.type })),
    snapshot: () => canonicalize(recordedRows()),
    reset: () => capture.reset(),
  }
}

export function diffGolden(expected: GoldenSnapshot, actual: GoldenSnapshot): GoldenDifference[] {
  const differences: GoldenDifference[] = []
  const expectedByAggregate = new Map(expected.aggregates.map((aggregate) => [aggregate.aggregate_id, aggregate]))
  const actualByAggregate = new Map(actual.aggregates.map((aggregate) => [aggregate.aggregate_id, aggregate]))

  for (const aggregateID of expectedByAggregate.keys()) {
    if (!actualByAggregate.has(aggregateID)) differences.push({ kind: "missing-aggregate", aggregate_id: aggregateID })
  }
  for (const aggregateID of actualByAggregate.keys()) {
    if (!expectedByAggregate.has(aggregateID)) differences.push({ kind: "extra-aggregate", aggregate_id: aggregateID })
  }

  for (const [aggregateID, expectedAggregate] of expectedByAggregate) {
    const actualAggregate = actualByAggregate.get(aggregateID)
    if (actualAggregate === undefined) continue

    const expectedBySeq = new Map(expectedAggregate.rows.map((row) => [row.seq, row]))
    const actualBySeq = new Map(actualAggregate.rows.map((row) => [row.seq, row]))

    for (const seq of expectedBySeq.keys()) {
      if (!actualBySeq.has(seq)) differences.push({ kind: "missing-row", aggregate_id: aggregateID, seq })
    }
    for (const seq of actualBySeq.keys()) {
      if (!expectedBySeq.has(seq)) differences.push({ kind: "extra-row", aggregate_id: aggregateID, seq })
    }

    for (const [seq, expectedRow] of expectedBySeq) {
      const actualRow = actualBySeq.get(seq)
      if (actualRow === undefined) continue
      for (const field of COMPARED_FIELDS) {
        if (!isDeepStrictEqual(expectedRow[field], actualRow[field])) {
          differences.push({
            kind: "field-mismatch",
            aggregate_id: aggregateID,
            seq,
            field,
            expected: expectedRow[field],
            actual: actualRow[field],
          })
        }
      }
    }
  }

  return differences
}

function stringify(value: unknown): string {
  if (value === undefined) return "undefined"
  try {
    return JSON.stringify(value) ?? String(value)
  } catch {
    return String(value)
  }
}

export function formatGoldenDiff(differences: ReadonlyArray<GoldenDifference>, context?: string): string {
  const header = context === undefined ? "golden mismatch" : `golden mismatch: ${context}`
  const lines = differences.map((difference) => {
    switch (difference.kind) {
      case "missing-aggregate":
        return `  - missing aggregate ${difference.aggregate_id}`
      case "extra-aggregate":
        return `  + extra aggregate ${difference.aggregate_id}`
      case "missing-row":
        return `  - missing row ${difference.aggregate_id}#${difference.seq}`
      case "extra-row":
        return `  + extra row ${difference.aggregate_id}#${difference.seq}`
      case "field-mismatch":
        return `  ~ ${difference.aggregate_id}#${difference.seq}.${difference.field}: expected ${stringify(
          difference.expected,
        )} but got ${stringify(difference.actual)}`
    }
  })
  return [header, ...lines].join("\n")
}

/** Throw a descriptive error unless the two golden snapshots are fully equivalent. */
export function assertGoldenEqual(expected: GoldenSnapshot, actual: GoldenSnapshot, context?: string): void {
  const differences = diffGolden(expected, actual)
  if (differences.length > 0) throw new Error(formatGoldenDiff(differences, context))
}
