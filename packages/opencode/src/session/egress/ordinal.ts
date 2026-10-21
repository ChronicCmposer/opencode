// Phase 8.5 — writer-stamped, generation-scoped global ordinal (plan task 8.5).
//
// PHASE 7 DEPENDENCY (interface assumption, documented):
//   The single-writer Worker (Phase 7) is responsible for stamping a global,
//   monotonically increasing `ordinal` at commit, scoped to a `generation`.
//   `generation` is bumped whenever the writer is respawned (crash/resupervision),
//   so ordinals from a dead writer can never be compared against a live one.
//   The egress frame carries `{generation, ordinal}` as plain `number` fields;
//   no schema change (plan decision line 66).
//
//   Until Phase 7 lands we expose `OrdinalSource` as the seam the writer will
//   implement, plus `createMonotonicOrdinalSource` as a deterministic fallback.
//
// Ordering is `(generation, ordinal)` lexicographic: a higher generation always
// wins, so a writer restart resets the ordinal without breaking commit order for
// consumers.

export interface WriterOrdinal {
  readonly generation: number
  readonly ordinal: number
}

/** The seam the Phase 7 writer implements; `next()` stamps one committed event. */
export interface OrdinalSource {
  next(): WriterOrdinal
}

/** Canonical `(generation, ordinal)` ordering: negative / zero / positive. */
export function compareOrdinals(a: WriterOrdinal, b: WriterOrdinal): number {
  if (a.generation !== b.generation) return a.generation - b.generation
  return a.ordinal - b.ordinal
}

export interface MonotonicOrdinalOptions {
  /** Writer generation; defaults to 1. */
  readonly generation?: number
  /** First ordinal returned; defaults to 1. */
  readonly start?: number
}

/**
 * Deterministic stand-in for the Phase 7 writer ordinal: increments the ordinal
 * by one per call within a fixed generation.
 */
export function createMonotonicOrdinalSource(options: MonotonicOrdinalOptions = {}): OrdinalSource {
  const generation = options.generation ?? 1
  const start = options.start ?? 1
  if (!Number.isInteger(generation)) throw new Error(`invalid ordinal generation: ${generation} (expected an integer)`)
  if (!Number.isInteger(start)) throw new Error(`invalid ordinal start: ${start} (expected an integer)`)
  let ordinal = start - 1
  return {
    next: () => ({ generation, ordinal: ++ordinal }),
  }
}

export * as EgressOrdinal from "./ordinal"
