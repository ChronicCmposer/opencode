// Writer-stamped, generation-scoped global ordinal.

export interface WriterOrdinal {
  readonly generation: number
  readonly ordinal: number
}

/** The seam the writer implements; `next()` stamps one committed event. */
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
 * Deterministic stand-in for the writer ordinal: increments the ordinal by one per call within a fixed
 * generation.
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
