// Parity matrix: modes x database x plugins x crash phases.
//
// This module is the selection and comparison layer, not a production driver.
// A caller supplies one `MatrixFixture` that knows how to configure the
// scripted session for a cell; the harness runs the SAME script through
// `runParitySession` (in-process and worker sides), then compares the stable
// event/status sequences and normalized durable rows.
//
// The matrix never skips silently. Every cell is planned as `run`, `todo`
// (owner named), or `worker-only` (owner named and explicitly listed). A cell
// that is merely "not implemented" cannot disappear from the matrix.

import type { SinkEvent } from "./event-sink"
import type { MockProviderOptions } from "./mock-provider"
import { normalizeDurableRow } from "./normalize"
import {
  compareDurable,
  compareSequences,
  runParitySession,
  type DurableComparison,
  type ParityDurableRow,
  type ParityResult,
  type ParityRunner,
  type SequenceComparison,
  type SessionScript,
} from "./parity-session"

/** The execution models compared by every cell. */
export const PARITY_MODES = ["in-process", "worker"] as const
export type ParityMode = (typeof PARITY_MODES)[number]

/** The durable store backing a cell. */
export const PARITY_DATABASES = ["memory", "file"] as const
export type ParityDatabase = (typeof PARITY_DATABASES)[number]

/** Plugin configurations a cell must cover. */
export const PARITY_PLUGINS = ["none", "supported", "unsupported"] as const
export type ParityPlugins = (typeof PARITY_PLUGINS)[number]

/** Crash phases a cell must cover (`none` is the happy path). */
export const PARITY_CRASHES = ["none", "boot", "stream", "tool", "commit"] as const
export type ParityCrash = (typeof PARITY_CRASHES)[number]

/** One point in the matrix. `id` is the stable selection key. */
export interface ParityCell {
  readonly id: string
  readonly mode: ParityMode
  readonly db: ParityDatabase
  readonly plugins: ParityPlugins
  readonly crash: ParityCrash
}

/** The stable, parseable cell id used by filters and todo names. */
export function cellID(
  mode: ParityMode,
  db: ParityDatabase,
  plugins: ParityPlugins,
  crash: ParityCrash,
): string {
  return `${mode}:${db}:${plugins}:${crash}`
}

/** Build one cell, routing through {@link cellID} so id and fields cannot drift. */
export function parityCell(
  mode: ParityMode,
  db: ParityDatabase,
  plugins: ParityPlugins,
  crash: ParityCrash,
): ParityCell {
  return { id: cellID(mode, db, plugins, crash), mode, db, plugins, crash }
}

/** The full 2 x 2 x 3 x 5 matrix in deterministic order. */
export function createParityMatrix(): ReadonlyArray<ParityCell> {
  const cells: ParityCell[] = []
  for (const mode of PARITY_MODES) {
    for (const db of PARITY_DATABASES) {
      for (const plugins of PARITY_PLUGINS) {
        for (const crash of PARITY_CRASHES) {
          cells.push(parityCell(mode, db, plugins, crash))
        }
      }
    }
  }
  return cells
}

function parseMember<const Values extends readonly string[]>(
  values: Values,
  value: string,
  label: string,
): Values[number] {
  if (!(values as readonly string[]).includes(value)) {
    throw new Error(`invalid parity ${label} "${value}" (expected one of ${values.join(", ")})`)
  }
  return value as Values[number]
}

/** Parse a cell id back into its trusted fields, failing loud on any malformed part. */
export function parseCellID(id: string): ParityCell {
  const parts = id.split(":")
  if (parts.length !== 4) {
    throw new Error(`invalid parity cell id "${id}" (expected mode:db:plugins:crash)`)
  }
  const [mode, db, plugins, crash] = parts as [string, string, string, string]
  return parityCell(
    parseMember(PARITY_MODES, mode, "mode"),
    parseMember(PARITY_DATABASES, db, "database"),
    parseMember(PARITY_PLUGINS, plugins, "plugins"),
    parseMember(PARITY_CRASHES, crash, "crash"),
  )
}

export interface MatrixFilter {
  /** Restrict to these cell ids; unknown ids fail loud when selecting. */
  readonly ids?: ReadonlyArray<string>
  readonly modes?: ReadonlyArray<ParityMode>
  readonly databases?: ReadonlyArray<ParityDatabase>
  readonly plugins?: ReadonlyArray<ParityPlugins>
  readonly crashes?: ReadonlyArray<ParityCrash>
  /** `pr` keeps the happy path cell of every other dimension; `full` is the matrix. */
  readonly subset?: "pr" | "full"
}

/** The PR subset: both modes x both stores x all plugin states, no crash injection. */
export function prSubset(): ReadonlyArray<ParityCell> {
  return selectParityCells(createParityMatrix(), { subset: "pr" })
}

/** Parse a comma-separated selection, dropping empty entries. */
export function parseCellList(value: string): ReadonlyArray<string> {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
}

/** Read the selection a run was started with, failing loud on a bad subset value. */
export function selectionFromEnv(env: Record<string, string | undefined> = process.env): MatrixFilter {
  const ids = env["OPENCODE_PARITY_CELLS"]
  const subset = env["OPENCODE_PARITY_SUBSET"]
  if (subset !== undefined && subset !== "" && subset !== "pr" && subset !== "full") {
    throw new Error(`invalid OPENCODE_PARITY_SUBSET "${subset}" (expected "pr" or "full")`)
  }
  return {
    ...(ids === undefined || ids === "" ? {} : { ids: parseCellList(ids) }),
    ...(subset === "pr" || subset === "full" ? { subset } : {}),
  }
}

function inSet<T>(values: ReadonlyArray<T> | undefined, value: T): boolean {
  return values === undefined || values.includes(value)
}

/**
 * Select cells in matrix order. An empty selection fails loud: a parity run that
 * silently exercises zero cells would report success without evidence.
 */
export function selectParityCells(
  matrix: ReadonlyArray<ParityCell>,
  filter: MatrixFilter = {},
): ReadonlyArray<ParityCell> {
  const ids = filter.ids === undefined ? undefined : new Set(filter.ids)
  if (ids !== undefined) {
    for (const id of ids) parseCellID(id)
  }

  const selected = matrix.filter((cell) => {
    if (filter.subset === "pr" && cell.crash !== "none") return false
    if (ids !== undefined && !ids.has(cell.id)) return false
    return (
      inSet(filter.modes, cell.mode) &&
      inSet(filter.databases, cell.db) &&
      inSet(filter.plugins, cell.plugins) &&
      inSet(filter.crashes, cell.crash)
    )
  })

  if (selected.length === 0) {
    throw new Error(`parity selection matched no cells (${JSON.stringify(filter)})`)
  }
  return selected
}

/** Fail loud when the matrix contains a duplicate or self-inconsistent cell. */
export function assertWellFormedMatrix(matrix: ReadonlyArray<ParityCell>): void {
  const seen = new Set<string>()
  for (const cell of matrix) {
    const expected = cellID(cell.mode, cell.db, cell.plugins, cell.crash)
    if (cell.id !== expected) {
      throw new Error(`parity cell id "${cell.id}" does not match its fields (expected "${expected}")`)
    }
    if (seen.has(cell.id)) throw new Error(`duplicate parity cell "${cell.id}"`)
    seen.add(cell.id)
  }
}

/** Whether a cell can run, or why it cannot (with the owning agent). */
export type CellCapability =
  | { readonly available: true }
  | { readonly available: false; readonly owner: string; readonly reason: string }

export const available: CellCapability = Object.freeze({ available: true })

/** Build an unavailable capability, rejecting the empty owner/reason at the boundary. */
export function unavailable(owner: string, reason: string): CellCapability {
  if (owner.trim() === "") throw new Error("an unavailable parity cell needs a named owner")
  if (reason.trim() === "") throw new Error(`unavailable parity cell owned by "${owner}" needs a reason`)
  return Object.freeze({ available: false, owner, reason })
}

export type CellCapabilityResolver = (cell: ParityCell) => CellCapability

/** A case that deliberately has no in-process counterpart, declared with an owner. */
export interface WorkerOnlyCase {
  readonly id: string
  readonly owner: string
  readonly reason: string
}

/**
 * Crash injection happens inside the worker isolate lifecycle (boot handshake,
 * stream, tool execution, commit). The in-process side has no equivalent hook,
 * so those cells are explicitly worker-only rather than silently skipped.
 */
export function workerOnlyCase(cell: ParityCell): WorkerOnlyCase | undefined {
  if (cell.mode !== "in-process" || cell.crash === "none") return undefined
  return {
    id: cell.id,
    owner: "w3-crash",
    reason: `crash phase "${cell.crash}" is injected through the worker isolate lifecycle; the in-process mode has no equivalent phase hook`,
  }
}

/** Enumerate the explicit worker-only list for a matrix (never silent). */
export function workerOnlyCases(matrix: ReadonlyArray<ParityCell>): ReadonlyArray<WorkerOnlyCase> {
  return matrix.flatMap((cell) => {
    const declared = workerOnlyCase(cell)
    return declared === undefined ? [] : [declared]
  })
}

export type PlanDisposition = "run" | "todo" | "worker-only"

/** One planned cell and the reason it will run, wait for an owner, or skip by rule. */
export interface PlannedCell {
  readonly cell: ParityCell
  readonly disposition: PlanDisposition
  readonly owner?: string
  readonly reason?: string
}

/**
 * Decide every cell's disposition. Worker-only rules win over capability: the
 * absence is structural, not a missing feature.
 */
export function planMatrix(
  matrix: ReadonlyArray<ParityCell>,
  capability: CellCapabilityResolver,
): ReadonlyArray<PlannedCell> {
  return matrix.map((cell) => {
    const declared = workerOnlyCase(cell)
    if (declared !== undefined) {
      return { cell, disposition: "worker-only", owner: declared.owner, reason: declared.reason }
    }
    const resolved = capability(cell)
    if (resolved.available) return { cell, disposition: "run" }
    return { cell, disposition: "todo", owner: resolved.owner, reason: resolved.reason }
  })
}

/**
 * Prove that no planned cell vanished silently. Every non-running cell must name
 * an owner and reason, and every declared worker-only case must be represented
 * with that same owner.
 */
export function assertNoSilentSkips(
  plan: ReadonlyArray<PlannedCell>,
  workerOnly: ReadonlyArray<WorkerOnlyCase> = [],
): void {
  const byID = new Map<string, PlannedCell>()
  for (const entry of plan) {
    if (byID.has(entry.cell.id)) throw new Error(`parity plan contains duplicate cell "${entry.cell.id}"`)
    byID.set(entry.cell.id, entry)
  }

  const declaredIDs = new Set(workerOnly.map((entry) => entry.id))
  for (const entry of plan) {
    if (entry.disposition === "run") continue
    if (entry.owner === undefined || entry.owner.trim() === "") {
      throw new Error(`parity cell "${entry.cell.id}" is ${entry.disposition} without an owner`)
    }
    if (entry.reason === undefined || entry.reason.trim() === "") {
      throw new Error(`parity cell "${entry.cell.id}" is ${entry.disposition} without a reason`)
    }
    if (entry.disposition === "worker-only" && !declaredIDs.has(entry.cell.id)) {
      throw new Error(`parity cell "${entry.cell.id}" is worker-only but not on the explicit list`)
    }
  }

  for (const declared of workerOnly) {
    const planned = byID.get(declared.id)
    if (planned === undefined) {
      throw new Error(`declared worker-only case "${declared.id}" is missing from the plan`)
    }
    if (planned.disposition !== "worker-only") {
      throw new Error(`declared worker-only case "${declared.id}" was planned as "${planned.disposition}"`)
    }
    if (planned.owner !== declared.owner) {
      throw new Error(
        `worker-only case "${declared.id}" owner mismatch: planned "${planned.owner}", declared "${declared.owner}"`,
      )
    }
  }
}

/** The two mode cells that share every non-mode coordinate. */
export interface ParityPair {
  readonly inProcess: ParityCell
  readonly worker: ParityCell
}

/** Resolve the paired in-process/worker cell for any selected cell. */
export function parityPair(cell: ParityCell): ParityPair {
  const parsed = parseCellID(cell.id)
  return {
    inProcess: parityCell("in-process", parsed.db, parsed.plugins, parsed.crash),
    worker: parityCell("worker", parsed.db, parsed.plugins, parsed.crash),
  }
}

/** How a fixture configures one matrix cell. */
export interface MatrixFixture {
  readonly script: SessionScript
  /** Build the runner for one side's cell (the harness invokes it once per mode). */
  readonly run: (cell: ParityCell) => ParityRunner
  readonly provider?: MockProviderOptions
  readonly timeoutMs?: number
  /** Event types that are hub-local telemetry and not part of the compared contract. */
  readonly ignoreEventTypes?: ReadonlyArray<string>
  /** Durable projection; defaults to generated ids collapsed and clocks zeroed. */
  readonly normalizeDurable?: (row: ParityDurableRow) => unknown
}

/** The captured, compared outcome of one matrix cell. */
export interface MatrixCellResult {
  readonly cell: ParityCell
  readonly parity: ParityResult
  readonly events: SequenceComparison
  readonly statuses: SequenceComparison
  readonly durable: DurableComparison
  readonly equal: boolean
}

/** Status-like events whose order is part of the user-visible contract. */
const STATUS_EVENT_TYPES: ReadonlySet<string> = new Set(["session.status", "session.idle"])

/** The status subsequence of a capture, in emission order. */
export function statusEvents(events: ReadonlyArray<SinkEvent>): ReadonlyArray<SinkEvent> {
  return events.filter((event) => STATUS_EVENT_TYPES.has(event.type))
}

function compareStatuses(off: ParityResult["off"], on: ParityResult["on"]): SequenceComparison {
  return compareSequences(statusEvents(off.events), statusEvents(on.events))
}

function describeCellError(side: ParityResult["off"] | ParityResult["on"]): string | undefined {
  return side.error === undefined ? undefined : `side ${side.flag ? "worker" : "in-process"} failed: ${side.error}`
}

/** Fail loud with the first divergence, or return the compared result unchanged. */
export function assertCellParity(result: MatrixCellResult): MatrixCellResult {
  if (result.equal) return result
  const failures = [
    describeCellError(result.parity.off),
    describeCellError(result.parity.on),
    result.events.equal ? undefined : `event sequence diverged (${result.events.mismatches.length} mismatch(es))`,
    result.statuses.equal ? undefined : `status sequence diverged (${result.statuses.mismatches.length} mismatch(es))`,
    result.durable.equal
      ? undefined
      : `durable rows diverged (${result.durable.missingInOn.length} missing on worker, ${result.durable.missingInOff.length} extra)`,
  ].filter((failure): failure is string => failure !== undefined)
  throw new Error(`parity cell "${result.cell.id}" failed: ${failures.join("; ")}`)
}

/**
 * Run one cell's mode pair through `runParitySession` and compare both sides.
 * The selected cell chooses the shared DB/plugin/crash coordinates and the
 * fixture is asked for each mode's own configuration.
 *
 * The function never throws for a behavioral divergence: it returns the
 * comparison so a test can report all failures uniformly and a runner can
 * persist the evidence.
 */
export async function runMatrixCell(cell: ParityCell, fixture: MatrixFixture): Promise<MatrixCellResult> {
  const parsed = parseCellID(cell.id)
  if (parsed.id !== cell.id) throw new Error(`parity cell "${cell.id}" is not canonical`)
  const pair = parityPair(parsed)

  const parity = await runParitySession({
    script: fixture.script,
    run: (context) => fixture.run(context.flag ? pair.worker : pair.inProcess)(context),
    ...(fixture.provider === undefined ? {} : { provider: fixture.provider }),
    ...(fixture.timeoutMs === undefined ? {} : { timeoutMs: fixture.timeoutMs }),
  })

  const events = compareSequences(parity.off, parity.on, {
    ...(fixture.ignoreEventTypes === undefined ? {} : { ignoreTypes: fixture.ignoreEventTypes }),
  })
  const statuses = compareStatuses(parity.off, parity.on)
  const durable = compareDurable(parity.off, parity.on, {
    normalize: fixture.normalizeDurable ?? normalizeDurableRow,
  })

  return {
    cell: parsed,
    parity,
    events,
    statuses,
    durable,
    equal: events.equal && statuses.equal && durable.equal,
  }
}
