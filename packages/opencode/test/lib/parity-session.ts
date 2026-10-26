// Flag-ON ≡ flag-OFF session parity harness (split from `parity-harness`).
//
// `runParitySession` starts ONE deterministic mock provider and runs ONE
// identical scripted session through an injected runner twice — `flag:false`
// (in-process / flag-OFF) and `flag:true` (worker pool / flag-ON) — capturing
// each side's event sequence via `event-sink`. `compareSequences` /
// `compareDurable` and the golden helpers then assert the two sides match.
//
// The runner owns how a mode is built (which layers, which flag) and receives a
// `capture` factory so its `EventV2` instance is the one the sink observes. The
// harness is failure-tolerant on purpose: a flag-ON runner that currently cannot
// deliver worker events (or hangs) is captured as `side.error`, never thrown, so
// the suite stays green while a known-red goal waits for the fix.

import { withDeadline } from "./async"
import { compareMultisets, keyAt, multisetDifference, stableStringify } from "./compare"
import { startMockProvider, type MockProviderHandle, type MockProviderOptions } from "./mock-provider"
import {
  createEventSink,
  type EventSink,
  type EventSinkOptions,
  type EventSinkSource,
  type SinkEvent,
} from "./event-sink"
import type { InstanceContext } from "@/project/instance-context"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { parityProject } from "./parity-project"

const DEFAULT_TIMEOUT_MS = 30_000

/** The environment-neutral description of the session both modes run. */
export interface SessionScript {
  /** Stable name used for the golden fixture and reports. */
  readonly name: string
  /** The user prompt handed to the session. */
  readonly prompt: string
  /** Mock-provider scripts; the provider is started by the harness. */
  readonly provider?: MockProviderOptions
}

/** One row read back from the durable store, tagged with its table. */
export interface ParityDurableRow {
  readonly table: string
  readonly row: Record<string, unknown>
}

/** @deprecated Use {@link ParityDurableRow}; kept so existing imports keep compiling. */
export type DurableRow = ParityDurableRow

/**
 * How a runner reports the durable rows its side wrote. The harness collects
 * them per side so flag-ON and flag-OFF can be compared on committed state, not
 * just on the live event sequence.
 */
export interface DurableRecorder {
  add(table: string, rows: ReadonlyArray<Record<string, unknown>>): void
}

/** Everything an injected runner needs to execute one flag-state of the session. */
export interface ParityRunContext {
  readonly flag: boolean
  readonly script: SessionScript
  /** The started mock provider (`baseUrl`/`apiKey`/`model` + observed requests). */
  readonly provider: MockProviderHandle
  /**
   * Create an event sink bound to the runner's own runtime/layer. The harness
   * collects every sink created here for the side's sequence.
   */
  readonly capture: (source: EventSinkSource, options?: EventSinkOptions) => EventSink
  /** Report the durable rows this side wrote (message/part/event/…). */
  readonly durable: DurableRecorder
  /**
   * An ambient `InstanceContext` a real runner can provide as `InstanceRef` so
   * `InstanceState`-backed services (e.g. `SessionStatus`) resolve a directory.
   * No project loader is attached: a parity runner needs no persisted project.
   */
  readonly instance: InstanceContext
  /** Fresh ids for the scripted session's events (stable per side, unique per run). */
  readonly sessionID: SessionID
  readonly messageID: MessageID
  readonly partID: PartID
  /**
   * Aborted when this side's timeout fires, so a long-running runner can stop
   * its work instead of leaking past the deadline (the harness still records the
   * timeout as `side.error`).
   */
  readonly signal: AbortSignal
}

/** Runs one flag-state of the scripted session; may reject (captured by the harness). */
export type ParityRunner = (context: ParityRunContext) => Promise<void> | void

/** The captured outcome of one flag-state. */
export interface ParitySide {
  readonly flag: boolean
  readonly events: ReadonlyArray<SinkEvent>
  readonly types: ReadonlyArray<string>
  /** The durable rows this side reported through {@link DurableRecorder}. */
  readonly durable: ReadonlyArray<ParityDurableRow>
  /** The runner's failure message, or `undefined` when it completed. */
  readonly error: string | undefined
  readonly durationMs: number
}

export interface ParityResult {
  readonly script: string
  readonly off: ParitySide
  readonly on: ParitySide
}

export interface RunParitySessionOptions {
  readonly script: SessionScript
  readonly run: ParityRunner
  /** Provider overrides; defaults to `script.provider`. */
  readonly provider?: MockProviderOptions
  /** Per-side timeout. Defaults to 30000 ms. */
  readonly timeoutMs?: number
}

/** The label used in timeout/error messages for a flag state. */
function flagLabel(flag: boolean): string {
  return flag ? "ON (worker pool)" : "OFF (in-process)"
}

/** A stable per-side directory for the ambient instance context (never touched on disk). */
function instanceDirectory(flag: boolean, name: string): string {
  return `/tmp/opencode-parity/${name}/${flag ? "on" : "off"}`
}

/** The script-level ids shared by both flag states (the same logical session). */
interface ScriptIds {
  readonly sessionID: SessionID
  readonly messageID: MessageID
  readonly partID: PartID
}

/** Run one flag state, collecting every sink the runner creates and swallowing failures. */
async function runSide(
  flag: boolean,
  options: RunParitySessionOptions,
  provider: MockProviderHandle,
  timeoutMs: number,
  ids: ScriptIds,
): Promise<ParitySide> {
  const sinks: EventSink[] = []
  const capture = (source: EventSinkSource, sinkOptions?: EventSinkOptions): EventSink => {
    const sink = createEventSink(source, sinkOptions)
    sinks.push(sink)
    return sink
  }
  const durable: ParityDurableRow[] = []
  const durableRecorder: DurableRecorder = {
    add: (table, rows) => {
      for (const row of rows) durable.push({ table, row })
    },
  }

  const startedAt = performance.now()
  let error: string | undefined
  const controller = new AbortController()
  const instance: InstanceContext = {
    directory: instanceDirectory(flag, options.script.name),
    worktree: instanceDirectory(flag, options.script.name),
    // A parity runner needs no project loader; the only reader on the event path
    // (`EventV2Bridge`'s GlobalBus listener) requires `project.id` to be present.
    // The record is parsed once at this boundary rather than forced past the type.
    project: parityProject("parity"),
  }
  try {
    const run = Promise.resolve(
      options.run({
        flag,
        script: options.script,
        provider,
        capture,
        durable: durableRecorder,
        instance,
        sessionID: ids.sessionID,
        messageID: ids.messageID,
        partID: ids.partID,
        signal: controller.signal,
      }),
    )
    await withDeadline(run, {
      timeoutMs,
      message: `parity ${flagLabel(flag)} run timed out after ${timeoutMs}ms`,
      onTimeout: () => controller.abort(),
    })
  } catch (cause) {
    error = cause instanceof Error ? cause.message : String(cause)
  }

  const events = sinks.flatMap((sink) => sink.events())
  await Promise.all(sinks.map((sink) => sink.dispose().catch(() => undefined)))

  return {
    flag,
    events,
    types: events.map((event) => event.type),
    durable,
    error,
    durationMs: performance.now() - startedAt,
  }
}

/**
 * Run the scripted session in both flag states and return their captured event
 * sequences. Never throws: a runner failure or timeout is recorded on the side.
 * The script-level ids are shared across both states so the two sequences are
 * comparable on `{ type, properties }` (per-publish ids are still ignored).
 */
export async function runParitySession(options: RunParitySessionOptions): Promise<ParityResult> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS
  const ids: ScriptIds = {
    sessionID: SessionID.descending(),
    messageID: MessageID.ascending(),
    partID: PartID.ascending(),
  }
  const provider = startMockProvider(options.provider ?? options.script.provider ?? {})
  try {
    const off = await runSide(false, options, provider, timeoutMs, ids)
    const on = await runSide(true, options, provider, timeoutMs, ids)
    return { script: options.script.name, off, on }
  } finally {
    await provider.stop().catch(() => undefined)
  }
}

// --- Comparison -------------------------------------------------------------

/** A captured sequence, or an object that carries one (`ParitySide`, golden fixture). */
export type SequenceInput = ReadonlyArray<SinkEvent> | { readonly events: ReadonlyArray<SinkEvent> }

export interface CompareOptions {
  /** Ignore event ids (the default: ids are non-deterministic). */
  readonly ignoreIds?: boolean
  /** Drop these event types before comparing (e.g. hub-only lifecycle noise). */
  readonly ignoreTypes?: ReadonlyArray<string>
  /** Override the canonicalization used to compare two events. */
  readonly key?: (event: SinkEvent) => string
}

/** A positional divergence between the two sequences. */
export interface SequenceMismatch {
  readonly index: number
  readonly off: SinkEvent | undefined
  readonly on: SinkEvent | undefined
}

export interface SequenceComparison {
  readonly equal: boolean
  readonly offCount: number
  readonly onCount: number
  /** Events present in `off` (multiset) with no counterpart in `on`. */
  readonly missingInOn: ReadonlyArray<SinkEvent>
  /** Events present in `on` (multiset) with no counterpart in `off`. */
  readonly missingInOff: ReadonlyArray<SinkEvent>
  /** Positional divergences (`off[i]` vs `on[i]`). */
  readonly mismatches: ReadonlyArray<SequenceMismatch>
}

function isEventArray(input: SequenceInput): input is ReadonlyArray<SinkEvent> {
  return Array.isArray(input)
}

function eventsOf(input: SequenceInput): ReadonlyArray<SinkEvent> {
  return isEventArray(input) ? input : input.events
}

/** The default canonical form: `{type, properties}`, with ids ignored by default. */
function canonicalKey(event: SinkEvent, ignoreIds: boolean): string {
  const shaped = ignoreIds
    ? { type: event.type, properties: event.properties }
    : { id: event.id, type: event.type, properties: event.properties }
  return stableStringify(shaped)
}

/**
 * Compare two captured sequences. Ids are ignored by default (they are generated
 * per publish and differ between runs), so flag-ON and flag-OFF can be compared
 * on `{ type, properties }` alone.
 */
export function compareSequences(
  offInput: SequenceInput,
  onInput: SequenceInput,
  options: CompareOptions = {},
): SequenceComparison {
  const ignoreIds = options.ignoreIds ?? true
  const ignoreTypes = new Set(options.ignoreTypes ?? [])
  const keyOf = options.key ?? ((event: SinkEvent) => canonicalKey(event, ignoreIds))

  const keep = (event: SinkEvent) => !ignoreTypes.has(event.type)
  const off = eventsOf(offInput).filter(keep)
  const on = eventsOf(onInput).filter(keep)

  const mismatches: SequenceMismatch[] = []
  for (let index = 0; index < Math.max(off.length, on.length); index++) {
    if (keyAt(off, index, keyOf) === keyAt(on, index, keyOf)) continue
    mismatches.push({ index, off: off[index], on: on[index] })
  }

  const missingInOn = multisetDifference(off, on, keyOf)
  const missingInOff = multisetDifference(on, off, keyOf)
  const equal = off.length === on.length && mismatches.length === 0

  return { equal, offCount: off.length, onCount: on.length, missingInOn, missingInOff, mismatches }
}

// --- Durable-row comparison -------------------------------------------------

/** A captured durable set, or an object that carries one (`ParitySide`). */
export type DurableInput = ReadonlyArray<ParityDurableRow> | { readonly durable: ReadonlyArray<ParityDurableRow> }

export interface CompareDurableOptions {
  /**
   * Project a row to the comparable shape, dropping volatile columns
   * (timestamps, generated ids). Defaults to the whole `{ table, row }`.
   */
  readonly normalize?: (row: ParityDurableRow) => unknown
}

export interface DurableComparison {
  readonly equal: boolean
  readonly offCount: number
  readonly onCount: number
  /** Rows present in `off` (multiset) with no counterpart in `on`. */
  readonly missingInOn: ReadonlyArray<ParityDurableRow>
  /** Rows present in `on` (multiset) with no counterpart in `off`. */
  readonly missingInOff: ReadonlyArray<ParityDurableRow>
}

function isParityDurableRowArray(input: DurableInput): input is ReadonlyArray<ParityDurableRow> {
  return Array.isArray(input)
}

function durableRowsOf(input: DurableInput): ReadonlyArray<ParityDurableRow> {
  return isParityDurableRowArray(input) ? input : input.durable
}

/**
 * Compare two durable captures as multisets, keyed by the normalized row. This
 * is the committed-state twin of {@link compareSequences}: flag-ON must write
 * the same rows as flag-OFF (modulo volatile columns), exactly once each.
 */
export function compareDurable(
  offInput: DurableInput,
  onInput: DurableInput,
  options: CompareDurableOptions = {},
): DurableComparison {
  const comparison = compareMultisets(durableRowsOf(offInput), durableRowsOf(onInput), {
    normalize: options.normalize ?? ((row) => row),
  })

  return {
    equal: comparison.equal,
    offCount: comparison.leftCount,
    onCount: comparison.rightCount,
    missingInOn: comparison.missingInRight,
    missingInOff: comparison.missingInLeft,
  }
}

// --- Golden fixture ---------------------------------------------------------

/** The flag-OFF reference sequence, suitable for committing as a fixture. */
export interface GoldenReference {
  readonly name: string
  readonly events: ReadonlyArray<SinkEvent>
}

/** Project a parity result's flag-OFF side into a golden fixture. */
export function goldenFromParity(result: ParityResult, name: string = result.script): GoldenReference {
  return { name, events: result.off.events }
}

/** Write the flag-OFF reference to `path` as JSON (creating no directories). */
export async function recordGolden(path: string, result: ParityResult, name?: string): Promise<GoldenReference> {
  const reference = goldenFromParity(result, name)
  await Bun.write(path, JSON.stringify(reference, null, 2))
  return reference
}

/** Parse one untrusted sink event from a golden fixture, failing loud on a bad shape. */
function parseGoldenEvent(value: unknown, path: string, index: number): SinkEvent {
  if (value === null || typeof value !== "object") {
    throw new Error(`golden fixture ${path}: event #${index} is not an object`)
  }
  const record = value as Record<string, unknown>
  if (typeof record.id !== "string" || typeof record.type !== "string") {
    throw new Error(`golden fixture ${path}: event #${index} needs string "id" and "type"`)
  }
  return { id: record.id, type: record.type, properties: record.properties }
}

/** Parse an untrusted golden fixture body into a trusted {@link GoldenReference}. */
export function parseGoldenReference(value: unknown, path = "<memory>"): GoldenReference {
  if (value === null || typeof value !== "object") {
    throw new Error(`golden fixture ${path} is not an object`)
  }
  const record = value as Record<string, unknown>
  if (typeof record.name !== "string") throw new Error(`golden fixture ${path} has no string "name"`)
  if (!Array.isArray(record.events)) throw new Error(`golden fixture ${path} has no "events" array`)
  return {
    name: record.name,
    events: record.events.map((event, index) => parseGoldenEvent(event, path, index)),
  }
}

/** Read a golden fixture, failing loud when it is missing or malformed. */
export async function readGolden(path: string): Promise<GoldenReference> {
  const file = Bun.file(path)
  if (!(await file.exists())) throw new Error(`golden fixture not found: ${path}`)
  return parseGoldenReference(await file.json(), path)
}

/** Compare a captured sequence against the flag-OFF golden reference. */
export function compareToGolden(
  side: SequenceInput,
  reference: GoldenReference,
  options?: CompareOptions,
): SequenceComparison {
  return compareSequences(reference.events, side, options)
}
