// Wave 1 — parity harness for streaming (spec Q1/Q12: "flag-ON must match
// flag-OFF non-worker behavior; core = live output visible").
//
// `runParitySession` starts ONE deterministic mock provider and runs ONE
// identical scripted session through an injected runner twice — `flag:false`
// (in-process / flag-OFF) and `flag:true` (worker pool / flag-ON) — capturing
// each side's event sequence via `event-sink`.
//
// The runner owns how a mode is built (which layers, which flag) and receives a
// `capture` factory so its `EventV2` instance is the one the sink observes. The
// harness is failure-tolerant on purpose: a flag-ON runner that currently cannot
// deliver worker events (or hangs) is captured as `side.error`, never thrown, so
// the suite stays green while the known-red goal waits for Wave 2.

import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import schema from "@opencode-ai/core/database/schema.gen"
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
import { SessionStatus } from "@/session/status"
import { MCP } from "@/mcp"
import { LSP } from "@/lsp/lsp"
import { Provider } from "@/provider/provider"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { createWriterCore } from "@/session/writer/core"
import { serveWriterPort, type HubWriter } from "@/session/writer/client"
import { createWorkerWriterClient, type WorkerWriterClient } from "@/session/worker/writer-client"
import {
  RUNNABLE_SESSION_NODES,
  buildSessionWorkerLayer,
  createHubProxyClient,
  llmModelClientLayer,
  reducedLlmLayer,
  reducedModelProviderLayer,
  serveHubProxy,
  type EventEgressClientHandle,
  type HubProxyClient,
  type HubProxyClients,
  type WorkerModelConfig,
  type WorkerSnapshot,
} from "@/session/worker/layer"
import { buildHubProxyHandlers, type HubProxyAuthorities } from "@/session/worker/hub"

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
export interface DurableRow {
  readonly table: string
  readonly row: Record<string, unknown>
}

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
}

/** Runs one flag-state of the scripted session; may reject (captured by the harness). */
export type ParityRunner = (context: ParityRunContext) => Promise<void> | void

/** The captured outcome of one flag-state. */
export interface ParitySide {
  readonly flag: boolean
  readonly events: ReadonlyArray<SinkEvent>
  readonly types: ReadonlyArray<string>
  /** The durable rows this side reported through {@link DurableRecorder}. */
  readonly durable: ReadonlyArray<DurableRow>
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

/** Reject `promise` with a descriptive error if it has not settled within `timeoutMs`. */
function withTimeout(promise: Promise<void>, timeoutMs: number, message: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), timeoutMs)
    promise.then(
      () => {
        clearTimeout(timer)
        resolve()
      },
      (cause: unknown) => {
        clearTimeout(timer)
        reject(cause)
      },
    )
  })
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
  const durable: DurableRow[] = []
  const durableRecorder: DurableRecorder = {
    add: (table, rows) => {
      for (const row of rows) durable.push({ table, row })
    },
  }

  const startedAt = performance.now()
  let error: string | undefined
  const instance: InstanceContext = {
    directory: instanceDirectory(flag, options.script.name),
    worktree: instanceDirectory(flag, options.script.name),
    // A parity runner needs no project loader; the only reader on the event path
    // (`EventV2Bridge`'s GlobalBus listener) requires `project.id` to be present.
    // The field is typed by `InstanceContext`, so it is parsed once here rather
    // than re-checked by every service.
    project: { id: "parity" } as never,
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
      }),
    )
    await withTimeout(run, timeoutMs, `parity ${flagLabel(flag)} run timed out after ${timeoutMs}ms`)
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

/** Deterministic JSON with sorted object keys, so key order never spuriously diffs. */
function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "undefined"
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`
  const record = value as Record<string, unknown>
  const entries = Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
  return `{${entries.join(",")}}`
}

/** The default canonical form: `{type, properties}`, with ids ignored by default. */
function canonicalKey(event: SinkEvent, ignoreIds: boolean): string {
  const shaped = ignoreIds
    ? { type: event.type, properties: event.properties }
    : { id: event.id, type: event.type, properties: event.properties }
  return stableStringify(shaped)
}

function keyAt(
  events: ReadonlyArray<SinkEvent>,
  index: number,
  keyOf: (event: SinkEvent) => string,
): string | undefined {
  const event = events[index]
  return event === undefined ? undefined : keyOf(event)
}

/** Items of `from` (multiset) whose key is not consumed by `other`. */
function multisetDifference<T>(
  from: ReadonlyArray<T>,
  other: ReadonlyArray<T>,
  keyOf: (item: T) => string,
): T[] {
  const remaining = new Map<string, number>()
  for (const item of other) {
    const key = keyOf(item)
    remaining.set(key, (remaining.get(key) ?? 0) + 1)
  }
  const missing: T[] = []
  for (const item of from) {
    const key = keyOf(item)
    const count = remaining.get(key) ?? 0
    if (count > 0) remaining.set(key, count - 1)
    else missing.push(item)
  }
  return missing
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
export type DurableInput = ReadonlyArray<DurableRow> | { readonly durable: ReadonlyArray<DurableRow> }

export interface CompareDurableOptions {
  /**
   * Project a row to the comparable shape, dropping volatile columns
   * (timestamps, generated ids). Defaults to the whole `{ table, row }`.
   */
  readonly normalize?: (row: DurableRow) => unknown
}

export interface DurableComparison {
  readonly equal: boolean
  readonly offCount: number
  readonly onCount: number
  /** Rows present in `off` (multiset) with no counterpart in `on`. */
  readonly missingInOn: ReadonlyArray<DurableRow>
  /** Rows present in `on` (multiset) with no counterpart in `off`. */
  readonly missingInOff: ReadonlyArray<DurableRow>
}

function isDurableRowArray(input: DurableInput): input is ReadonlyArray<DurableRow> {
  return Array.isArray(input)
}

function durableRowsOf(input: DurableInput): ReadonlyArray<DurableRow> {
  return isDurableRowArray(input) ? input : input.durable
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
  const normalize = options.normalize ?? ((row: DurableRow) => row)
  const keyOf = (row: DurableRow) => stableStringify(normalize(row))
  const off = durableRowsOf(offInput)
  const on = durableRowsOf(onInput)

  const missingInOn = multisetDifference(off, on, keyOf)
  const missingInOff = multisetDifference(on, off, keyOf)

  return {
    equal: off.length === on.length && missingInOn.length === 0 && missingInOff.length === 0,
    offCount: off.length,
    onCount: on.length,
    missingInOn,
    missingInOff,
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

/** Read a golden fixture, failing loud when it is missing. */
export async function readGolden(path: string): Promise<GoldenReference> {
  const file = Bun.file(path)
  if (!(await file.exists())) throw new Error(`golden fixture not found: ${path}`)
  return (await file.json()) as GoldenReference
}

/** Compare a captured sequence against the flag-OFF golden reference. */
export function compareToGolden(
  side: SequenceInput,
  reference: GoldenReference,
  options?: CompareOptions,
): SequenceComparison {
  return compareSequences(reference.events, side, options)
}

// --- Writer-backed worker E2E context ---------------------------------------

/** One `session.status` the worker set through the hub proxy. */
export interface E2EStatusRecord {
  readonly sessionID: string
  readonly status: string
}

export interface E2EContextOptions {
  /** The worker snapshot; its `directory` MUST exist on disk. */
  readonly snapshot: WorkerSnapshot
  /** The serializable model bootstrap the worker talks to. */
  readonly modelConfig: WorkerModelConfig
  /** Extra replacements (e.g. a `SessionProcessor` mock). */
  readonly replacements?: LayerNode.Replacements
  /** Extra nodes to add to the worker graph (e.g. a mock seam). */
  readonly nodes?: ReadonlyArray<LayerNode.AnyNode>
  /**
   * Worker event egress. When supplied, the worker graph forwards its
   * session-scoped events over this handle (e.g. a hub events port) instead of
   * dropping them. Defaults to a no-op sink (the flag-OFF baseline observes the
   * graph's own `EventV2` directly).
   */
  readonly egress?: EventEgressClientHandle
  /**
   * How the worker reaches the hub-resident authorities (`SessionStatus` /
   * `Permission` / `Question`):
   *
   *  * `"local"` (default) serves an in-context recording `SessionStatus`
   *    authority over a private port — the existing E2E behavior;
   *  * `"in-process"` leaves the real `SessionStatus` node in the worker graph,
   *    so status publishes land on the worker's own `EventV2` (flag-OFF parity);
   *  * explicit {@link HubProxyClients} route the calls to a caller-owned hub
   *    (flag-ON: the hub authority publishes `session.status` on the hub surface).
   */
  readonly hubProxies?: "local" | "in-process" | HubProxyClients
}

/**
 * A fully wired, in-process "production worker" over a REAL writer: an
 * in-memory SQLite connection behind the writer core, exposed to the worker over
 * a `MessagePort` and driven by the worker's writer-backed Drizzle `Database`.
 *
 * The `SessionProjector` node is included explicitly: `Session.updateMessage` /
 * `updatePart` publish durable events, and only the projector materializes the
 * `session`/`message`/`part` rows the session loop reads back. Without it the
 * loop commits events but cannot find its own messages.
 */
export interface E2EContext {
  readonly connection: ReturnType<typeof createBunSqliteConnection>
  readonly writer: WorkerWriterClient
  readonly layer: Layer.Layer<unknown, unknown>
  readonly instance: InstanceContext
  /** Every `session.status` the worker set through the hub proxy, in order. */
  readonly statusLog: ReadonlyArray<E2EStatusRecord>
  /** Every writer op the hub served, in order (one entry per statement). */
  readonly writerOps: ReadonlyArray<string>
  /** Read every row of `table` in insertion order. */
  rows(table: string): ReadonlyArray<Record<string, unknown>>
  dispose(): Promise<void>
}

/** Apply the generated base schema to a raw connection (no migrations needed). */
export function applyBaseSchema(connection: { exec(sql: string): void }): void {
  const statements: string[] = []
  Effect.runSync(
    schema.up({
      run: (statement: string) => {
        statements.push(statement)
        return Effect.void
      },
    } as never),
  )
  connection.exec(statements.join(";\n"))
}

/** Build the real hub writer over an in-memory SQLite connection, recording ops. */
function createRecordingHub(
  connection: ReturnType<typeof createBunSqliteConnection>,
  writerOps: string[],
): HubWriter {
  const core = createWriterCore({ connection, sleep: async () => {} })
  const record = <T>(op: string, run: () => Promise<T>): Promise<T> => {
    writerOps.push(op)
    return run()
  }
  return {
    query: (method, sql, params) => record(`query:${method}`, () => core.query(method, sql, params)),
    publish: (event, options) =>
      record(`publish:${event.type}`, () => core.publish({ event, durable: options.durable })),
    begin: () => record("begin", () => core.beginTransaction()),
    txQuery: (handle, method, sql, params) =>
      record(`tx:${method}`, () => core.txQuery(handle, method, sql, params)),
    commit: (handle) => record("commit", () => core.commitTransaction(handle)),
    rollback: (handle) => record("rollback", () => core.rollbackTransaction(handle)),
  }
}

/** The in-context recording hub authorities (`"local"` mode) and their ports. */
interface LocalHubProxies {
  readonly clients: HubProxyClients
  readonly statusLog: E2EStatusRecord[]
  dispose(): void
}

/** Serve a private `SessionStatus`/`Permission`/`Question` authority that records status. */
function createLocalHubProxies(): LocalHubProxies {
  const statusLog: E2EStatusRecord[] = []
  const statusStore = new Map<SessionID, SessionStatus.Info>()
  const authorities: HubProxyAuthorities = {
    sessionStatus: {
      get: (sessionID) => Effect.sync(() => statusStore.get(sessionID) ?? { type: "idle" }),
      set: (sessionID, status) =>
        Effect.sync(() => {
          statusStore.set(sessionID, status)
          statusLog.push({ sessionID, status: status.type })
        }),
      list: () => Effect.sync(() => new Map(statusStore)),
    },
    permission: { ask: () => Effect.void, reply: () => Effect.void, list: () => Effect.succeed([]) },
    question: {
      ask: () => Effect.succeed([]),
      reply: () => Effect.void,
      reject: () => Effect.void,
      list: () => Effect.succeed([]),
    },
  }

  const channel = new MessageChannel()
  const disposeProxy = serveHubProxy(
    buildHubProxyHandlers(authorities, (effect) => Effect.runPromise(effect)),
    channel.port2,
  )
  const client: HubProxyClient = createHubProxyClient(channel.port1)
  return {
    clients: { sessionStatus: client, permission: client, question: client },
    statusLog,
    dispose() {
      disposeProxy()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** Resolve the requested hub-proxy mode; `undefined` means the real in-process services. */
function hubProxiesFor(
  mode: E2EContextOptions["hubProxies"],
  local: LocalHubProxies,
): HubProxyClients | undefined {
  if (mode === "in-process") return undefined
  if (mode === undefined || mode === "local") return local.clients
  return mode
}

/**
 * Assemble the in-process worker E2E context. The worker graph is the production
 * `RUNNABLE_SESSION_NODES` plus the projector and functional boundary mocks; the
 * `Database` replacement is left to the production `writerDatabaseLayer` (built
 * from the supplied writer), so this exercises the real worker DB wiring.
 */
export function createE2EContext(options: E2EContextOptions): E2EContext {
  const { snapshot, modelConfig } = options
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  applyBaseSchema(connection)

  const writerOps: string[] = []
  const hub = createRecordingHub(connection, writerOps)
  const writerChannel = new MessageChannel()
  const disposeWriter = serveWriterPort(hub, writerChannel.port1)
  const writer = createWorkerWriterClient(writerChannel.port2)

  const local = createLocalHubProxies()
  const hubProxies = hubProxiesFor(options.hubProxies, local)

  // Functional boundary mocks: MCP/LSP are broker-owned in production; a worker
  // session with no MCP tools still reads their empty catalogs.
  const mcpLayer = Layer.mock(MCP.Service, {
    status: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
    instructions: () => Effect.succeed([]),
    tools: () => Effect.succeed({}),
    prompts: () => Effect.succeed({}),
    resources: () => Effect.succeed({}),
    resourceTemplates: () => Effect.succeed({}),
  })
  const lspLayer = Layer.mock(LSP.Service, {
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    documentSymbol: () => Effect.succeed([]),
  })

  const extraNodes = options.nodes ?? []
  const built = buildSessionWorkerLayer({
    nodes: LayerNode.group([RUNNABLE_SESSION_NODES, SessionProjector.node, ...extraNodes]),
    snapshot,
    env: { values: {} },
    clients: {
      writer,
      egress: options.egress ?? { emit: () => {}, emitDurable: () => {} },
      broker: { call: async () => null },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    },
    modelClient: llmModelClientLayer(modelConfig).pipe(
      Layer.provide(reducedLlmLayer(snapshot, modelConfig, hubProxies)),
    ),
    ...(hubProxies === undefined ? {} : { hubProxies }),
    replacements: [
      // `SessionProjector` runs IN the worker (o1-w3): `Session.updateMessage` /
      // `updatePart` publish durable events, and the projector materializes the
      // `session`/`message`/`part` rows the loop reads back. It is already in
      // `RUNNABLE_SESSION_NODES`; this explicit entry keeps it present when a
      // caller supplies a reduced `nodes` group.
      [SessionProjector.node, SessionProjector.node],
      [Provider.node, reducedModelProviderLayer(modelConfig)],
      [MCP.node, mcpLayer],
      [LSP.node, lspLayer],
      ...(options.replacements ?? []),
    ],
  })

  const instance: InstanceContext = {
    directory: snapshot.directory,
    worktree: snapshot.directory,
    project: { id: "e2e" } as never,
  }

  let disposed = false
  const dispose = async (): Promise<void> => {
    if (disposed) return
    disposed = true
    writer.dispose()
    disposeWriter()
    local.dispose()
    writerChannel.port1.close()
    writerChannel.port2.close()
  }

  return {
    connection,
    writer,
    layer: built.layer,
    instance,
    statusLog: local.statusLog,
    writerOps,
    rows: (table) =>
      connection.all(`SELECT * FROM ${table} ORDER BY rowid`, []) as ReadonlyArray<Record<string, unknown>>,
    dispose,
  }
}
