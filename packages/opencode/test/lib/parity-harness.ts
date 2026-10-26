// Writer-backed worker E2E context (split from the former `parity-harness`).
//
// `createE2EContext` assembles a fully wired, in-process "production worker"
// over a REAL writer: an in-memory SQLite connection behind the writer core,
// exposed to the worker over a `MessagePort` and driven by the worker's
// writer-backed Drizzle `Database`. It is the shared fixture for the
// session-worker / layer-session / prompt-sequential suites; the flag-ON ≡
// flag-OFF session parity harness lives in `./parity-session`.

import { Effect, Layer, ManagedRuntime } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import schema from "@opencode-ai/core/database/schema.gen"
import type { InstanceContext } from "@/project/instance-context"
import { SessionID } from "@/session/schema"
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
import { InstanceRef } from "@/effect/instance-ref"
import { instantRunner } from "./hub-proxy"
import { parityProject } from "./parity-project"

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

/**
 * The one capability `schema.up` uses from its Drizzle transaction: run a SQL
 * string. The generated migration is typed against the full Drizzle transaction,
 * so this isolates that boundary into a single, named assertion rather than
 * forcing an arbitrary value past the compiler.
 */
type MigrationTx = { run(sql: string): Effect.Effect<unknown> }

/**
 * `schema.up` restricted to the SQL-recording transaction it is called with here.
 * The assertion is confined to this boundary: the generated migration only ever
 * calls `tx.run(sql)`, so the real Drizzle transaction type is not reconstructible
 * and is bridged through `unknown` rather than forcing the value to `never`.
 */
const upBaseSchema = schema.up as unknown as (tx: MigrationTx) => Effect.Effect<void>

/** Apply the generated base schema to a raw connection (no migrations needed). */
export function applyBaseSchema(connection: { exec(sql: string): void }): void {
  const statements: string[] = []
  Effect.runSync(
    upBaseSchema({
      run: (sql) => {
        statements.push(sql)
        return Effect.void
      },
    }),
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
    buildHubProxyHandlers(authorities, instantRunner),
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

/** The tables the E2E context exposes to {@link E2EContext.rows}; anything else fails loud. */
const E2E_TABLES: ReadonlySet<string> = new Set(["session", "message", "part", "event"])

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
    project: parityProject("e2e"),
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
    rows: (table) => {
      if (!E2E_TABLES.has(table)) {
        throw new Error(`unknown E2E table "${table}" (expected one of ${[...E2E_TABLES].join(", ")})`)
      }
      return connection.all(`SELECT * FROM ${table} ORDER BY rowid`, [])
    },
    dispose,
  }
}

// --- Comparable single-side scenario runner ---------------------------------
//
// `runScenario` runs one scenario through the worker E2E context above in the
// requested flag state, so a test can execute the SAME deterministic scenario
// twice and compare the normalized outcomes:
//
//   * `flagOn: true`  — hub-owned authorities (`SessionStatus`/`Permission`/
//     `Question`) are reached over the hub proxy, as a per-session worker does;
//   * `flagOn: false` — those authorities stay in the worker graph, the
//     in-process/flag-OFF parity wiring.
//
// The runner is deliberately single-sided: it never starts two runtimes itself.
// The caller owns the comparison and decides whether the two sides are allowed
// to differ (see `parity-flag.test.ts`).
//
// LIMITATION: this runner does not set `OPENCODE_EXPERIMENTAL_SESSION_WORKERS`
// or launch the production hub/worker-pool graph. It pins the *observable
// authority wiring* a per-session worker sees (hub proxy vs in-process), which
// is where the flag changes behavior inside one worker. The full env-flag,
// provider-driven session graph parity (events + durable rows across two real
// modes) lives in `./parity-session.ts` (`runParitySession`); use that when a
// scenario needs the real flag-off session loop. Callers that need the env flag
// itself must toggle it around spawning a separate process, not here.

/** What a scenario sees: the flag state and the fully wired worker context. */
export interface ScenarioContext {
  readonly flagOn: boolean
  readonly context: E2EContext
}

/** One deterministic scenario both flag states can run. */
export interface Scenario<A, R = never> {
  readonly name: string
  readonly snapshot: WorkerSnapshot
  readonly modelConfig: WorkerModelConfig
  readonly nodes?: ReadonlyArray<LayerNode.AnyNode>
  readonly replacements?: LayerNode.Replacements
  /** Runs against the worker runtime; `R` is the services the runtime provides. */
  readonly run: (context: ScenarioContext) => Effect.Effect<A, never, R>
}

/** The captured outcome of running one scenario in one flag state. */
export interface ScenarioTrace<A> {
  readonly name: string
  readonly flagOn: boolean
  readonly value: A
}

/** Run `scenario` once in the requested flag state, disposing the whole graph. */
export async function runScenario<A, R = never>(
  flagOn: boolean,
  scenario: Scenario<A, R>,
): Promise<ScenarioTrace<A>> {
  const context = createE2EContext({
    snapshot: scenario.snapshot,
    modelConfig: scenario.modelConfig,
    hubProxies: flagOn ? "local" : "in-process",
    ...(scenario.nodes === undefined ? {} : { nodes: scenario.nodes }),
    ...(scenario.replacements === undefined ? {} : { replacements: scenario.replacements }),
  })
  const runtime = ManagedRuntime.make(context.layer)
  try {
    const value = await runtime.runPromise(
      scenario.run({ flagOn, context }).pipe(Effect.provideService(InstanceRef, context.instance)),
    )
    return { name: scenario.name, flagOn, value }
  } finally {
    await runtime.dispose()
    await context.dispose()
  }
}

/** Generated identifier families whose concrete values differ per run. */
const TRACE_ID_PATTERNS: ReadonlyArray<readonly [RegExp, string]> = [
  [/\bses_[A-Za-z0-9]+/g, "ses_<id>"],
  [/\bmsg_[A-Za-z0-9]+/g, "msg_<id>"],
  [/\bprt_[A-Za-z0-9]+/g, "prt_<id>"],
  [/\bcall_[A-Za-z0-9]+/g, "call_<id>"],
  [/\bjob_[A-Za-z0-9]+/g, "job_<id>"],
  [/\bevt_[A-Za-z0-9]+/g, "evt_<id>"],
]

/** Fields carrying wall-clock values that differ between the two sides. */
const VOLATILE_TIME_KEYS: ReadonlySet<string> = new Set([
  "time",
  "created",
  "updated",
  "started_at",
  "completed_at",
])

/**
 * Project a scenario outcome to its comparable shape: generated ids collapse to
 * one placeholder and wall-clock fields become `0`, recursively. Non-string
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
