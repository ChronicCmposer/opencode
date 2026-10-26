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
import { tmpdir, type TmpDirOptions } from "../fixture/fixture"
import { workerSnapshot } from "./snapshot"

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
export interface E2EContext extends AsyncDisposable {
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
  /** Idempotent teardown; alias of {@link dispose} for resource ownership. */
  close(): Promise<void>
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
    close: dispose,
    dispose,
    [Symbol.asyncDispose]: dispose,
  }
}

// ---------------------------------------------------------------------------
// Scoped construction helpers
// ---------------------------------------------------------------------------

/** The handle returned by the shared `tmpdir` fixture. */
export type TempDirHandle = Awaited<ReturnType<typeof tmpdir>>

/**
 * Run `use` against a fresh temp directory, always removing it afterwards —
 * success, throw, or a failure inside the caller's own setup.
 */
export async function withTempDir<T>(
  use: (tmp: TempDirHandle) => Promise<T> | T,
  options: TmpDirOptions<unknown> = {},
): Promise<T> {
  const tmp = await tmpdir(options)
  try {
    return await use(tmp)
  } finally {
    await tmp[Symbol.asyncDispose]()
  }
}

/** What a {@link withWorkerE2E} callback receives. */
export interface WorkerE2E {
  readonly context: E2EContext
  readonly directory: string
}

/** Options for {@link withWorkerE2E}; `snapshot` is derived from the directory. */
export interface WithWorkerE2EOptions extends Omit<E2EContextOptions, "snapshot"> {
  /** Use an existing directory instead of creating a temp one (the caller then owns it). */
  readonly directory?: string
  /** Options for the owned temp directory (ignored when `directory` is set). */
  readonly temp?: TmpDirOptions<unknown>
  /**
   * Construction seam, defaulting to {@link createE2EContext}. Tests override it
   * to prove a failed construction still releases the temp directory.
   */
  readonly createContext?: (options: E2EContextOptions) => E2EContext
}

/**
 * Build a worker E2E context over a temp directory and dispose BOTH on every
 * exit path. Construction itself is inside the `try`, so a context that throws
 * halfway still releases the directory it never got to hand to the caller.
 */
export async function withWorkerE2E<T>(
  options: WithWorkerE2EOptions,
  use: (e2e: WorkerE2E) => Promise<T> | T,
): Promise<T> {
  const ownsDirectory = options.directory === undefined
  const tmp = ownsDirectory ? await tmpdir(options.temp) : undefined
  const directory = options.directory ?? tmp!.path
  const create = options.createContext ?? createE2EContext
  let context: E2EContext | undefined
  try {
    context = create({
      ...options,
      snapshot: workerSnapshot({ directory }),
    })
    return await use({ context, directory })
  } finally {
    await context?.close()
    if (tmp !== undefined) await tmp[Symbol.asyncDispose]()
  }
}
