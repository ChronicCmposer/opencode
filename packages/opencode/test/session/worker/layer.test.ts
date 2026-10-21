// Phase 6.1 / 6.2 / 6.5 / 6.7 — reduced session layer tests (in-process).

import { Database as BunDatabase } from "bun:sqlite"
import { describe, expect, test } from "bun:test"
import { Effect, Exit, Layer, Schema, Stream } from "effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
import { eq, sql } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { EventV2 } from "@opencode-ai/core/event"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { Config } from "@/config/config"
import { Auth } from "@/auth"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { InstanceRef } from "@/effect/instance-ref"
import { createSessionScopeHost } from "@/session/worker/session-scope"
import { EffectDrizzleSqlite } from "../../../../effect-drizzle-sqlite/src/index"
import {
  ModelClient,
  RUNNABLE_SESSION_NODES,
  SESSION_NODES,
  SessionPipeline,
  WorkerEnvService,
  WorkerSnapshotService,
  boundaryReplacements,
  buildSessionWorkerLayer,
  databaseBoundaryLayer,
  snapshotAuthLayer,
  snapshotConfigLayer,
  snapshotModelsDevLayer,
  writerDatabaseLayer,
  type SessionLayerOptions,
  type WorkerSnapshot,
  type WriterClientHandle,
} from "@/session/worker/layer"
import {
  REQUIRED_SESSION_NODE_KEYS,
  SESSION_CLIENT_KEYS,
  collectNodeNames,
  validateSessionLayer,
} from "@/session/worker/node-table"
import { workerSnapshot } from "../../lib/snapshot"

/** Project defaults layered on the shared builder (config/auth are asserted). */
const snapshot = (overrides: Partial<WorkerSnapshot> = {}): WorkerSnapshot =>
  workerSnapshot({
    directory: "/tmp/project-a",
    config: { model: "test/test-model" },
    auth: { test: { type: "api", key: "secret-a" } },
    ...overrides,
  })

const clients = (): SessionLayerOptions["clients"] => ({
  writer: {
    query: async () => [],
    publish: async () => ({ seq: 7, committed: true }),
  },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const modelLayer = (text: string) =>
  Layer.succeed(ModelClient, {
    complete: () => Effect.succeed({ text, usage: { input: 3, output: 4 } }),
  })

const options = (overrides: Partial<SessionLayerOptions> = {}): SessionLayerOptions => ({
  snapshot: snapshot(),
  env: { values: { OPENCODE_TEST_KEY: "worker-local" } },
  clients: clients(),
  modelClient: modelLayer("hello from the worker"),
  ...overrides,
})

describe("buildSessionWorkerLayer (6.5)", () => {
  test("the full allowlist graph is exhaustively classified after boundary replacements", () => {
    const report = validateSessionLayer(SESSION_NODES, boundaryReplacements(snapshot()))
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    // Every reachable application node is now allowlisted, excluded+replaced, or
    // a boundary replacement: the fail-closed `unknown` bucket is empty.
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("the runnable graph contains every required session-node service", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    for (const key of REQUIRED_SESSION_NODE_KEYS) expect(names.has(key)).toBe(true)
  })

  test("the runnable graph is fully classified with boundary replacements", () => {
    const report = validateSessionLayer(RUNNABLE_SESSION_NODES, boundaryReplacements(snapshot(), clients().writer))
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("the runnable graph reaches the real SessionPrompt/SessionProcessor loop", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    expect(names.has("@opencode/SessionPrompt")).toBe(true)
    expect(names.has("@opencode/SessionProcessor")).toBe(true)
    expect(names.has("@opencode/LLM")).toBe(true)
    expect(names.has("@opencode/ToolRegistry")).toBe(true)
  })

  test("the runnable graph carries the in-worker SessionProjector", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    expect(names.has("session-projector")).toBe(true)
    // The boundary replacements must not blank it out.
    const replaced = new Set(boundaryReplacements(snapshot(), clients().writer).map(([node]) => node.name))
    expect(replaced.has("session-projector")).toBe(false)
  })

  test("the runnable graph fails closed on an unknown reachable node", () => {
    const orphan = LayerNode.make({
      name: "@opencode/Unclassified",
      layer: Layer.empty as Layer.Layer<never>,
      deps: [] as const,
    })
    const report = validateSessionLayer(
      LayerNode.group([RUNNABLE_SESSION_NODES, orphan]),
      boundaryReplacements(snapshot()),
    )
    expect(report.unknown).toContain("@opencode/Unclassified")
    expect(report.ok).toBe(false)
  })

  test("the runnable layer replaces Database but runs SessionProjector in-worker", () => {
    const built = buildSessionWorkerLayer(options())
    const replaced = new Set(built.replacements.map(([node]) => node.name))
    expect(replaced.has("@opencode/v2/storage/Database")).toBe(true)
    // The projector is now a real worker node: replacing it with `Layer.empty`
    // would leave durable events with no materialized session/message/part rows.
    expect(replaced.has("session-projector")).toBe(false)
    expect(collectNodeNames(built.root).has("session-projector")).toBe(true)
    // The effective graph (replacements applied) is fully classified: the
    // Database boundary is replaced, the projector runs for real.
    const report = validateSessionLayer(built.root, built.replacements)
    expect(report.excluded).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("the runnable layer contains all four client nodes", () => {
    const built = buildSessionWorkerLayer(options())
    const names = collectNodeNames(built.root)
    for (const key of SESSION_CLIENT_KEYS) expect(names.has(key)).toBe(true)
    expect(built.root.dependencies.length).toBeGreaterThan(0)
  })

  test("without replacements the allowlist graph is rejected", () => {
    const report = validateSessionLayer(SESSION_NODES, [])
    expect(report.ok).toBe(false)
    expect(report.excluded).toContain("@opencode/v2/storage/Database")
  })
})

describe("per-worker snapshot isolation (6.2)", () => {
  test("two layers resolve different snapshots from the same process", async () => {
    const a = buildSessionWorkerLayer(options({ snapshot: snapshot({ epoch: 1, directory: "/a" }) }))
    const b = buildSessionWorkerLayer(
      options({
        snapshot: snapshot({ epoch: 2, directory: "/b", auth: { test: { type: "api", key: "secret-b" } } }),
      }),
    )

    const read = (built: ReturnType<typeof buildSessionWorkerLayer>) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const snap = yield* WorkerSnapshotService
          const env = yield* WorkerEnvService
          return { epoch: snap.epoch, directory: snap.directory, env: env.values }
        }).pipe(Effect.provide(built.layer)),
      )

    const [ra, rb] = await Promise.all([read(a), read(b)])
    expect(ra).toEqual({ epoch: 1, directory: "/a", env: { OPENCODE_TEST_KEY: "worker-local" } })
    expect(rb).toEqual({ epoch: 2, directory: "/b", env: { OPENCODE_TEST_KEY: "worker-local" } })
  })

  test("the env overlay never mutates process.env", async () => {
    const key = "OPENCODE_TEST_WORKER_OVERLAY"
    delete process.env[key]
    const built = buildSessionWorkerLayer(options({ env: { values: { [key]: "only-in-worker" } } }))
    await Effect.runPromise(
      Effect.gen(function* () {
        const env = yield* WorkerEnvService
        expect(env.values[key]).toBe("only-in-worker")
      }).pipe(Effect.provide(built.layer)),
    )
    expect(process.env[key]).toBeUndefined()
  })
})

describe("session pipeline (6.1)", () => {
  test("runs the model seam, emits egress and publishes durably", async () => {
    const emitted: string[] = []
    const published: string[] = []
    const built = buildSessionWorkerLayer(
      options({
        modelClient: modelLayer("pipeline text"),
        clients: {
          writer: {
            query: async () => [],
            publish: async (event) => {
              published.push(event.type)
              return { seq: 11, committed: true }
            },
          },
          egress: { emit: (event) => void emitted.push(event.type) },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
      }),
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const pipeline = yield* SessionPipeline
        return yield* pipeline.run({ sessionID: "s1", messages: [{ role: "user", content: "hi" }] })
      }).pipe(Effect.provide(built.layer)),
    )

    expect(result.text).toBe("pipeline text")
    expect(result.publishedSeq).toBe(11)
    expect(result.emitted).toBe(1)
    expect(emitted).toEqual(["session.text"])
    expect(published).toEqual(["message.assistant"])
  })

  test("a non-committed publish is not emitted and reports emitted 0", async () => {
    const emitted: string[] = []
    const built = buildSessionWorkerLayer(
      options({
        clients: {
          writer: { query: async () => [], publish: async () => ({ seq: null, committed: false }) },
          egress: { emit: (event) => void emitted.push(event.type) },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
      }),
    )

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const pipeline = yield* SessionPipeline
        return yield* pipeline.run({ sessionID: "s1", messages: [] })
      }).pipe(Effect.provide(built.layer)),
    )

    expect(result.publishedSeq).toBeNull()
    expect(result.emitted).toBe(0)
    expect(emitted).toEqual([])
  })

  test("a writer rejection surfaces in the typed error channel", async () => {
    const built = buildSessionWorkerLayer(
      options({
        clients: {
          writer: {
            query: async () => [],
            publish: async () => {
              throw new Error("writer down")
            },
          },
          egress: { emit: () => {} },
          broker: { call: async () => null },
          backgroundJob: { wait: async () => null, cancel: async () => {} },
        },
      }),
    )

    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const pipeline = yield* SessionPipeline
          return yield* pipeline.run({ sessionID: "s1", messages: [] })
        }).pipe(Effect.provide(built.layer)),
      ),
    ).rejects.toThrow("writer down")
  })
})

describe("boundary layers (6.x)", () => {
  test("snapshot Config serves reads but rejects writes loudly", async () => {
    const layer = snapshotConfigLayer(snapshot())
    const read = await Effect.runPromise(
      Effect.gen(function* () {
        const config = yield* Config.Service
        return yield* config.get()
      }).pipe(Effect.provide(layer)),
    )
    expect(read).toEqual({ model: "test/test-model" })

    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const config = yield* Config.Service
          return yield* config.update({})
        }).pipe(Effect.provide(layer)),
      ),
    ).rejects.toThrow(/Config.*update/)
  })

  test("snapshot Auth serves reads but rejects writes loudly", async () => {
    const layer = snapshotAuthLayer(snapshot())
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const auth = yield* Auth.Service
          return yield* auth.set("test", Auth.Api.make({ type: "api", key: "x" }))
        }).pipe(Effect.provide(layer)),
      ),
    ).rejects.toThrow(/Auth.*set/)
  })

  test("snapshot ModelsDev rejects refresh loudly", async () => {
    const layer = snapshotModelsDevLayer(snapshot())
    await expect(
      Effect.runPromise(
        Effect.gen(function* () {
          const modelsDev = yield* ModelsDev.Service
          return yield* modelsDev.refresh()
        }).pipe(Effect.provide(layer)),
      ),
    ).rejects.toThrow(/ModelsDev.*refresh/)
  })

  test("the Database boundary throws when the connection is touched", () => {
    expect(() =>
      Effect.runSync(
        Effect.gen(function* () {
          const database = yield* Database.Service
          return database.db
        }).pipe(Effect.provide(databaseBoundaryLayer())),
      ),
    ).toThrow(/Database/)
  })
})

describe("session scope integration (6.7)", () => {
  test("closes the session scope and runs finalizers after the pipeline", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()
    const built = buildSessionWorkerLayer(options())

    const result = await Effect.runPromise(
      host.run(
        "s1",
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s1")))
          const pipeline = yield* SessionPipeline
          return yield* pipeline.run({ sessionID: "s1", messages: [{ role: "user", content: "hi" }] })
        }).pipe(Effect.provide(built.layer)),
      ),
    )

    expect(result.text).toBe("hello from the worker")
    expect(finalized).toEqual(["s1"])
    expect(host.open).toBe(0)
    expect(host.closed).toBe(1)
  })
})

describe("node table report", () => {
  test("describes the table without throwing", () => {
    expect(typeof LayerNode.group).toBe("function")
  })

  test("the shared isNode predicate separates layer nodes from raw layers", () => {
    const node = LayerNode.make({ name: "test-node", layer: Layer.empty, deps: [] as const })
    expect(LayerNode.isNode(node)).toBe(true)
    expect(LayerNode.isNode(Layer.empty)).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// o1-w2 — the worker Database is a real writer-backed Drizzle database.
//
// The writer handle is backed by a real in-memory SQLite `SqlClient` so the
// worker layer is compared apples-to-apples with the flag-OFF driver.
// ---------------------------------------------------------------------------

const widget = sqliteTable("widget", {
  id: integer("id").primaryKey(),
  name: text("name").notNull(),
})

/** A real in-memory SQLite `SqlClient`, mirroring the flag-OFF Bun driver. */
function realSqliteClient(native: BunDatabase): Effect.Effect<SqlClient.SqlClient, never, Reactivity.Reactivity> {
  const rows = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.try({
      try: () => native.query(query).all(...(params as never[])) as Array<Record<string, unknown>>,
      catch: (cause) =>
        new SqlError({
          reason: classifySqliteError(cause, { message: "reference sqlite failed", operation: "execute" }),
        }),
    })

  const values = (query: string, params: ReadonlyArray<unknown> = []) =>
    Effect.try({
      try: () => native.query(query).values(...(params as never[])) as Array<Array<unknown>>,
      catch: (cause) =>
        new SqlError({
          reason: classifySqliteError(cause, { message: "reference sqlite failed", operation: "execute" }),
        }),
    })

  const connection: Connection = {
    execute: (query, params, transformRows) =>
      transformRows ? Effect.map(rows(query, params), transformRows) : rows(query, params),
    executeRaw: (query, params) => rows(query, params),
    executeValues: (query, params) => values(query, params),
    executeUnprepared: (query, params, transformRows) =>
      transformRows ? Effect.map(rows(query, params), transformRows) : rows(query, params),
    executeStream: () => Stream.die("reference sqlite streaming is not implemented"),
  }

  return SqlClient.make({
    acquirer: Effect.succeed(connection),
    compiler: Statement.makeCompilerSqlite(),
    transactionAcquirer: Effect.succeed(connection),
    spanAttributes: [["db.system.name", "sqlite"]],
  })
}

/** A writer handle backed by a real SQLite client, so transaction control is real. */
function writerOver(reference: SqlClient.SqlClient): WriterClientHandle {
  let nextTxId = 1
  const runQuery = (method: string, sql: string, params?: readonly unknown[]) => {
    const statement = reference.unsafe(sql, params)
    return Effect.runPromise(method === "values" ? statement.values : statement.withoutTransform)
  }
  return {
    query: (method, sql, params) => runQuery(method, sql, params),
    publish: async () => ({ seq: null, committed: false }),
    begin: async () => {
      await Effect.runPromise(reference.unsafe("BEGIN IMMEDIATE").withoutTransform)
      return `tx-${nextTxId++}`
    },
    txQuery: (_handle, method, sql, params) => runQuery(method, sql, params),
    commit: async () => {
      await Effect.runPromise(reference.unsafe("COMMIT").withoutTransform)
    },
    rollback: async () => {
      await Effect.runPromise(reference.unsafe("ROLLBACK").withoutTransform)
    },
  }
}

/** Builds the Drizzle database exactly the way the worker layer does. */
const drizzleOver = (client: SqlClient.SqlClient): Effect.Effect<EffectDrizzleSqlite.EffectSQLiteDatabase> =>
  EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient.SqlClient, client))

const runReactive = <A, E>(effect: Effect.Effect<A, E, Reactivity.Reactivity>) =>
  Effect.runPromise(effect.pipe(Effect.provide(Reactivity.layer)))

const crud = (db: EffectDrizzleSqlite.EffectSQLiteDatabase) =>
  Effect.gen(function* () {
    yield* db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
    yield* db
      .insert(widget)
      .values([
        { id: 1, name: "alpha" },
        { id: 2, name: "beta" },
      ])
      .run()
    yield* db.update(widget).set({ name: "ALPHA" }).where(eq(widget.id, 1)).run()
    yield* db.delete(widget).where(eq(widget.id, 2)).run()
    return yield* db.select().from(widget).orderBy(widget.id).all()
  })

describe("writer-backed worker Database (o1-w2)", () => {
  test("CRUD round-trip matches the flag-OFF driver", async () => {
    const flagNative = new BunDatabase(":memory:")
    const workerNative = new BunDatabase(":memory:")
    try {
      const result = await runReactive(
        Effect.gen(function* () {
          const flagDb = yield* drizzleOver(yield* realSqliteClient(flagNative))
          const workerDb = yield* Database.Service.pipe(
            Effect.provide(writerDatabaseLayer(writerOver(yield* realSqliteClient(workerNative)))),
          )
          return {
            flag: yield* crud(flagDb),
            worker: yield* crud(workerDb.db),
          }
        }),
      )

      expect(result.worker).toEqual(result.flag)
      expect(result.worker).toEqual([{ id: 1, name: "ALPHA" }])
    } finally {
      flagNative.close()
      workerNative.close()
    }
  })

  test("db.transaction is atomic: a throw leaves no partial rows", async () => {
    const native = new BunDatabase(":memory:")
    try {
      const result = await runReactive(
        Effect.gen(function* () {
          const database = yield* Database.Service.pipe(
            Effect.provide(writerDatabaseLayer(writerOver(yield* realSqliteClient(native)))),
          )
          const db = database.db
          yield* db.run(sql`CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
          const exit = yield* db
            .transaction((tx) =>
              Effect.gen(function* () {
                yield* tx.insert(widget).values({ id: 2, name: "beta" }).run()
                yield* Effect.fail(new Error("boom"))
              }),
            )
            .pipe(Effect.exit)
          return { exit, rows: yield* db.select().from(widget).all() }
        }),
      )

      expect(Exit.isFailure(result.exit)).toBe(true)
      expect(result.rows).toEqual([])
    } finally {
      native.close()
    }
  })

  test("writerDatabaseLayer type-checks as a Database.Service layer with no cast", () => {
    const layer: Layer.Layer<Database.Service> = writerDatabaseLayer({
      query: async () => [],
      publish: async () => ({ seq: null, committed: false }),
      begin: async () => "tx-1",
      txQuery: async () => [],
      commit: async () => {},
      rollback: async () => {},
    })
    expect(layer).toBeDefined()
  })

  test("a missing writer transaction primitive fails loud, never silently", async () => {
    const error = await runReactive(
      Effect.gen(function* () {
        const database = yield* Database.Service.pipe(
          Effect.provide(
            writerDatabaseLayer({
              query: async () => [],
              publish: async () => ({ seq: null, committed: false }),
            }),
          ),
        )
        return yield* database.db
          .transaction((tx) => tx.insert(widget).values({ id: 1, name: "alpha" }).run())
          .pipe(Effect.flip)
      }),
    )

    expect(error).toBeInstanceOf(SqlError)
    expect(String((error as { reason: { cause: unknown } }).reason.cause)).toMatch(/WriterClient.*begin/)
  })
})

// ---------------------------------------------------------------------------
// o1-w2 — the runnable graph gives Session/EventV2 the writer-backed Database.
// ---------------------------------------------------------------------------

const WorkerPing = EventV2.define({
  type: "test.worker.ping",
  durable: { version: 1, aggregate: "id" },
  schema: { id: Schema.String },
})

describe("runnable graph persistence (o1-w2)", () => {
  test("Session.get and a durable EventV2.publish succeed through the worker Database", async () => {
    const native = new BunDatabase(":memory:")
    try {
      const reference = Effect.runSync(realSqliteClient(native).pipe(Effect.provide(Reactivity.layer)))
      // The hub owns migrations: seed the shared store the worker writes through.
      await runReactive(
        Effect.gen(function* () {
          const db = yield* drizzleOver(reference)
          yield* DatabaseMigration.apply(db)
          yield* db
            .insert(SessionTable)
            .values({
              id: SessionID.make("ses_worker"),
              project_id: ProjectV2.ID.make("prj_worker"),
              slug: "worker",
              directory: "/tmp/project-a",
              title: "Worker session",
              version: "1",
            })
            .run()
        }),
      )

      const built = buildSessionWorkerLayer({
        ...options(),
        clients: {
          ...clients(),
          writer: writerOver(reference),
        },
      })

      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const session = yield* Session.Service
          const info = yield* session.get(SessionID.make("ses_worker"))
          const events = yield* EventV2.Service
          const published = yield* events.publish(WorkerPing, { id: "agg_worker" })
          return { info, durable: published.durable }
        }).pipe(Effect.provide(built.layer)),
      )

      expect(result.info.id).toBe(SessionID.make("ses_worker"))
      expect(result.info.title).toBe("Worker session")
      expect(result.durable).toMatchObject({ aggregateID: "agg_worker", seq: 0, version: 1 })
    } finally {
      native.close()
    }
  })

  test("Session.create materializes the row through the DEFAULT graph's in-worker projector", async () => {
    const native = new BunDatabase(":memory:")
    try {
      const reference = Effect.runSync(realSqliteClient(native).pipe(Effect.provide(Reactivity.layer)))
      await runReactive(
        Effect.gen(function* () {
          const db = yield* drizzleOver(reference)
          yield* DatabaseMigration.apply(db)
        }),
      )

      // The DEFAULT production graph: no `nodes` override and no test-added
      // projector. `Session.create` only PUBLISHES `session.created`; the session
      // row exists only if the default graph's projector materialized it.
      const built = buildSessionWorkerLayer({
        ...options(),
        clients: { ...clients(), writer: writerOver(reference) },
      })

      const instance = {
        directory: "/tmp/project-a",
        worktree: "/tmp/project-a",
        project: { id: "prj_worker" } as never,
      }

      const created = await Effect.runPromise(
        Effect.gen(function* () {
          const session = yield* Session.Service
          const info = yield* session.create({ title: "projected", agent: "build" })
          // Reading the row back proves projection: without it `get` fails loud.
          return yield* session.get(info.id)
        }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, instance)),
      )

      expect(created.title).toBe("projected")
      // Exactly one durable row in the shared store.
      const stored = await Effect.runPromise(
        reference.unsafe("SELECT id FROM session").withoutTransform.pipe(Effect.provide(Reactivity.layer)),
      )
      expect(stored).toHaveLength(1)
    } finally {
      native.close()
    }
  })
})
