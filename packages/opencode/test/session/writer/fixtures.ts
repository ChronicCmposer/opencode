// Shared writer test fixtures (plan finding #184).
//
// The writer suites each rebuilt the same in-memory schema, fault-free
// environment overlay and event shape. Keeping them here gives the schema and
// the event contract a single definition.

import { createBunSqliteConnection, type WriterConnection } from "@/session/writer/connection"
import { createWriterCore, type WriterCore, type WriterCoreOptions } from "@/session/writer/core"
import type { HubWriter } from "@/session/writer/client"
import { FAULTS_ENV } from "@/testing/faults"

/** Schema statements for the writer test database (ordering matters). */
export const SCHEMA: ReadonlyArray<string> = [
  "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, owner_id TEXT)",
  "CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL, data TEXT NOT NULL)",
  "CREATE TABLE projector_log (aggregate_id TEXT, seq INTEGER NOT NULL, type TEXT)",
]

/** {@link SCHEMA} as one SQL string, for `connection.exec`-style setup. */
export const SCHEMA_SQL = SCHEMA.map((statement) => `${statement};`).join("\n")

/** A client environment whose writer isolate never sees an armed fault. */
export const healthyEnv: Record<string, string | undefined> = { [FAULTS_ENV]: undefined }

let eventCounter = 0

/** Build a writer event with a unique id. */
export const ev = (type: string, aggregateID: string, data: Record<string, unknown> = {}) => ({
  id: `evt-${++eventCounter}`,
  type,
  aggregateID,
  data,
})

// ---------------------------------------------------------------------------
// In-process writer core harness
// ---------------------------------------------------------------------------

export interface Harness {
  readonly core: WriterCore
  readonly rows: (sql: string, params?: ReadonlyArray<unknown>) => ReadonlyArray<Record<string, unknown>>
}

export interface HarnessOptions extends Omit<WriterCoreOptions, "connection"> {
  /**
   * Wrap the raw `bun:sqlite` connection before the core is built, so a test can
   * fail one statement (a COMMIT, a ROLLBACK) without a real busy/poisoned database.
   */
  readonly guard?: (connection: WriterConnection) => WriterConnection
}

/** A `WriterCore` over a fresh in-memory database seeded with {@link SCHEMA_SQL}. */
export function writerHarness(options: HarnessOptions = {}): Harness {
  const { guard, ...coreOptions } = options
  const connection = createBunSqliteConnection({ filename: ":memory:" })
  connection.exec(SCHEMA_SQL)
  const core = createWriterCore({
    connection: guard === undefined ? connection : guard(connection),
    sleep: async () => {},
    ...coreOptions,
  })
  return { core, rows: (sql, params = []) => connection.all(sql, params) }
}

/**
 * Settle `promise` within `ms` or fail with `message`, turning a hang (an
 * unreleased transaction pin, a starved read) into a bounded, descriptive failure.
 */
export async function settlesWithin<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms)
  })
  try {
    return await Promise.race([promise, deadline])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

// ---------------------------------------------------------------------------
// Hub writer fake (for the worker port server)
// ---------------------------------------------------------------------------

/** Transaction methods that fail loud when a test never exercises them. */
const unexpectedTransaction: Pick<HubWriter, "begin" | "txQuery" | "commit" | "rollback"> = {
  begin: async () => {
    throw new Error("begin was not expected in this test")
  },
  txQuery: async () => {
    throw new Error("txQuery was not expected in this test")
  },
  commit: async () => {
    throw new Error("commit was not expected in this test")
  },
  rollback: async () => {
    throw new Error("rollback was not expected in this test")
  },
}

/** A `HubWriter` fake: inert defaults with per-test overrides. */
export function fakeHub(overrides: Partial<HubWriter> = {}): HubWriter {
  return {
    query: async () => null,
    publish: async () => ({ seq: null, committed: false }),
    ...unexpectedTransaction,
    ...overrides,
  }
}
