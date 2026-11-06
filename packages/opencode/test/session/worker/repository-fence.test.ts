// Worker repository retry fences (D5).
//
// A repository write retries the whole unit only when it owns that unit. A
// nested savepoint and an external caller-owned transaction both mean someone
// else owns the rollback boundary, so the repository must join instead of
// replaying. Validation reads are part of the unit: a busy validation read is
// retried together with the write it guards, never the write alone.

import { describe, expect, test } from "bun:test"
import {
  createWriteCapability,
  type RepositoryConnection,
  type WriteCapability,
} from "@/session/worker/repository/capability"
import { makeRepository } from "@/session/worker/repository/write"

const SESSION_ID = "ses_fence"

// A failed ROLLBACK leaves the connection's transaction state unknown. Assert on
// the error's identity by name/tag so this suite compiles without depending on
// the source module that owns the class.
const TRANSACTION_STATE_FAILURE = {
  name: "RepositoryTransactionStateError",
  tag: "DB_REPOSITORY_TRANSACTION_STATE",
} as const

const busy = () => Object.assign(new Error("SQLITE_BUSY: database is locked"), { code: "SQLITE_BUSY" })

const todo = {
  session_id: SESSION_ID,
  content: "retry me",
  status: "pending",
  priority: "high",
  position: 1,
  time_created: 1,
  time_updated: 1,
}

type Kind = "run" | "exec" | "all"
type Outcome = "ok" | "busy" | "boom"

/** Minimal scripted connection so every busy/failure point is deterministic. */
class ScriptedRepository {
  readonly execs: string[] = []
  runs = 0
  reads = 0
  private readonly occurrences = new Map<string, number>()

  constructor(private readonly behaviour: (sql: string, occurrence: number, kind: Kind) => Outcome = () => "ok") {}

  private step = (sql: string, kind: Kind): void => {
    this.execs.push(sql)
    const key = `${kind}:${sql.trim().toLowerCase()}`
    const occurrence = (this.occurrences.get(key) ?? 0) + 1
    this.occurrences.set(key, occurrence)
    const result = this.behaviour(sql, occurrence, kind)
    if (result === "busy") throw busy()
    if (result === "boom") throw new Error("injected failure")
  }

  all = (sql: string): ReadonlyArray<unknown> => {
    this.reads += 1
    this.step(sql, "all")
    // Both the FK-ownership and unique-key probes select the bound scope.
    return [{ scope: SESSION_ID }]
  }

  values = (): ReadonlyArray<unknown> => []

  run = (sql: string): { changes: number; lastInsertRowid: number } => {
    this.runs += 1
    this.step(sql, "run")
    return { changes: 1, lastInsertRowid: this.runs }
  }

  exec = (sql: string): void => {
    this.step(sql, "exec")
  }

  count = (prefix: string): number =>
    this.execs.filter((sql) => sql.trim().toLowerCase().startsWith(prefix)).length
}

function capabilityOver(
  connection: RepositoryConnection,
  options: { readonly inExternalTransaction?: () => boolean } = {},
): WriteCapability {
  return createWriteCapability({
    connection,
    sessionID: SESSION_ID,
    scopeFunction: "worker_scope_session",
    withMain: (operation) => operation(),
    ...(options.inExternalTransaction === undefined ? {} : { inExternalTransaction: options.inExternalTransaction }),
  })
}

function repositoryOver(
  connection: RepositoryConnection,
  options: { readonly inExternalTransaction?: () => boolean } = {},
) {
  return makeRepository(capabilityOver(connection, options))
}

function captureFailure(operation: () => unknown): unknown {
  try {
    operation()
    return undefined
  } catch (error) {
    return error
  }
}

describe("repository retry fences", () => {
  test("an external transaction fence suppresses the inner repository retry", () => {
    const scripted = new ScriptedRepository((_sql, occurrence, kind) =>
      kind === "run" && occurrence === 1 ? "busy" : "ok",
    )
    const external = repositoryOver(scripted as unknown as RepositoryConnection, {
      inExternalTransaction: () => true,
    })

    expect(() => external.insert("todo", todo)).toThrow("database is locked")
    expect(scripted.runs).toBe(1)

    // The same busy failure is retried when the repository owns the unit.
    const owned = repositoryOver(scripted as unknown as RepositoryConnection)
    expect(owned.insert("todo", todo).changes).toBe(1)
    expect(scripted.runs).toBe(2)
  })

  test("a busy validation read in insertOne is retried with the write", () => {
    let reads = 0
    const scripted = new ScriptedRepository((_sql, _occurrence, kind) => {
      if (kind !== "all") return "ok"
      reads += 1
      return reads === 1 ? "busy" : "ok"
    })
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    const outcome = repository.insert("todo", todo)

    expect(outcome.changes).toBe(1)
    expect(scripted.runs).toBe(1)
    // First attempt failed on the FK probe; the retry re-ran both probes.
    expect(scripted.reads).toBe(3)
  })

  test("a busy validation read in update is retried with the write", () => {
    let reads = 0
    const scripted = new ScriptedRepository((_sql, _occurrence, kind) => {
      if (kind !== "all") return "ok"
      reads += 1
      return reads === 1 ? "busy" : "ok"
    })
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    const outcome = repository.update("todo", { content: "updated" }, { session_id: SESSION_ID, position: 1 })

    expect(outcome.changes).toBe(1)
    expect(scripted.runs).toBe(1)
    // First attempt failed on currentRows; the retry re-ran the read.
    expect(scripted.reads).toBe(2)
  })

  test("a failed rollback stops the retry with a terminal transaction-state error", () => {
    const scripted = new ScriptedRepository((sql, occurrence, kind) => {
      if (kind === "run" && occurrence === 1) return "busy"
      if (kind === "exec" && sql.startsWith("ROLLBACK TO")) return "boom"
      return "ok"
    })
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    const failure = captureFailure(() => repository.atomic(() => repository.insert("todo", todo)))

    expect(failure).toMatchObject(TRANSACTION_STATE_FAILURE)
    expect(scripted.count("savepoint")).toBe(1)
    expect(scripted.runs).toBe(1)
  })

  test("a poisoned connection rejects later writes without executing SQL", () => {
    const scripted = new ScriptedRepository((sql, occurrence, kind) => {
      if (kind === "run" && occurrence === 1) return "busy"
      if (kind === "exec" && sql.startsWith("ROLLBACK TO")) return "boom"
      return "ok"
    })
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    expect(captureFailure(() => repository.atomic(() => repository.insert("todo", todo)))).toMatchObject(
      TRANSACTION_STATE_FAILURE,
    )
    const runsAfterFailure = scripted.runs
    const statementsAfterFailure = scripted.execs.length

    const blocked = captureFailure(() => repository.insert("todo", todo))

    expect(blocked).toMatchObject(TRANSACTION_STATE_FAILURE)
    expect(scripted.runs).toBe(runsAfterFailure)
    expect(scripted.execs.length).toBe(statementsAfterFailure)
  })

  test("a fresh connection is not poisoned by another connection's failed rollback", () => {
    const failingScripted = new ScriptedRepository((sql, occurrence, kind) => {
      if (kind === "run" && occurrence === 1) return "busy"
      if (kind === "exec" && sql.startsWith("ROLLBACK TO")) return "boom"
      return "ok"
    })
    const failingRepository = repositoryOver(failingScripted as unknown as RepositoryConnection)
    expect(captureFailure(() => failingRepository.atomic(() => failingRepository.insert("todo", todo)))).toMatchObject(
      TRANSACTION_STATE_FAILURE,
    )

    const scripted = new ScriptedRepository()
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    const outcome = repository.insert("todo", todo)

    expect(outcome.changes).toBe(1)
    expect(scripted.runs).toBe(1)
  })

  test("a nested atomic joins the outer retry instead of retrying independently", () => {
    const scripted = new ScriptedRepository((_sql, occurrence, kind) =>
      kind === "run" && occurrence === 1 ? "busy" : "ok",
    )
    const repository = repositoryOver(scripted as unknown as RepositoryConnection)

    const outcome = repository.atomic(() => repository.atomic(() => repository.insert("todo", todo)))

    expect(outcome.changes).toBe(1)
    // Two savepoints per attempt, two attempts: the inner unit never retried itself.
    expect(scripted.count("savepoint")).toBe(4)
    expect(scripted.runs).toBe(2)
  })
})
