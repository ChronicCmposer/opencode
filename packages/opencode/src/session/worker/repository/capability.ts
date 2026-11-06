// Private write capability.

import type { Connection } from "@opencode-ai/core/database/connection"

/** The minimal raw surface a repository capability needs; never exposed publicly. */
export type RepositoryConnection = Pick<Connection, "all" | "run" | "exec" | "values">

export interface WriteCapability {
  readonly connection: RepositoryConnection
  /** The one session this worker owns; absent for degraded/unbound handles. */
  readonly sessionID?: string
  /** SQL function the live bound views call to resolve the session fence. */
  readonly scopeFunction: string
  /** Allow the authorizer to attribute the current statement to the repository. */
  /** Max statements per outermost repository transaction; defaults to 500. */
  readonly statementBudget?: number
  readonly withMain: <T>(operation: () => T) => T
  /**
   * True while the caller owns an external (non-repository) transaction. The
   * repository joins that transaction instead of retrying on its own, because
   * replaying a unit inside a transaction the caller will roll back would
   * duplicate a partial write.
   */
  readonly inExternalTransaction?: () => boolean
}

export function createWriteCapability(input: WriteCapability): WriteCapability {
  return Object.freeze({
    connection: input.connection,
    ...(input.sessionID === undefined ? {} : { sessionID: input.sessionID }),
    scopeFunction: input.scopeFunction,
    ...(input.statementBudget === undefined ? {} : { statementBudget: input.statementBudget }),
    withMain: input.withMain,
    ...(input.inExternalTransaction === undefined ? {} : { inExternalTransaction: input.inExternalTransaction }),
  })
}

export * as RepositoryCapability from "./capability"
