// Phase 7.2 — remote-query proxy at the Connection layer (plan task 7.2).
//
// Session workers hold no SQLite connection: they hold a `RemoteConnection`, the
// async mirror of `WriterConnection`. Each call reduces to the `{method, sql,
// params}` frame the writer worker understands, so the execution path is identical
// to a local `Connection` — only the transport differs.

import type { WriterClient } from "./client"
import type { QueryMethod } from "./protocol"
import type { WriteOutcome } from "./connection"

export interface RemoteConnection {
  all(sql: string, params?: ReadonlyArray<unknown>): Promise<ReadonlyArray<Record<string, unknown>>>
  values(sql: string, params?: ReadonlyArray<unknown>): Promise<ReadonlyArray<ReadonlyArray<unknown>>>
  run(sql: string, params?: ReadonlyArray<unknown>): Promise<WriteOutcome>
  exec(sql: string): Promise<void>
}

export function createRemoteConnection(client: WriterClient): RemoteConnection {
  const call = <T>(method: QueryMethod, sql: string, params: ReadonlyArray<unknown>): Promise<T> =>
    client.query(method, sql, params) as Promise<T>

  return {
    all: (sql, params = []) => call<ReadonlyArray<Record<string, unknown>>>("all", sql, params),
    values: (sql, params = []) => call<ReadonlyArray<ReadonlyArray<unknown>>>("values", sql, params),
    run: (sql, params = []) => call<WriteOutcome>("run", sql, params),
    exec: (sql) => call<unknown>("exec", sql, []).then(() => undefined),
  }
}

export * as WriterSqlProxy from "./sql-proxy"
