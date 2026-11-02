import { appendFileSync } from "node:fs"
import path from "path"
import { Global } from "../global"
import { minimumLogLevel } from "./logging"

export type SyncTraceLevel = "debug" | "info" | "warn" | "error"

export interface SyncTraceInput {
  readonly prefix: string
  readonly level?: SyncTraceLevel
  readonly event: string
  readonly fields?: Record<string, unknown>
  readonly file?: string
}

const RANK: Record<SyncTraceLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 }

const threshold = (): number => RANK[minimumLogLevel().toLowerCase() as SyncTraceLevel] ?? RANK.info

const lastWriteAt = new Map<string, number>()

/** True when `key` has not been written within `intervalMs`; the first call is always true. */
export function traceOnce(key: string, intervalMs: number): boolean {
  const now = Date.now()
  const last = lastWriteAt.get(key)
  if (last !== undefined && now - last < intervalMs) return false
  if (lastWriteAt.size > 1_000) lastWriteAt.clear()
  lastWriteAt.set(key, now)
  return true
}

/** One synchronous line appended to `opencode.log`; fail-soft and level-gated. */
export function syncTrace(input: SyncTraceInput): void {
  const level = input.level ?? "info"
  if (RANK[level] < threshold()) return
  try {
    const fields = input.fields === undefined ? "" : ` ${JSON.stringify(input.fields)}`
    const file = input.file ?? path.join(Global.Path.log, "opencode.log")
    appendFileSync(file, `${input.prefix}${input.event}${fields}\n`)
  } catch {}
}

/** Core DB trace: `[db:<area>] <event> <json>`. */
export function dbTrace(
  area: string,
  level: SyncTraceLevel,
  event: string,
  fields?: Record<string, unknown>,
  file?: string,
): void {
  syncTrace({ prefix: `[db:${area}] `, level, event, fields, file })
}
