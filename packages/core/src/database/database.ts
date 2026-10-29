export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Context, Effect, Layer } from "effect"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration"
import { SchemaGate } from "./schema-gate"
import { resolveWalConfig, walPragmas } from "./pragmas"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

export interface DatabaseLayerOptions {
  /**
   * `false` only for a caller that already holds the cross-process gate (the
   * session hub's synchronous ensure). Every other construction takes the gate
   * inside this layer, before any migration statement runs.
   */
  readonly gate?: boolean
}

export function layer(filename: string, options: DatabaseLayerOptions = {}) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const db = yield* makeDatabase

      // One resolved config feeds both the pragma list applied here (journal size
      // limit + busy timeout) and the hub executor's periodic checkpoint cadence
      // (`checkpointIntervalMs`, consumed by `session/worker/db-executor.ts`);
      // neither setting is dropped between the two connections.
      for (const pragma of walPragmas(resolveWalConfig())) yield* db.run(pragma)
      yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")

      const migrate = () => DatabaseMigration.apply(db)
      if (options.gate === false) yield* migrate()
      else yield* SchemaGate.withSchemaGate(filename, migrate)

      return { db }
    }).pipe(Effect.orDie),
  )
}

export function layerFromPath(filename: string, options: DatabaseLayerOptions = {}) {
  return layer(filename, options).pipe(Layer.provide(sqliteLayer({ filename })))
}

export function path() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  )
    return join(Global.Path.data, "opencode.db")
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

export const node = makeGlobalNode({ service: Service, layer: layerFromPath(path()), deps: [] })
