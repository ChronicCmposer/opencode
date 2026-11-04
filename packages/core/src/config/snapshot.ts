import { Effect, Layer, Schema } from "effect"
import { Config } from "../config"
import { ConfigMigrateV1 } from "../v1/config/migrate"
import type { ConfigV1 } from "../v1/config/config"

/**
 * Serve the worker's merged V1 snapshot through the core V2 Config interface.
 * There is one document and no path: relative references resolve against the
 * worker location directory, which is the documented limitation of this seam.
 */
export function snapshotLayer(info: ConfigV1.Info): Layer.Layer<Config.Service> {
  const migrated = Schema.decodeUnknownSync(Config.Info)(ConfigMigrateV1.migrate(info))

  return Layer.succeed(
    Config.Service,
    Config.Service.of({
      entries: () =>
        Effect.succeed([
          new Config.Document({
            type: "document",
            info: migrated,
          }),
        ]),
    }),
  )
}
