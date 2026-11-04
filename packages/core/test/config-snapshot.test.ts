// The worker's merged V1 snapshot is served through the core V2 Config seam as
// exactly one document. There is no file path, so relative references keep
// pointing at the worker location directory.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Config } from "@opencode-ai/core/config"
import { snapshotLayer } from "@opencode-ai/core/config/snapshot"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"

const mergedSnapshot = (): ConfigV1.Info => ({
  model: "anthropic/claude-sonnet-4",
  snapshot: true,
  references: {
    shared: { path: "../shared" },
    docs: "https://example.com/docs",
  },
})

const entriesOf = (info: ConfigV1.Info): Promise<Config.Entry[]> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const config = yield* Config.Service
      return yield* config.entries()
    }).pipe(Effect.provide(snapshotLayer(info))),
  )

describe("snapshotLayer", () => {
  test("serves the merged snapshot as one pathless document", async () => {
    const entries = await entriesOf(mergedSnapshot())

    expect(entries).toHaveLength(1)
    expect(entries[0]!.type).toBe("document")
    const document = entries[0] as Config.Document
    expect(document.path).toBeUndefined()
  })

  test("migrates V1 fields to their V2 names", async () => {
    const entries = await entriesOf(mergedSnapshot())
    const document = entries[0] as Config.Document

    expect(document.info.model).toBe("anthropic/claude-sonnet-4")
    expect(document.info.snapshots).toBe(true)
  })

  test("preserves relative references verbatim", async () => {
    const entries = await entriesOf(mergedSnapshot())
    const document = entries[0] as Config.Document
    const shared = document.info.references?.shared

    if (typeof shared !== "object") throw new Error("expected a structured reference")
    expect("path" in shared ? shared.path : undefined).toBe("../shared")
    expect(document.info.references?.docs).toBe("https://example.com/docs")
  })
})
