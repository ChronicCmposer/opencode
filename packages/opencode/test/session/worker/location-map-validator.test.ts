// Location-map validator tests.
//
// The worker location map is a narrower graph than the V1 session layer and has
// its own classification table. These tests pin the pure validator and the
// combined `assertWorkerLayer` gate against synthetic graphs.

import { describe, expect, test } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Location } from "@opencode-ai/core/location"
import { buildLocationServiceMapNode } from "@opencode-ai/core/location-services"
import {
  SESSION_CLIENT_KEYS,
  assertWorkerLayer,
  validateLocationMap,
} from "@/session/worker/node-table"

const serviceNode = (
  name: string,
  dependencies: readonly LayerNode.AnyNode[] = [],
): LayerNode.AnyNode => ({
  kind: "layer",
  name,
  dependencies,
})

const clientGroup = () => LayerNode.group(SESSION_CLIENT_KEYS.map((key) => serviceNode(key)))

describe("validateLocationMap", () => {
  test("flags watcher and filesystem-search nodes as forbidden", () => {
    const map = buildLocationServiceMapNode([
      serviceNode("@opencode/v2/FileWatcher"),
      serviceNode("@opencode/v2/FileSystem/Search"),
    ])

    const report = validateLocationMap(map)

    expect(report.forbidden).toEqual(["@opencode/v2/FileSystem/Search", "@opencode/v2/FileWatcher"])
    expect(report.ok).toBe(false)
  })

  test("flags an unclassified application node as unknown", () => {
    const map = buildLocationServiceMapNode([serviceNode("@opencode/v2/Unclassified")])

    const report = validateLocationMap(map)

    expect(report.unknown).toEqual(["@opencode/v2/Unclassified"])
    expect(report.ok).toBe(false)
  })

  test("inner replacements shadow outer replacements", () => {
    const database = serviceNode("@opencode/v2/storage/Database")
    const innerReplacement = serviceNode("@opencode/v2/storage/Database", [
      serviceNode("@opencode/v2/FileWatcher"),
    ])
    const outerReplacement = serviceNode("@opencode/v2/storage/Database")
    const map = buildLocationServiceMapNode([database], [[database, innerReplacement]])

    const report = validateLocationMap(map, [[database, outerReplacement]])

    expect(report.replaced).toContain("@opencode/v2/storage/Database")
    expect(report.forbidden).toContain("@opencode/v2/FileWatcher")
    expect(report.ok).toBe(false)
  })

  test("a small valid map has no forbidden or unknown entries", () => {
    const database = serviceNode("@opencode/v2/storage/Database")
    const databaseReplacement = serviceNode("@opencode/v2/storage/Database")
    const map = buildLocationServiceMapNode(
      [
        Location.node,
        database,
        serviceNode("@opencode/v2/Policy"),
        serviceNode("@opencode/Event"),
      ],
      [[database, databaseReplacement]],
    )

    const report = validateLocationMap(map)

    expect(report.allowed).toEqual(["@opencode/Event", "@opencode/v2/Policy"])
    expect(report.replaced).toEqual(["@opencode/Location", "@opencode/v2/storage/Database"])
    expect(report.forbidden).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })
})

describe("assertWorkerLayer", () => {
  test("rejects a non-branded LocationServiceMap replacement", () => {
    const unbranded = serviceNode("@opencode/example/LocationServiceMap")

    expect(() => assertWorkerLayer(clientGroup(), [[unbranded, unbranded]])).toThrow(
      "worker layer has no branded location-service map replacement",
    )
  })

  test("accepts a compliant V1 graph plus a branded valid map", () => {
    const map = buildLocationServiceMapNode([serviceNode("@opencode/v2/Policy")])

    expect(() => assertWorkerLayer(clientGroup(), [[map, map]])).not.toThrow()
  })
})
