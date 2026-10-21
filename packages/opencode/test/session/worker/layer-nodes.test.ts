// Phase 6.3 / 6.5 — node-table validation tests.
//
// The table is the mechanical guard that keeps the session worker layer from
// instantiating hub/writer/broker-owned nodes. These tests pin the pure
// validators against synthetic graphs and the real `Session`/`Database` nodes.

import { describe, expect, test } from "bun:test"
import { Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { Session } from "@/session/session"
import {
  SESSION_CLIENT_KEYS,
  SESSION_EXCLUDED,
  assertSessionLayer,
  collectNodeNames,
  findNode,
  lookupNode,
  validateSessionLayer,
  WorkerNodeTable,
} from "@/session/worker/node-table"

const clientNode = (name: string) =>
  LayerNode.make({ name, layer: Layer.empty as Layer.Layer<never>, deps: [] as const })

const writerClient = clientNode(SESSION_CLIENT_KEYS[0]!)
const egressClient = clientNode(SESSION_CLIENT_KEYS[1]!)
const brokerClient = clientNode(SESSION_CLIENT_KEYS[2]!)
const backgroundJobClient = clientNode(SESSION_CLIENT_KEYS[3]!)

const clientGroup = () => LayerNode.group([writerClient, egressClient, brokerClient, backgroundJobClient])

describe("session node table", () => {
  test("every allowlisted node is classified worker-local or a proxy", () => {
    for (const item of WorkerNodeTable.SESSION_ALLOWLIST) {
      expect(["worker-local", "broker-proxy", "hub-proxy"]).toContain(item.boundary)
    }
  })

  test("the excluded set covers the task exclusions", () => {
    const labels = SESSION_EXCLUDED.map((item) => item.label)
    expect(labels).toEqual(
      expect.arrayContaining([
        "Database",
        "SessionProjector",
        "LSP",
        "MCP",
        "McpAuth",
        "Workspace",
        "Worktree",
        "Vcs",
        "Installation",
        "Npm",
        "ModelsDev",
      ]),
    )
  })

  test("lookupNode resolves by key and by label", () => {
    expect(lookupNode("@opencode/Config")?.label).toBe("Config")
    expect(lookupNode("Database")?.key).toBe("@opencode/v2/storage/Database")
    expect(lookupNode("does-not-exist")).toBeUndefined()
  })

  test("collectNodeNames walks the whole dependency graph", () => {
    const names = collectNodeNames(Session.node)
    expect(names.has("@opencode/Session")).toBe(true)
    expect(names.has("@opencode/v2/storage/Database")).toBe(true)
  })

  test("findNode locates a transitive node by name", () => {
    expect(findNode(Session.node, "@opencode/v2/storage/Database")?.name).toBe("@opencode/v2/storage/Database")
    expect(findNode(Session.node, "nope")).toBeUndefined()
  })
})

describe("validateSessionLayer", () => {
  test("flags an excluded node that is reachable without a replacement", () => {
    const report = validateSessionLayer(LayerNode.group([Session.node, clientGroup()]))
    expect(report.ok).toBe(false)
    expect(report.excluded).toContain("@opencode/v2/storage/Database")
  })

  test("passes when excluded nodes are replaced and clients are present", () => {
    const report = validateSessionLayer(LayerNode.group([Session.node, clientGroup()]), [
      [Database.node, Layer.succeed(Database.Service, { db: {} as never })],
    ])
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("flags missing client nodes", () => {
    const report = validateSessionLayer(LayerNode.group([Session.node]), [
      [Database.node, Layer.succeed(Database.Service, { db: {} as never })],
    ])
    expect(report.missingClients).toEqual([...SESSION_CLIENT_KEYS])
    expect(report.ok).toBe(false)
  })

  test("a replacement cannot smuggle an excluded node back in", () => {
    const mcpNode = clientNode("@opencode/MCP")
    const smuggler = LayerNode.make({
      name: Database.node.name,
      layer: Layer.empty as Layer.Layer<never>,
      deps: [mcpNode],
    })
    const report = validateSessionLayer(LayerNode.group([Database.node, clientGroup()]), [[Database.node, smuggler]])
    expect(report.excluded).toContain("@opencode/MCP")
    expect(report.ok).toBe(false)
  })
})

describe("assertSessionLayer", () => {
  test("throws a descriptive error on violation", () => {
    expect(() => assertSessionLayer(LayerNode.group([Session.node, clientGroup()]))).toThrow(
      /session layer violates the node table/,
    )
    try {
      assertSessionLayer(LayerNode.group([Session.node, clientGroup()]))
    } catch (error) {
      expect(error).toBeInstanceOf(WorkerNodeTable.SessionLayerViolationError)
      expect((error as Error).message).toContain("@opencode/v2/storage/Database")
    }
  })

  test("is silent for a compliant layer", () => {
    expect(() =>
      assertSessionLayer(LayerNode.group([Session.node, clientGroup()]), [
        [Database.node, Layer.succeed(Database.Service, { db: {} as never })],
      ]),
    ).not.toThrow()
  })
})
