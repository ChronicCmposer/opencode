// Node-table validation tests.
//
// The table is the mechanical guard that keeps the session worker layer from
// instantiating hub/writer/broker-owned nodes. These tests pin the pure
// validators against synthetic graphs and the real `Session`/`Database` nodes.

import { describe, expect, test } from "bun:test"
import { Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import {
  REQUIRED_SESSION_NODE_KEYS,
  SESSION_CLIENT_KEYS,
  SESSION_EXCLUDED,
  assertSessionLayer,
  collectNodeNames,
  findNode,
  lookupNode,
  validateSessionLayer,
  WorkerNodeTable,
} from "@/session/worker/node-table"
import { boundaryReplacements } from "@/session/worker/layer"
import { workerSnapshot } from "../../lib/snapshot"

/**
 * The 7 residual hub/writer/broker/infra-owned nodes the reduced worker must
 * never own. They are reachable from the real `SessionPrompt` graph, so they
 * must be classified `excluded` (and replaced) rather than left `unknown`.
 *
 * `AppProcess` and `v2/Ripgrep` are deliberately NOT here: a Bun worker is a
 * separate isolate in the same OS process and already spawns real detached
 * children in-worker, so ripgrep is worker-local (see the allowlist tests).
 * `RipgrepBinary` stays excluded — its resolved binary path comes from the hub
 * snapshot rather than a worker-side download.
 */
const RESIDUAL_EXCLUDED_KEYS = [
  "@opencode/Storage",
  "@opencode/Git",
  "@opencode/BackgroundJob",
  "@opencode/Global",
  "@opencode/RipgrepBinary",
  "@opencode/McpBrowser",
  "@opencode/example/LocationServiceMap",
] as const

/** Worker-local ripgrep nodes: safe to instantiate inside a session worker. */
const WORKER_LOCAL_RIPGREP_KEYS = ["@opencode/AppProcess", "@opencode/v2/Ripgrep"] as const

const clientNode = (name: string) =>
  LayerNode.make({ name, layer: Layer.empty as Layer.Layer<never>, deps: [] as const })

const writerClient = clientNode(SESSION_CLIENT_KEYS[0]!)
const egressClient = clientNode(SESSION_CLIENT_KEYS[1]!)

const clientGroup = () => LayerNode.group([writerClient, egressClient])

describe("session node table", () => {
  test("every allowlisted node is classified worker-local or a proxy", () => {
    for (const item of WorkerNodeTable.SESSION_ALLOWLIST) {
      expect(["worker-local", "hub-proxy"]).toContain(item.boundary)
    }
  })

  test("the excluded set covers the task exclusions", () => {
    const labels = SESSION_EXCLUDED.map((item) => item.label)
    expect(labels).toEqual(
      expect.arrayContaining([
        "Database",
        "LSP",
        "MCP",
        "McpAuth",
        "Workspace",
        "Worktree",
        "Vcs",
        "Installation",
        "Npm",
        "ModelsDev",
        "Storage",
        "Git",
        "BackgroundJob",
        "Global",
        "RipgrepBinary",
        "McpBrowser",
        "LocationServiceMap",
      ]),
    )
  })

  test("SessionProjector is a worker-local allowlist node, not excluded", () => {
    // The projector materializes session/message/part rows from durable events.
    // It must run IN the worker (over the writer-backed Database), not be
    // replaced by an empty boundary, so it is allowlisted worker-local.
    expect(lookupNode("session-projector")?.boundary).toBe("worker-local")
    expect(SESSION_EXCLUDED.map((item) => item.key)).not.toContain("session-projector")
    expect(REQUIRED_SESSION_NODE_KEYS).toContain("session-projector")
  })

  test("the 7 residual hub/writer/broker/infra nodes are classified excluded", () => {
    for (const key of RESIDUAL_EXCLUDED_KEYS) {
      const entry = lookupNode(key)
      expect(entry?.boundary).toBe("excluded")
    }
  })

  test("AppProcess and v2/Ripgrep are allowlisted worker-local, never excluded", () => {
    // A Bun worker is a separate isolate in the same OS process and spawns real
    // detached children in-worker (`ToolRegistry.node` provides the real
    // `CrossSpawnSpawner`), so ripgrep — over `AppProcess` + `RipgrepBinary` — is
    // worker-local rather than a hub/broker fail-loud boundary.
    const excludedKeys = SESSION_EXCLUDED.map((item) => item.key)
    for (const key of WORKER_LOCAL_RIPGREP_KEYS) {
      expect(lookupNode(key)?.boundary).toBe("worker-local")
      expect(excludedKeys).not.toContain(key)
    }
  })

  test("RipgrepBinary stays excluded with a snapshot-backed note", () => {
    const entry = lookupNode("@opencode/RipgrepBinary")
    expect(entry?.boundary).toBe("excluded")
    expect(entry?.note).toBe("snapshot-backed binary path")
  })

  test("lookupNode resolves by key and by label", () => {
    expect(lookupNode("@opencode/Config")?.label).toBe("Config")
    expect(lookupNode("Database")?.key).toBe("@opencode/v2/storage/Database")
    expect(lookupNode("does-not-exist")).toBeUndefined()
  })

  test("the client keys are the exported single-source constants", () => {
    expect(SESSION_CLIENT_KEYS).toEqual([
      WorkerNodeTable.WRITER_CLIENT_KEY,
      WorkerNodeTable.EVENT_EGRESS_CLIENT_KEY,
    ])
    // The background-job node has no hub counterpart anymore: the worker's job
    // registry is process-local, so it must never be a `hub-proxy` client.
    expect(SESSION_CLIENT_KEYS).not.toContain("@opencode/session-worker/BackgroundJobClient")
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
    const report = validateSessionLayer(LayerNode.group([Database.node, clientGroup()]), [
      [Database.node, Layer.succeed(Database.Service, { db: {} as never })],
    ])
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    expect(report.unknown).toEqual([])
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

  test("unknown application nodes are violations (fail closed)", () => {
    const orphan = clientNode("@opencode/Unclassified")
    const report = validateSessionLayer(LayerNode.group([clientGroup(), orphan]))
    expect(report.unknown).toContain("@opencode/Unclassified")
    expect(report.ok).toBe(false)
  })

  test("non-application infrastructure nodes are outside the table", () => {
    const infra = clientNode("effect/HttpClient")
    const report = validateSessionLayer(LayerNode.group([clientGroup(), infra]))
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
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
      assertSessionLayer(LayerNode.group([Database.node, clientGroup()]), [
        [Database.node, Layer.succeed(Database.Service, { db: {} as never })],
      ]),
    ).not.toThrow()
  })

  test("names the unknown application nodes in the violation", () => {
    try {
      assertSessionLayer(LayerNode.group([clientGroup(), clientNode("@opencode/Unclassified")]))
      throw new Error("expected assertSessionLayer to throw")
    } catch (error) {
      expect(error).toBeInstanceOf(WorkerNodeTable.SessionLayerViolationError)
      expect((error as Error).message).toContain("@opencode/Unclassified")
    }
  })
})

describe("required session node set", () => {
  test("every required session node is classified (never unknown)", () => {
    for (const key of REQUIRED_SESSION_NODE_KEYS) expect(lookupNode(key)).toBeDefined()
  })

  test("a graph using the required nodes validates", () => {
    const required = REQUIRED_SESSION_NODE_KEYS.map((key) => clientNode(key))
    const report = validateSessionLayer(LayerNode.group([...required, clientGroup()]))
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
  })

  test("the real session pipeline no longer reports any required node as unknown", () => {
    const report = validateSessionLayer(LayerNode.group([SessionPrompt.node, clientGroup()]))
    for (const key of REQUIRED_SESSION_NODE_KEYS) expect(report.unknown).not.toContain(key)
  })

  test("hub/writer/broker-owned reachable nodes are excluded, never unknown", () => {
    const report = validateSessionLayer(LayerNode.group([SessionPrompt.node, clientGroup()]))
    // The residual nodes are classified `excluded`: a worker that reaches them
    // without a replacement is a violation, reported as `excluded` (not unknown).
    expect(report.excluded).toContain("@opencode/Storage")
    expect(report.excluded).toContain("@opencode/Global")
    for (const key of RESIDUAL_EXCLUDED_KEYS) expect(report.unknown).not.toContain(key)
    expect(report.ok).toBe(false)
  })

  test("worker-local ripgrep is never reported excluded by validateSessionLayer", () => {
    // Even in the bare graph (no replacements) the worker-local ripgrep nodes are
    // allowlisted, so they are neither `excluded` nor `unknown`.
    const report = validateSessionLayer(LayerNode.group([SessionPrompt.node, clientGroup()]))
    for (const key of WORKER_LOCAL_RIPGREP_KEYS) {
      expect(report.excluded).not.toContain(key)
      expect(report.unknown).not.toContain(key)
    }
  })

  test("boundary replacements make the real SessionPrompt graph fully classified", () => {
    const report = validateSessionLayer(
      LayerNode.group([SessionPrompt.node, clientGroup()]),
      boundaryReplacements(workerSnapshot()),
    )
    expect(report.excluded).toEqual([])
    expect(report.missingClients).toEqual([])
    expect(report.unknown).toEqual([])
    expect(report.ok).toBe(true)
    for (const key of WORKER_LOCAL_RIPGREP_KEYS) expect(report.excluded).not.toContain(key)
  })
})
