// TDD steps 90–91 — boundary composition + a single node source.
//
// The full session-worker graph and the reduced in-worker LLM sub-graph are two
// views of the same boundary set: they must share ONE composition of the
// snapshot-backed and hub-authority fragments, so a change to `Config`/`Auth`/
// `RuntimeFlags`/`Permission`/`Plugin` wiring can never land in one view only.
//
// The required session-node keys are also declared once: the layer's key→node
// registry must equal `REQUIRED_SESSION_NODE_KEYS` at module load, and hub-proxy
// authorities are installed by REPLACEMENT (not appended as duplicate root
// nodes), so a worker graph has exactly one node per service key.

import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { collectNodeNames, REQUIRED_SESSION_NODE_KEYS, validateSessionLayer } from "@/session/worker/node-table"
import {
  ModelClient,
  RUNNABLE_SESSION_NODES,
  SESSION_NODE_REGISTRY,
  boundaryReplacements,
  buildSessionWorkerLayer,
  llmBoundaryReplacements,
  type HubProxyClients,
  type SessionLayerOptions,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import { workerSnapshot } from "../../lib/snapshot"

const snapshot = () =>
  workerSnapshot({
    directory: "/tmp/layer-composition",
    config: { model: "test/test-model" },
    auth: { test: { type: "api", key: "secret" } },
  })

const model: WorkerModelConfig = {
  providerID: "test",
  modelID: "test-model",
  apiId: "test-api-id",
  baseUrl: "http://127.0.0.1:1/v1",
  apiKey: "sk-test",
}

const clients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const modelLayer = Layer.succeed(ModelClient, {
  complete: () => Effect.succeed({ text: "worker" }),
} as never)

/** The fragments both graphs must compose, keyed by service name. */
const SHARED_BOUNDARY_KEYS = [
  Config.node.name,
  Auth.node.name,
  RuntimeFlags.node.name,
  Permission.node.name,
  Plugin.node.name,
] as const

const hubProxies = (): HubProxyClients => {
  const client = { call: async () => null }
  return {
    sessionStatus: client as never,
    permission: client as never,
    question: client as never,
  }
}

/** Count node occurrences (groups excluded) so duplicate root nodes are visible. */
function countNodeNames(root: typeof RUNNABLE_SESSION_NODES, name: string): number {
  let count = 0
  const seen = new Set<object>()
  const visit = (node: typeof RUNNABLE_SESSION_NODES): void => {
    if (seen.has(node)) return
    seen.add(node)
    if (node.kind !== "group" && node.name === name) count += 1
    for (const dependency of node.dependencies) visit(dependency)
  }
  visit(root)
  return count
}

describe("90 shared boundary fragments", () => {
  test("full and LLM graphs install the same shared boundary keys", () => {
    const full = new Set(boundaryReplacements(snapshot()).map(([node]) => node.name))
    const llm = new Set(llmBoundaryReplacements(snapshot(), model).map(([node]) => node.name))
    for (const key of SHARED_BOUNDARY_KEYS) {
      expect(full.has(key), `full graph is missing shared boundary ${key}`).toBe(true)
      expect(llm.has(key), `LLM graph is missing shared boundary ${key}`).toBe(true)
    }
  })

  test("neither graph installs a replacement key twice", () => {
    for (const replacements of [boundaryReplacements(snapshot()), llmBoundaryReplacements(snapshot(), model)]) {
      const names = replacements.map(([node]) => node.name)
      expect(new Set(names).size).toBe(names.length)
    }
  })
})

describe("91 single node source", () => {
  test("the key→node registry equals the required session keys at module load", () => {
    expect([...SESSION_NODE_REGISTRY.keys()].sort()).toEqual([...REQUIRED_SESSION_NODE_KEYS].sort())
  })

  test("hub-proxy authorities are replacements, not duplicate root nodes", () => {
    const built = buildSessionWorkerLayer({
      snapshot: snapshot(),
      env: { values: {} },
      clients: clients(),
      modelClient: modelLayer,
      hubProxies: hubProxies(),
    })

    // The root is the declared runnable graph: no appended proxy group.
    expect(built.root).toBe(RUNNABLE_SESSION_NODES)
    // Exactly one node per hub-authority service key in the root.
    expect(countNodeNames(built.root, "@opencode/SessionStatus")).toBe(1)
    expect(countNodeNames(built.root, "@opencode/Permission")).toBe(1)
    expect(countNodeNames(built.root, "@opencode/Question")).toBe(1)
    expect(collectNodeNames(built.root).has("@opencode/SessionHub")).toBe(false)
    expect(validateSessionLayer(built.root, built.replacements).ok).toBe(true)
  })
})
