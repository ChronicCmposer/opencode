import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import type { Hooks, PluginInput } from "@opencode-ai/plugin"
import { Npm } from "@opencode-ai/core/npm"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import fs from "node:fs"
import os from "node:os"
import path from "path"
import { pathToFileURL } from "url"
import { PluginLoader } from "../../src/plugin/loader"
import { Auth } from "../../src/auth"
import { Account } from "../../src/account/account"
import { RuntimeFlags } from "../../src/effect/runtime-flags"
import { Workspace } from "../../src/control-plane/workspace"
import { Plugin } from "../../src/plugin/index"
import { InstanceBootstrap } from "../../src/project/bootstrap"
import { InstanceStore } from "../../src/project/instance-store"
import { InstanceState } from "../../src/effect/instance-state"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"

const noopBootstrapLayer = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Plugin.node, Workspace.node, InstanceStore.node, Ripgrep.node]), [
    [Auth.node, AuthTest.empty],
    [Account.node, AccountTest.empty],
    [Npm.node, NpmTest.noop],
    [InstanceStore.bootstrapNode, noopBootstrapLayer],
    [RuntimeFlags.node, RuntimeFlags.layer({ disableDefaultPlugins: true, experimentalWorkspaces: true })],
  ]),
)

afterEach(async () => {
  await disposeAllInstances()
})

describe("plugin.workspace", () => {
  it.instance("plugin can install a workspace adapter", () =>
    Effect.gen(function* () {
      const dir = (yield* TestInstance).directory
      const type = `plug-${Math.random().toString(36).slice(2)}`
      const file = path.join(dir, "plugin.ts")
      const mark = path.join(dir, "created.json")
      const space = path.join(dir, "space")
      yield* Effect.promise(() =>
        Bun.write(
          file,
          [
            "export default async ({ experimental_workspace }) => {",
            `  experimental_workspace.register(${JSON.stringify(type)}, {`,
            '    name: "plug",',
            '    description: "plugin workspace adapter",',
            "    configure(input) {",
            `      return { ...input, name: "plug", branch: "plug/main", directory: ${JSON.stringify(space)} }`,
            "    },",
            "    async create(input) {",
            `      await Bun.write(${JSON.stringify(mark)}, JSON.stringify(input))`,
            "    },",
            "    async remove() {},",
            "    target(input) {",
            '      return { type: "local", directory: input.directory }',
            "    },",
            "  })",
            "  return {}",
            "}",
            "",
          ].join("\n"),
        ),
      )

      yield* Effect.promise(() =>
        Bun.write(
          path.join(dir, "opencode.json"),
          JSON.stringify(
            {
              $schema: "https://opencode.ai/config.json",
              plugin: [pathToFileURL(file).href],
            },
            null,
            2,
          ),
        ),
      )

      const plugin = yield* Plugin.Service
      yield* plugin.init()
      const workspace = yield* Workspace.Service
      const ctx = yield* InstanceState.context
      const info = yield* workspace.create({
        type,
        branch: null,
        extra: { key: "value" },
        projectID: ctx.project.id,
      })

      expect(info.type).toBe(type)
      expect(info.name).toBe("plug")
      expect(info.branch).toBe("plug/main")
      expect(info.directory).toBe(space)
      expect(info.extra).toEqual({ key: "value" })
      expect(JSON.parse(yield* Effect.promise(() => Bun.file(mark).text()))).toMatchObject({
        type,
        name: "plug",
        branch: "plug/main",
        directory: space,
        extra: { key: "value" },
      })
    }),
  )
})

/** The real workspace plugin shipped in the user's ws profile. */
const REAL_WORKSPACE_PLUGIN = "/var/lib/opencode/.config/opencode/profiles/ws/plugins/workspace-plugin.ts"

/** A valid plan the real workspace plugin's `plan_save` accepts. */
const PLAN = `---
status: in-progress
phase: 1
updated: 2026-11-03
---
## Goal
Verify the real workspace plugin hooks and tools end to end.
## Phase 1: Verify [IN PROGRESS]
- [ ] 1.1 Exercise plan_save and plan_read
`

/** Load the real workspace plugin through a temporary opencode config. */
async function loadRealWorkspacePlugin(directory: string, client: PluginInput["client"]): Promise<Hooks> {
  const configDir = fs.mkdtempSync(path.join(os.tmpdir(), "oc-real-ws-config-"))
  const configFile = path.join(configDir, "opencode.json")
  await Bun.write(
    configFile,
    JSON.stringify({ plugin: [pathToFileURL(REAL_WORKSPACE_PLUGIN).href] }, null, 2),
  )
  const loaded = await PluginLoader.loadExternal({
    items: [{ spec: pathToFileURL(REAL_WORKSPACE_PLUGIN).href, source: configFile, scope: "local" }],
    kind: "server",
  })
  try {
    const row = loaded[0]
    if (row === undefined) throw new Error("real workspace plugin did not load")
    const value = row.mod.default
    const server = typeof value === "function" ? value : (value as { server?: unknown } | undefined)?.server
    if (typeof server !== "function") throw new Error("real workspace plugin has no server entrypoint")
    return await (server as (input: PluginInput) => Promise<Hooks>)({
      client,
      project: { id: "real-workspace-plugin" },
      worktree: directory,
      directory,
      experimental_workspace: { register() {} },
      serverUrl: new URL("http://localhost:4096"),
    } as unknown as PluginInput)
  } finally {
    fs.rmSync(configDir, { recursive: true, force: true })
  }
}

describe("real workspace plugin", () => {
  test("loads from a temp config and exercises its hooks and tools", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-real-ws-project-"))
    const home = fs.mkdtempSync(path.join(os.tmpdir(), "oc-real-ws-home-"))
    const previousHome = process.env.HOME
    process.env.HOME = home
    const sessionID = "ses_real_workspace_plugin"
    const client = {
      session: {
        get: async ({ path: target }: { path: { id: string } }) => ({ data: { id: target.id } }),
        messages: async () => ({ data: [] }),
        prompt: async () => ({ data: {} }),
      },
      tui: { showToast: async () => {} },
      app: { log: async () => {} },
    } as unknown as PluginInput["client"]
    try {
      const hooks = await loadRealWorkspacePlugin(directory, client)

      expect(hooks.tool?.plan_save).toBeDefined()
      expect(hooks.tool?.plan_read).toBeDefined()
      expect(hooks["experimental.chat.system.transform"]).toBeDefined()
      expect(hooks["tool.execute.before"]).toBeDefined()
      expect(hooks["tool.execute.after"]).toBeDefined()
      expect(hooks["experimental.session.compacting"]).toBeDefined()

      const system = { system: [] as string[] }
      await hooks["experimental.chat.system.transform"]?.({ agent: "build" } as never, system)
      expect(system.system.join("\n")).toContain("<date-awareness>")
      expect(system.system.join("\n")).toContain("<code-review-protocol>")

      const toolCtx = {
        sessionID,
        messageID: "msg_real",
        callID: "call_real",
        agent: "build",
        directory,
        worktree: directory,
        abort: new AbortController().signal,
        metadata: () => {},
        ask: async () => {},
        extra: {},
        messages: [],
      }
      const planSave = hooks.tool?.plan_save
      const planRead = hooks.tool?.plan_read
      if (planSave === undefined || planRead === undefined) throw new Error("real workspace plugin has no plan tools")
      const saved = await planSave.execute({ content: PLAN }, toolCtx as never)
      expect(saved).toBe("Plan saved.")
      const read = await planRead.execute({ reason: "verify" }, toolCtx as never)
      expect(read).toBe(PLAN)

      const after = { title: "Plan saved.", output: "Plan saved.", metadata: {} }
      await hooks["tool.execute.after"]?.(
        { tool: "plan_save", sessionID, callID: "call_real", args: {} },
        after,
      )
      expect(after.output).toContain("Plan saved successfully")

      const compacting = { context: [] as string[] }
      await hooks["experimental.session.compacting"]?.({ sessionID }, compacting)
      expect(compacting.context.join("\n")).toContain("<workspace-context>")
      expect(compacting.context.join("\n")).toContain("Verify the real workspace plugin hooks")
    } finally {
      process.env.HOME = previousHome
      fs.rmSync(directory, { recursive: true, force: true })
      fs.rmSync(home, { recursive: true, force: true })
    }
  }, 30_000)
})
