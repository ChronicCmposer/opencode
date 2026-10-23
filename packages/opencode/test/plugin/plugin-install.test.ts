import { describe, expect, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect, Layer, Logger } from "effect"
import { HttpClient } from "effect/unstable/http"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Npm } from "@opencode-ai/core/npm"
import { verifyPluginDeps } from "@/config/plugin-install"
import { Config } from "@/config/config"
import { Env } from "@/env"
import { Auth } from "@/auth"
import { Account } from "@/account/account"
import { TestInstance, tmpdir } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { AccountTest } from "../fake/account"
import { AuthTest } from "../fake/auth"
import { NpmTest } from "../fake/npm"

const unexpectedHttp = HttpClient.make((request) =>
  Effect.die(`unexpected http request: ${request.method} ${request.url}`),
)

const configLayer = LayerNode.compile(LayerNode.group([Config.node, FSUtil.node, Env.node, CrossSpawnSpawner.node]), [
  [Auth.node, AuthTest.empty],
  [Account.node, AccountTest.empty],
  [Npm.node, NpmTest.noop],
  [httpClient, Layer.succeed(HttpClient.HttpClient, unexpectedHttp)],
])

const it = testEffect(configLayer)

function withProcessEnv<A, E, R>(key: string, value: string | undefined, effect: Effect.Effect<A, E, R>) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const original = process.env[key]
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
      return original
    }),
    () => effect,
    (original) =>
      Effect.sync(() => {
        if (original === undefined) delete process.env[key]
        else process.env[key] = original
      }),
  )
}

describe("plugin.install.verify", () => {
  test("reports not ok when node_modules is missing", async () => {
    await using tmp = await tmpdir()
    expect(verifyPluginDeps(tmp.path)).toEqual({ ok: false, reason: expect.any(String) })
  })

  test("reports ok when @opencode-ai/plugin is installed", async () => {
    await using tmp = await tmpdir()
    await fs.mkdir(path.join(tmp.path, "node_modules", "@opencode-ai", "plugin"), { recursive: true })
    expect(verifyPluginDeps(tmp.path)).toEqual({ ok: true })
  })

  it.instance("config install logs a loud, non-fatal error when a config dir has no deps", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const configDir = path.join(test.directory, "configdir")
      yield* FSUtil.use.ensureDir(configDir)

      const logs: Array<{ logLevel: string; message: unknown }> = []
      const logger = Logger.layer([
        Logger.make<unknown, void>((entry) => {
          logs.push({ logLevel: entry.logLevel, message: entry.message })
        }),
      ])

      yield* withProcessEnv(
        "OPENCODE_CONFIG_DIR",
        configDir,
        Config.Service.use((svc) => svc.get().pipe(Effect.andThen(svc.waitForDependencies()))),
      ).pipe(Effect.provide(logger))

      const errors = logs.filter((entry) => entry.logLevel === "Error")
      const hit = errors.filter((entry) => JSON.stringify(entry.message).includes(configDir))
      expect(hit.length).toBeGreaterThan(0)
      expect(hit.some((entry) => JSON.stringify(entry.message).includes("plugin"))).toBe(true)
    }),
  )
})
