// Runtime guard (ws4): a session worker must NOT build the server app graph.
//
// WHY THIS TEST EXISTS
// --------------------
// The session worker isolate exists to run one reduced session without ever
// standing up the HTTP server. The real `Plugin.node` (`src/plugin/index.ts`)
// defers its heavy implementation to `src/plugin/effect-layer.ts` via
// `Layer.unwrap(Effect.promise(() => import("./effect-layer")))`. That heavy
// layer, in `Plugin.state`, does
//
//     const { Server } = yield* Effect.promise(() => import("../server/server"))
//     ... fetch: async (...args) => Server.Default().app.fetch(...args)
//
// so a worker that reaches plugin state materializes the FULL server app graph
// (`Server.Default()` → `HttpApiApp.webHandler()` → `toWebHandler(routes)`).
// The worker session graph must instead replace `Plugin.node` with its
// worker-hosted plugin replacement, so `effect-layer` — and through it the
// server graph — is never imported. This test asserts that, at RUNTIME, in a
// real worker isolate.
//
// OBSERVATION
// -----------
// `httpapi/server.ts` exposes a test-only counter incremented exactly when the
// app graph is built (`webHandler` forced). A configured plugin whose `server()`
// uses `input.client` forces that build on the buggy path; the worker reports
// the counter value back. The main-thread control forces a fresh `webHandler()`
// and proves the counter observes a real build.
//
// SENSITIVITY (verified during authoring): replacing the worker-safe plugin
// replacement with the pre-ws2 real `Plugin.node` (heavy layer + deps) makes the
// worker report `1` — i.e. the test fails — so it genuinely detects the bug.
//
// DEPENDENCY: the worker-safe `Plugin.node` replacement is owned by ws2
// (worker-layer replacement); this test is RED without it and GREEN with it.

import { isMainThread } from "node:worker_threads"
import path from "node:path"
import { pathToFileURL } from "node:url"

interface CounterReport {
  readonly type: "counter"
  /** Server app-graph builds observed in the worker isolate. */
  readonly value: number
}

interface ErrorReport {
  readonly type: "error"
  readonly message: string
}

type WorkerReport = CounterReport | ErrorReport

/**
 * The probe plugin publishes its bounded client call on the isolate global so
 * the probe can await the actual async work instead of sleeping for a fixed
 * window. Absent on the worker-safe path, where the plugin never runs.
 */
interface WorkerProbeGlobal {
  __ws4PluginCall?: Promise<unknown>
}

/**
 * The worker half. Boots the REAL worker session graph (the same
 * `buildSessionWorkerLayer` the isolate entry builds), configures a plugin that
 * uses its client, runs a plugin-triggering hook, then reports the number of
 * server app-graph builds observed in this isolate.
 */
async function runWorkerProbe(): Promise<CounterReport> {
  const { Effect, Layer } = await import("effect")
  const fs = await import("node:fs")
  const os = await import("node:os")
  const { buildSessionWorkerLayer, ModelClient } = await import("@/session/worker/layer")
  const { Plugin } = await import("@/plugin")
  const { InstanceRef } = await import("@/effect/instance-ref")
  const { workerSnapshot } = await import("../../lib/snapshot")
  const { serverAppGraphBuildCount } = await import("@/server/routes/instance/httpapi/server")

  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-ws4-worker-"))
  try {
    const pluginFile = path.join(directory, "app-graph-probe-plugin.ts")

    // An isolated plugin that uses its client. `Plugin.state` hands plugins a
    // client whose `fetch` falls back to `Server.Default().app.fetch`, so this
    // call is exactly what forces the server app graph on the buggy path. It is
    // bounded by a timeout so a hung request can never wedge the worker, and the
    // promise is published so the probe can await the real work (never a sleep).
    fs.writeFileSync(
      pluginFile,
      [
        "export default async function plugin(input) {",
        "  const call = Promise.race([",
        "    input.client.config.get(),",
        "    new Promise((resolve) => setTimeout(resolve, 3000)),",
        "  ]).catch(() => {})",
        "  globalThis.__ws4PluginCall = call",
        "  await call",
        "  return {}",
        "}",
        "",
      ].join("\n"),
    )

    const snapshot = workerSnapshot({
      directory,
      config: {
        plugin_origins: [
          {
            spec: pathToFileURL(pluginFile).href,
            source: path.join(directory, "opencode.json"),
            scope: "local",
          },
        ],
      } as never,
    })

    // The exact clients the runtime tests use: the full runnable session graph
    // (which would carry the real `Plugin.node` if the worker replacement were
    // missing) with inert boundary clients.
    const clients = {
      writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
      egress: { emit: () => {} },
      backgroundJob: { wait: async () => null, cancel: async () => {} },
    } as never
    const modelClient = Layer.succeed(ModelClient, {
      complete: () => Effect.succeed({ text: "worker" }),
    } as never)

    const built = buildSessionWorkerLayer({
      snapshot,
      env: { values: {} },
      clients,
      modelClient,
    })

    const instance = { directory, worktree: directory, project: { id: "ws4" } as never }

    await Effect.runPromise(
      Effect.gen(function* () {
        const plugin = yield* Plugin.Service
        yield* plugin.trigger("chat.params" as never, {} as never, {} as never)
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, instance)),
    )

    // Await the plugin's bounded client call when the real plugin ran (buggy
    // path). On the worker-safe path the plugin never loads, so there is no
    // pending work and no fixed settle window to race.
    await (globalThis as WorkerProbeGlobal).__ws4PluginCall

    return { type: "counter", value: serverAppGraphBuildCount() }
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

async function runWorker(): Promise<void> {
  const { parentPort } = await import("node:worker_threads")
  try {
    parentPort?.postMessage(await runWorkerProbe())
  } catch (error) {
    parentPort?.postMessage({ type: "error", message: error instanceof Error ? error.message : String(error) })
  }
}

if (isMainThread === false) {
  await runWorker()
} else {
  const { describe, expect, test } = await import("bun:test")
  const { createWorkerHarness } = await import("../../lib/worker-harness")
  const WORKER_ENTRY = new URL(import.meta.url)

  describe("session worker does not build the server app graph (runtime)", () => {
    test("a plugin-triggering hook in a worker isolate builds the app graph zero times", async () => {
      const harness = createWorkerHarness({ timeoutMs: 30_000 })
      try {
        const worker = harness.spawn<WorkerReport, never>(WORKER_ENTRY)
        const report = await worker.waitFor((message) => message.type === "counter" || message.type === "error")
        if (report.type === "error") throw new Error(`worker probe failed: ${report.message}`)
        expect(report.value).toBe(0)
      } finally {
        await harness.dispose()
      }
    }, 40_000)

    test("main-thread control: forcing webHandler builds the app graph", async () => {
      // A query-suffixed import yields a FRESH module instance (its own lazy
      // `webHandler` and counter), so the control is independent of whatever the
      // worker test — or any other file — already memoized on the shared graph.
      // The specifier is non-literal so TypeScript does not try to resolve the
      // query string; the cast restores the module's types.
      const controlSpecifier: string = "@/server/routes/instance/httpapi/server?ws4-main-control"
      const fresh = (await import(controlSpecifier)) as typeof import("@/server/routes/instance/httpapi/server")
      fresh.resetServerAppGraphBuildCount()
      fresh.webHandler()
      expect(fresh.serverAppGraphBuildCount()).toBeGreaterThan(0)
    }, 20_000)
  })
}
