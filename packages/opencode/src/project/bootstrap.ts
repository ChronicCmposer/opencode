import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import { isMainThread } from "node:worker_threads"
import { Plugin } from "../plugin"
import { Format } from "../format"
import { LSP } from "@/lsp/lsp"
import { Snapshot } from "../snapshot"
import * as Project from "./project"
import * as Vcs from "./vcs"
import { InstanceState } from "@/effect/instance-state"
import { Effect, Layer } from "effect"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { processRole, setProcessRole } from "@/effect/process-role"
import { SessionWorkerNodes } from "@/session/worker/nodes"
import { Service } from "./bootstrap-service"

export { Service } from "./bootstrap-service"
export type { Interface } from "./bootstrap-service"

// The process main thread IS the hub: declare it explicitly, before any app
// graph module evaluates its flag-gated membership. The role is fail-closed
// (`effect/process-role.ts`), so without this declaration no isolate could ever
// contribute the hub node. A worker isolate skips this branch; its entry adopts
// the role from `workerData` instead.
if (isMainThread && processRole() === undefined) setProcessRole("hub")

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    // Yield each bootstrap dep at layer init so `run` itself has R = never.
    // InstanceStore imports only the lightweight tag from bootstrap-service.ts,
    // so it can depend on bootstrap without importing this implementation graph.
    const config = yield* Config.Service
    const format = yield* Format.Service
    const lsp = yield* LSP.Service
    const plugin = yield* Plugin.Service
    const project = yield* Project.Service
    const snapshot = yield* Snapshot.Service
    const vcs = yield* Vcs.Service
    const runtimeFlags = yield* RuntimeFlags.Service

    const run = Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      yield* Effect.logInfo("session workers flag", {
        experimentalSessionWorkers: runtimeFlags.experimentalSessionWorkers,
      })
      // The graph gate is frozen from `process.env`; the runtime flag follows the
      // ConfigProvider. A disagreement means the hub graph and the runtime
      // dispatch gate would take different paths, so diagnose it loudly instead
      // of silently running sessions in-process (or leaving the hub unused).
      const graphEnabled = SessionWorkerNodes.sessionWorkersEnabled()
      const flagMismatch = RuntimeFlags.sessionWorkersFlagMismatch({
        graphEnabled,
        runtimeEnabled: runtimeFlags.experimentalSessionWorkers,
      })
      if (flagMismatch !== undefined) {
        yield* Effect.logWarning(flagMismatch, {
          graphEnabled,
          runtimeEnabled: runtimeFlags.experimentalSessionWorkers,
        })
      }
      yield* Effect.logInfo("bootstrapping", { directory: ctx.directory })
      // everything depends on config so eager load it for nice traces
      yield* config.get()
      // Plugin can mutate config so it has to be initialized before anything else.
      yield* plugin.init()
      // Each service self-manages its own slow work via Effect.forkScoped against
      // its per-instance state scope. We just await materialization here.
      yield* Effect.forEach(
        [lsp, format, vcs, snapshot, project],
        (s) => s.init().pipe(Effect.catchCause((cause) => Effect.logWarning("init failed", { cause }))),
        { concurrency: "unbounded", discard: true },
      ).pipe(Effect.withSpan("InstanceBootstrap.init"))
    }).pipe(Effect.withSpan("InstanceBootstrap"))

    return Service.of({ run })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer: layer,
  deps: [Config.node, Format.node, LSP.node, Plugin.node, Project.node, RuntimeFlags.node, Snapshot.node, Vcs.node],
})

export * as InstanceBootstrap from "./bootstrap"
