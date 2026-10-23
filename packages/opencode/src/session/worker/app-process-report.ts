// Fix 2 — worker-local `AppProcess` PID reporting.
//
// A session worker spawns real OS children: the shell tool (its own detached
// children) and the in-worker `Ripgrep` search (over `AppProcess`). Worker
// terminate/rebind runs NO Effect finalizers (`hub.ts`), and `CrossSpawnSpawner`
// spawns detached with its own PGID, so a mid-flight `rg` would be ORPHANED on
// teardown. The hub therefore mirrors every child PGID a worker owns and
// group-kills it on cancel/terminate; the shell tool already reports its own
// children through `PidReporter` (`tool/shell.ts`), and this module reports the
// children spawned through `AppProcess`.
//
// Seam choice (documented): `Ripgrep.Service` spawns via `AppProcess.spawn`, and
// `AppProcess.run`/`runStream` spawn through the `ChildProcessSpawner` captured
// by the real `AppProcess` layer. Wrapping `AppProcess.Interface` directly would
// miss `run`/`runStream` (a `RunResult` exposes no pid), while wrapping the
// shared `CrossSpawnSpawner.node` globally would double-report the shell tool's
// children with a different label — which `PidRegistry.register` rejects
// (`assertSameOwner`). Instead the worker replaces `AppProcess.node` with a layer
// built over a PRIVATE reporting `ChildProcessSpawner`: every `AppProcess`
// spawn/run/runStream reports, while the shell tool keeps its own untouched
// spawner and its own reports. No overlap, so no dedup conflict.

import { Effect, Layer } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner, make as makeSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { AppProcess } from "@opencode-ai/core/process"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { makeGlobalNode, type GlobalNode } from "@opencode-ai/core/effect/app-node"
import type { PidReporterInterface } from "./pid-registry"

/** Cap the reported label so a long command line never bloats a report. */
const MAX_LABEL = 120

/**
 * A stable, non-empty label for the hub's PGID mirror of an `AppProcess` child.
 * Mirrors the shell tool's `shellChildLabel` intent: the registry rejects an
 * empty label, so an empty description degrades to the generic `"process"`.
 */
export function appProcessLabel(command: ChildProcess.Command): string {
  const description = describeCommand(command)
  return description === "" ? "process" : description.slice(0, MAX_LABEL)
}

function describeCommand(command: ChildProcess.Command): string {
  if (command._tag === "StandardCommand") {
    return command.args.length === 0 ? command.command : `${command.command} ${command.args.join(" ")}`
  }
  return `${describeCommand(command.left)} | ${describeCommand(command.right)}`
}

/**
 * Report a spawned child's process group to the hub mirror. POSIX-only: Windows
 * has no process-group semantics (mirrors `reportDetachedShell`). A missing
 * reporter is a no-op (degraded boot); a present one never drops silently —
 * `PidReporterInterface.report` throws through the spawn on a closed port.
 */
export function reportSpawn(
  reporter: PidReporterInterface | undefined,
  command: ChildProcess.Command,
  pid: number,
  platform: NodeJS.Platform = process.platform,
): void {
  if (reporter === undefined || platform === "win32") return
  // The spawner detaches, so the child leads its own group: `pgid === pid`.
  reporter.report({ pid, pgid: pid, label: appProcessLabel(command) })
}

/**
 * The real `AppProcess` implementation layer. `core/process.ts` exports only the
 * node, so its layer is reached through the node's `implementation`; the guard
 * fails loud rather than deferring to an opaque `undefined` at build time.
 */
function realAppProcessLayer(): Layer.Layer<AppProcess.Service, never, ChildProcessSpawner> {
  const implementation = AppProcess.node.implementation
  if (implementation === undefined) throw new Error("AppProcess.node has no implementation layer")
  return implementation as Layer.Layer<AppProcess.Service, never, ChildProcessSpawner>
}

/**
 * A private reporting `ChildProcessSpawner`: it delegates to the real spawner and
 * reports each spawned handle's process group. Only `AppProcess` consumes this
 * instance, so the shell tool's own spawner (and its own reports) are untouched.
 */
export function reportingSpawnerLayer(
  reporter: PidReporterInterface | undefined,
): Layer.Layer<ChildProcessSpawner, never, ChildProcessSpawner> {
  return Layer.effect(
    ChildProcessSpawner,
    Effect.gen(function* () {
      const base = yield* ChildProcessSpawner
      return makeSpawner((command) =>
        base.spawn(command).pipe(
          Effect.tap((handle) => Effect.sync(() => reportSpawn(reporter, command, Number(handle.pid)))),
        ),
      )
    }),
  )
}

/**
 * The worker-local `AppProcess`: the real service built over the reporting
 * spawner, so every spawn — including `run`/`runStream`, which delegate to that
 * spawner — is mirrored. Requires the real `ChildProcessSpawner`, which the graph
 * provides via `CrossSpawnSpawner.node`.
 */
export function workerAppProcessLayer(
  reporter: PidReporterInterface | undefined,
): Layer.Layer<AppProcess.Service, never, ChildProcessSpawner> {
  return realAppProcessLayer().pipe(Layer.provide(reportingSpawnerLayer(reporter)))
}

/**
 * The `AppProcess.node` replacement the worker graph installs: a node (so the
 * builder provides its `CrossSpawnSpawner` dependency) that reports every child
 * PGID through the worker's control-port reporter.
 */
export function workerAppProcessNode(reporter: PidReporterInterface | undefined): GlobalNode<AppProcess.Service> {
  return makeGlobalNode({
    service: AppProcess.Service,
    layer: workerAppProcessLayer(reporter),
    deps: [CrossSpawnSpawner.node],
  })
}
