// Shared harness for the worker-path grep/glob/skill tests.
//
// These tests must exercise the PRODUCTION worker wiring, not a hand-rolled
// approximation: the reduced session graph (`RUNNABLE_SESSION_NODES`) composed
// with the same `boundaryReplacements` the worker entry installs. After Fix 2
// that means `AppProcess` + `v2/Ripgrep` are worker-local and `RipgrepBinary` is
// snapshot-backed, so `Ripgrep.Service` is REAL inside the isolate and a search
// spawns the resolved binary (the test snapshot pins `/usr/bin/rg`, with `rg` on
// PATH as the fallback).
//
// Every helper is dependency-free: stub client ports (the worker never reaches a
// hub) and a model seam the tools never call.

import { Cause, Effect, Exit, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import {
  buildSessionWorkerLayer,
  ModelClient,
  RUNNABLE_SESSION_NODES,
  SessionLayerBoundaryError,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { InstanceRef } from "@/effect/instance-ref"
import type { InstanceContext } from "@/project/instance-context"
import { workerSnapshot } from "./snapshot"

/** Stub client handles: no ports, so the worker never talks to a hub. */
export const stubClients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

/** The model seam the reduced graph requires; the tool tests never call it. */
export const modelLayer = (text = "worker text") =>
  Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text }) })

/**
 * The worker node graph the tests resolve services from: the production runnable
 * graph plus `Ripgrep`/`FSUtil` hoisted to the top level. They are already
 * reachable as `ToolRegistry` dependencies, but `LayerNode.compile` only exposes
 * top-level nodes, so the tests must hoist them to read them directly.
 */
export const WORKER_TEST_NODES = LayerNode.group([RUNNABLE_SESSION_NODES, Ripgrep.node, FSUtil.node])

/** Build the full runnable worker graph with test defaults. */
export function buildWorkerLayer(overrides: Partial<SessionLayerOptions> = {}) {
  return buildSessionWorkerLayer({
    nodes: WORKER_TEST_NODES,
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: stubClients(),
    modelClient: modelLayer(),
    ...overrides,
  }).layer
}

/**
 * The ambient `InstanceRef` a worker-side tool reads (`InstanceState.context`).
 * `project` is cast because the tools only read `directory`/`worktree`; the
 * worker rebuilds a real `Project.Info` from the snapshot in production.
 */
export const workerInstance = (directory: string): InstanceContext => ({
  directory,
  worktree: directory,
  project: { id: "worker-test" } as never,
})

/** The shared worker layer every test file runs against. */
export const workerLayer = buildWorkerLayer()

/**
 * Run a worker effect against an explicit worker layer and fail loudly on a
 * defect, naming a {@link SessionLayerBoundaryError} specifically so the Fix 2
 * regression cannot be mistaken for an unrelated failure. Pass `directory` to
 * expose the ambient `InstanceRef` a tool reads.
 */
export async function runWithLayer<A, E, R>(
  layer: Layer.Layer<unknown, unknown>,
  effect: Effect.Effect<A, E, R>,
  directory?: string,
): Promise<A> {
  const scoped =
    directory === undefined
      ? effect
      : effect.pipe(Effect.provideService(InstanceRef, workerInstance(directory)))
  const exit = await Effect.runPromise(scoped.pipe(Effect.provide(layer), Effect.exit))
  if (Exit.isFailure(exit)) {
    const defect = Cause.squash(exit.cause)
    if (defect instanceof SessionLayerBoundaryError) {
      throw new Error(`worker boundary regression: ${defect.message}`)
    }
    throw defect
  }
  return exit.value
}

/** {@link runWithLayer} against the shared {@link workerLayer}. */
export const runWorker = <A, E, R>(effect: Effect.Effect<A, E, R>, directory?: string): Promise<A> =>
  runWithLayer(workerLayer, effect, directory)


