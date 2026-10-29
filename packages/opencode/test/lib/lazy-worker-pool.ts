// Test-only lazy executor pool.
//
// The product only builds workers through the scoped production pool; the
// lazy `create(callback)` variants were test seams that never ran in a hub. They
// live here so tests keep an in-memory worker factory while production carries
// only the live pool. The materialization and idle-cap mechanics below are the
// same primitives the production pool composes.

import os from "node:os"
import {
  createExecutorCancelPort,
  createExecutorWorkerPool,
  type ExecutorRun,
  type ExecutorWorker,
  type ExecutorWorkerPool,
  type WorkerRunModel,
} from "@/session/worker/execute"
import { Effect } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import type { SessionID } from "@/session/schema"
import type { SessionWorkerInit } from "@/session/worker/session-worker"
import { workerLogMessage } from "@/session/worker/worker-logging"
import {
  createIdleIsolateCap,
  createWorkerCap,
  resolveIdleWorkerCapacity,
  type IdleBoundedExecutorWorkerPool,
  type IdleIsolateCap,
  type SessionBoundSpawner,
  type WorkerGeneration,
  type WorkerCap,
} from "@/session/worker/worker-pools"

interface MaterializingWorkerState {
  inner: ExecutorWorker | undefined
  terminated: boolean
  materializing: Promise<ExecutorWorker> | undefined
}

function createMaterializingExecutorWorker(
  id: number,
  materialize: (run: ExecutorRun, state: MaterializingWorkerState) => Promise<ExecutorWorker>,
  bridge: EffectBridge.Shape | undefined,
  cap?: WorkerCap,
): ExecutorWorker {
  const state: MaterializingWorkerState = { inner: undefined, terminated: false, materializing: undefined }
  let termination: Promise<void> | undefined
  const worker: ExecutorWorker = {
    id,
    get alive() {
      return !state.terminated && (state.inner === undefined || state.inner.alive)
    },
    execute: async (run) => {
      const release = await cap?.acquire(run.depth)
      try {
        return await (await materialize(run, state)).execute(run)
      } finally {
        release?.()
      }
    },
    cancel: (sessionID) => (state.inner === undefined ? Promise.resolve() : state.inner.cancel(sessionID)),
    cancelBoot: () => state.inner?.cancelBoot?.() ?? Promise.resolve(undefined),
    assertNotBusy: (sessionID) =>
      state.inner === undefined ? Promise.resolve(false) : state.inner.assertNotBusy(sessionID),
    resultFor: (sessionID) => state.inner?.resultFor(sessionID),
    hasActiveWork: (sessionID) => state.inner?.hasActiveWork?.(sessionID) ?? Promise.resolve(false),
    awaitEgress: (sessionID) => state.inner?.awaitEgress?.(sessionID) ?? Promise.resolve(),
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), ...(bridge === undefined ? {} : { bridge }) }),
    terminate: () => {
      if (termination !== undefined) return termination
      state.terminated = true
      const materializing = state.materializing
      state.materializing = undefined
      const current = state.inner
      state.inner = undefined
      termination = (async () => {
        if (current !== undefined) {
          await current.terminate()
          return
        }
        if (materializing !== undefined) {
          await materializing.then((worker) => worker.terminate()).catch(() => undefined)
        }
      })()
      return termination
    },
  }
  return worker
}

export type LazyExecutorWorkerFactory = () => ExecutorWorker | Promise<ExecutorWorker>

export interface LazyExecutorWorkerPoolOptions {
  readonly create: (id: number) => ExecutorWorker | Promise<ExecutorWorker>
  readonly bridge?: EffectBridge.Shape
  readonly maxRunning?: number
  readonly maxIdleWorkers?: number
}

export function createLazyExecutorWorker(
  id: number,
  create: LazyExecutorWorkerFactory,
  bridge: EffectBridge.Shape | undefined,
  cap?: WorkerCap,
): ExecutorWorker {
  return createMaterializingExecutorWorker(
    id,
    async (_run, state) => {
      if (state.terminated) throw new Error(`executor worker ${id} is terminated`)
      if (state.inner !== undefined) return state.inner
      state.materializing ??= Promise.resolve()
        .then(create)
        .then((worker) => {
          if (state.terminated) {
            void worker.terminate()
            throw new Error(`executor worker ${id} is terminated`)
          }
          state.inner = worker
          return worker
        })
      return state.materializing
    },
    bridge,
    cap,
  )
}

function withIdleIsolateCap(pool: ExecutorWorkerPool, cap: IdleIsolateCap): IdleBoundedExecutorWorkerPool {
  return {
    get workers() {
      return pool.workers
    },
    spawn: (id?: number) => pool.spawn(id),
    releaseSession: (sessionID, workerID) => {
      if (workerID !== undefined) cap.forget(workerID)
      pool.releaseSession?.(sessionID, workerID)
    },
    terminateAll: async () => {
      cap.clear()
      await pool.terminateAll()
    },
    byID: (id) => pool.byID(id),
    resultFor: (sessionID) => pool.resultFor(sessionID),
    get idleWorkers() {
      return cap.idle
    },
    get idleCount() {
      return cap.idle
    },
    get idleCapacity() {
      return cap.capacity
    },
    get idleEvictions() {
      return cap.evictions
    },
  }
}

export function createLazyExecutorWorkerPool(options: LazyExecutorWorkerPoolOptions): IdleBoundedExecutorWorkerPool {
  const cap = createWorkerCap(options.maxRunning ?? os.availableParallelism())
  let pool: ExecutorWorkerPool | undefined
  const idleCap = createIdleIsolateCap({
    capacity: options.maxIdleWorkers ?? resolveIdleWorkerCapacity(),
    ...(options.bridge === undefined ? {} : { bridge: options.bridge }),
    onEvict: (entry) => pool?.releaseSession?.(entry.sessionID, entry.worker.id),
  })
  pool = createExecutorWorkerPool({
    spawn: (id) => idleCap.track(createLazyExecutorWorker(id, () => options.create(id), options.bridge, cap)),
  })
  return withIdleIsolateCap(pool, idleCap)
}

/**
 * A worker whose isolate is bound to ONE dispatched session. A worker isolate
 * owns exactly one serialized model (`reducedModelProviderLayer` rejects any
 * other with `WorkerModelMismatchError`), so when the dispatcher hands this
 * worker a *different* session it terminates the old isolate and respawns under
 * the same id with that session's init. Reuse across turns of the same session
 * (the common case) keeps the isolate.
 */
export function createSessionBoundExecutorWorker(
  id: number,
  spawnFor: SessionBoundSpawner,
  bridge?: EffectBridge.Shape,
  /**
   * Runs when this isolate rebinds to a different session. The old session's
   * hub-side state is no longer reachable from this worker.
   */
  onSessionRebound?: (sessionID: SessionID) => unknown,
  cap?: WorkerCap,
  /**
   * The generation for a dispatch: a changed key (a different requested or
   * persisted model) retires the isolate even though the session id is the same,
   * so a legitimate mid-session model switch rebuilds the worker instead of
   * surfacing `WorkerModelMismatchError` on the next turn.
   */
  generationOf?: (sessionID: SessionID, model?: WorkerRunModel) => Promise<WorkerGeneration>,
  /**
   * Full retirement for a same-session generation change. Awaited after the old
   * isolate exits and before the successor spawns, so the successor never boots
   * against the retired generation's live lease or hub-side state.
   */
  onGenerationRetired?: (sessionID: SessionID, workerID?: number) => void | Promise<void>,
): ExecutorWorker {
  let boundSession: SessionID | undefined
  let boundGeneration: string | undefined
  return createMaterializingExecutorWorker(
    id,
    async (run, state) => {
      const sessionID = run.sessionID
      if (state.terminated) throw new Error(`executor worker ${id} is terminated`)
      const resolved = generationOf === undefined ? { generation: sessionID } : await generationOf(sessionID, run.model)
      const generation = resolved.generation
      if (state.inner !== undefined && boundSession === sessionID && boundGeneration === generation) {
        return state.inner
      }
      const previousSession = boundSession
      const previousGeneration = boundGeneration
      const previous = state.inner
      state.inner = undefined
      boundSession = sessionID
      boundGeneration = generation
      // Retire the previous isolate for ANY generation change, including the
      // same session moving to a new model. A rebind releases the old session's
      // state; a same-session generation change runs the hub's full retirement
      // path (the dispatcher never sees an in-place replacement).
      if (previousSession !== undefined && previous !== undefined) {
        const rebound = previousSession !== sessionID
        void bridge?.fork(
          Effect.logInfo(workerLogMessage(rebound ? "worker rebound" : "worker generation changed"), {
            workerID: id,
            ...(rebound
              ? { fromSession: previousSession, toSession: sessionID }
              : { fromGeneration: previousGeneration, toGeneration: generation }),
            sessionID,
          }),
        )
      }
      const retiredSession = previousSession
      const retiredIsRebind = retiredSession !== undefined && retiredSession !== sessionID
      state.materializing = Promise.resolve()
        // Retire the old isolate first so two models never share one worker.
        .then(() => previous?.terminate().catch(() => undefined))
        .then(async () => {
          if (retiredSession === undefined || previous === undefined) return
          try {
            if (retiredIsRebind) await onSessionRebound?.(retiredSession)
            else await onGenerationRetired?.(retiredSession, id)
          } catch (error) {
            // Cleanup must never fail the new run; the successor still boots.
            void bridge?.fork(
              Effect.logWarning(workerLogMessage("worker retirement callback failed"), {
                workerID: id,
                sessionID: retiredSession,
                error: error instanceof Error ? error.message : String(error),
              }),
            )
          }
        })
        .then(() => spawnFor(id, sessionID, resolved.init))
        .then((worker) => {
          if (state.terminated) {
            void worker.terminate()
            throw new Error(`executor worker ${id} is terminated`)
          }
          state.inner = worker
          return worker
        })
      return state.materializing
    },
    bridge,
    cap,
  )
}
