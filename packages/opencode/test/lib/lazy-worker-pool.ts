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
} from "@/session/worker/execute"
import type { EffectBridge } from "@/effect/bridge"
import {
  createIdleIsolateCap,
  createWorkerCap,
  resolveIdleWorkerCapacity,
  type IdleBoundedExecutorWorkerPool,
  type IdleIsolateCap,
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
    cancelPort: createExecutorCancelPort({ cancel: (sessionID) => worker.cancel(sessionID), ...(bridge === undefined ? {} : { bridge }) }),
    terminate: async () => {
      state.terminated = true
      const materializing = state.materializing
      state.materializing = undefined
      const current = state.inner
      state.inner = undefined
      if (current !== undefined) {
        await current.terminate()
        return
      }
      if (materializing !== undefined) {
        await materializing.then((worker) => worker.terminate()).catch(() => undefined)
      }
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
