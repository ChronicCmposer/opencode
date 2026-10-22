import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { Identifier } from "@opencode-ai/core/id/id"
import { InstanceState } from "@/effect/instance-state"
import type { JobClient } from "@/session/worker/job-client"
import { Cause, Effect, Layer, Scope } from "effect"

export {
  Service,
  type ExtendInput,
  type Info,
  type Interface,
  type StartInput,
  type Status,
  type WaitInput,
  type WaitResult,
} from "@opencode-ai/core/background-job"

/** Keeps the legacy service instance-scoped while sharing the core registry engine. */
const layer = Layer.effect(
  CoreBackgroundJob.Service,
  Effect.gen(function* () {
    const state = yield* InstanceState.make(() => CoreBackgroundJob.make)
    return CoreBackgroundJob.Service.of({
      list: () => InstanceState.useEffect(state, (jobs) => jobs.list()),
      get: (id) => InstanceState.useEffect(state, (jobs) => jobs.get(id)),
      start: (input) => InstanceState.useEffect(state, (jobs) => jobs.start(input)),
      extend: (input) => InstanceState.useEffect(state, (jobs) => jobs.extend(input)),
      wait: (input) => InstanceState.useEffect(state, (jobs) => jobs.wait(input)),
      waitForPromotion: (id) => InstanceState.useEffect(state, (jobs) => jobs.waitForPromotion(id)),
      promote: (id) => InstanceState.useEffect(state, (jobs) => jobs.promote(id)),
      cancel: (id) => InstanceState.useEffect(state, (jobs) => jobs.cancel(id)),
    })
  }),
)

/**
 * Worker-side `BackgroundJob` backed by the hub-hosted cross-worker registry
 * (plan decision line 54). The worker owns the run locally (`runTask()`), but
 * every lifecycle transition is reported to the hub registry through the thin
 * client, so a `background.wait` on another worker resolves and a parent crash
 * can cancel children. `task.ts` needs no edits: it keeps importing this
 * service.
 */
export function clientLayer(client: JobClient): Layer.Layer<CoreBackgroundJob.Service> {
  const report = (effect: () => Promise<unknown>): Effect.Effect<void> =>
    Effect.promise(() => effect().then(() => undefined))

  return Layer.effect(
    CoreBackgroundJob.Service,
    Effect.gen(function* () {
      const scope = yield* Scope.Scope

      // The hub registry joins a running id, but it cannot tell this worker
      // whether it created the job or joined it. A local ownership set closes
      // that gap: `start` runs the work at most once per id per worker.
      const running = new Set<string>()
      // `onPromote` is an Effect closure and cannot cross the job port, so keep
      // it worker-local and run it when this worker promotes the job.
      const onPromoteCallbacks = new Map<string, Effect.Effect<void>>()

      /** A failed report must not strand the job: log it, then terminate the entry. */
      const terminate = (id: string, cause: Cause.Cause<unknown>): Effect.Effect<void> =>
        Effect.logError("BackgroundJob.clientLayer: failed to report job result", { id, cause }).pipe(
          Effect.andThen(report(() => client.cancel(id)).pipe(Effect.catchCause(() => Effect.void))),
        )

      const run = (id: string, work: Effect.Effect<string, unknown>, ownsGuard: boolean): Effect.Effect<void> =>
        work.pipe(
          Effect.matchCauseEffect({
            onSuccess: (output) => report(() => client.complete(id, output)),
            onFailure: (cause) => report(() => client.fail(id, Cause.squash(cause))),
          }),
          Effect.catchCause((cause) => terminate(id, cause)),
          Effect.ensuring(
            Effect.sync(() => {
              onPromoteCallbacks.delete(id)
              if (ownsGuard) running.delete(id)
            }),
          ),
          Effect.forkIn(scope, { startImmediately: true }),
          Effect.asVoid,
        )

      return CoreBackgroundJob.Service.of({
        list: () => Effect.promise(() => client.list()),
        get: (id) => Effect.promise(() => client.get(id)),
        start: (input) =>
          Effect.gen(function* () {
            const id = input.id ?? Identifier.ascending("job")
            const existing = yield* Effect.promise(() => client.get(id))
            if (existing?.status === "running") return existing
            const info = yield* Effect.promise(() =>
              client.start({
                id,
                type: input.type,
                ...(input.title === undefined ? {} : { title: input.title }),
                ...(input.metadata === undefined ? {} : { metadata: input.metadata }),
                ...(input.parentID === undefined ? {} : { parentID: input.parentID }),
              }),
            )
            if (input.onPromote !== undefined) onPromoteCallbacks.set(id, input.onPromote)
            if (!running.has(id)) {
              running.add(id)
              yield* run(id, input.run, true)
            }
            return info
          }),
        extend: (input) =>
          Effect.gen(function* () {
            const reserved = yield* Effect.promise(() => client.extend(input.id))
            if (!reserved) return false
            yield* run(input.id, input.run, false)
            return true
          }),
        wait: (input) =>
          Effect.promise(() =>
            client.wait(input.id, input.timeout).then((result) => ({
              ...(result.info === undefined ? {} : { info: result.info }),
              timedOut: result.timedOut,
            })),
          ),
        waitForPromotion: (id) => Effect.promise(() => client.waitForPromotion(id)),
        promote: (id) =>
          Effect.gen(function* () {
            const info = yield* Effect.promise(() => client.promote(id))
            const onPromote = onPromoteCallbacks.get(id)
            if (info?.metadata?.background === true && onPromote !== undefined) {
              onPromoteCallbacks.delete(id)
              yield* onPromote.pipe(Effect.ignore)
            }
            return info
          }),
        cancel: (id) => Effect.promise(() => client.cancel(id).then(() => client.get(id))),
      })
    }),
  )
}

/** A `BackgroundJob` node backed by the cross-worker client (worker role). */
export function clientNode(client: JobClient): LayerNode.Node<CoreBackgroundJob.Service, never, undefined> {
  return LayerNode.make({ service: CoreBackgroundJob.Service, layer: clientLayer(client), deps: [] })
}

export const node = LayerNode.make({ service: CoreBackgroundJob.Service, layer, deps: [] })

export * as BackgroundJob from "./job"
