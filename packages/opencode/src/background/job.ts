import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { Identifier } from "@opencode-ai/core/id/id"
import { InstanceState } from "@/effect/instance-state"
import type { JobClient } from "@/session/worker/job-client"
import { Effect, Layer, Scope } from "effect"

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

      const run = (id: string, work: Effect.Effect<string, unknown>): Effect.Effect<void> =>
        work.pipe(
          Effect.matchEffect({
            onSuccess: (output) => report(() => client.complete(id, output)),
            onFailure: (error) => report(() => client.fail(id, error)),
          }),
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
              }),
            )
            yield* run(id, input.run)
            return info
          }),
        extend: (input) =>
          Effect.gen(function* () {
            const reserved = yield* Effect.promise(() => client.extend(input.id))
            if (!reserved) return false
            yield* run(input.id, input.run)
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
        promote: (id) => Effect.promise(() => client.promote(id)),
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
