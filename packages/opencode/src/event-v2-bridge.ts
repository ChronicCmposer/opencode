// Opencode publish boundary for core events. Attach routed instance location
// so direct EventV2 consumers can isolate directory/workspace streams.
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceRef, WorkspaceRef } from "@/effect/instance-ref"
import { GlobalBus } from "@/bus/global"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { Project } from "@opencode-ai/core/project"
import { AbsolutePath } from "@opencode-ai/core/schema"
import type { InstanceContext } from "@/project/instance-context"
import type { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { Context, Effect, Layer } from "effect"

export class Service extends Context.Service<Service, EventV2.Interface>()("@opencode/EventV2Bridge") {}

/**
 * Build the `Location.Ref` an event must carry to reach an instance `/event` SSE
 * stream. Shared by the publish boundary and the live-only republish seam so a
 * republished worker event is stamped with the same directory the stream filters on.
 */
export function instanceLocation(
  ctx: InstanceContext | undefined,
  workspaceID?: WorkspaceV2.ID,
): Location.Ref | undefined {
  if (ctx === undefined || ctx.project === undefined) return undefined
  return new Location.Info({
    directory: AbsolutePath.make(ctx.directory),
    ...(workspaceID ? { workspaceID } : {}),
    project: { id: Project.ID.make(ctx.project.id), directory: AbsolutePath.make(ctx.worktree) },
  })
}

/** An already-formed event to republish on the hub surface. */
export interface LiveEvent {
  readonly id?: string
  readonly type: string
  readonly data: unknown
  readonly location?: Location.Ref
}

/** The hub's live-only publish seam: emit an event without a durable commit. */
export interface LiveEventPublisher {
  /** Republish an already-formed event (a worker event crosses this way). */
  publish(event: LiveEvent): void
  /** Publish a NON-durable definition's payload live-only. Durable definitions are refused. */
  publishDefinition<D extends EventV2.Definition>(definition: D, data: EventV2.Data<D>): void
}

export interface LiveEventPublisherOptions {
  /** Resolve the location to stamp on an event that carries none. */
  readonly location?: () => Location.Ref | undefined
}

/**
 * Republish events live-only. Unlike `publish`, this bypasses the durable commit
 * path, so a worker event that already committed in its isolate is never
 * re-committed on the hub. Events reach the instance `/event` pubsub and — through
 * the bridge's own listener — `GlobalBus`.
 */
export function createLiveEventPublisher(
  events: EventV2.Interface,
  options: LiveEventPublisherOptions = {},
): LiveEventPublisher {
  const location = () => options.location?.()
  return {
    publish(event) {
      const resolved = event.location ?? location()
      Effect.runSync(
        events.publishLive({
          id: event.id === undefined ? EventV2.ID.create() : EventV2.ID.make(event.id),
          type: event.type,
          data: event.data,
          ...(resolved === undefined ? {} : { location: resolved }),
        }),
      )
    },
    publishDefinition(definition, data) {
      // A durable definition would commit to SQLite; that write belongs to the
      // writer, never to a live-only republish. Refuse it loudly.
      if (definition.durable !== undefined) {
        throw new Error(`live-only publish refuses durable event "${definition.type}"`)
      }
      const resolved = location()
      Effect.runSync(events.publish(definition, data, resolved === undefined ? undefined : { location: resolved }))
    },
  }
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2.Service

    const publish: EventV2.Interface["publish"] = (definition, data, options) =>
      Effect.gen(function* () {
        if (options?.location) return yield* events.publish(definition, data, options)
        const ctx = yield* InstanceRef
        if (!ctx) return yield* events.publish(definition, data, options)
        const workspaceID = yield* WorkspaceRef
        return yield* events.publish(definition, data, { ...options, location: instanceLocation(ctx, workspaceID) })
      })

    const unsubscribe = yield* events.listen((event) =>
      Effect.gen(function* () {
        const ctx = yield* InstanceRef
        const workspaceID = (yield* WorkspaceRef) ?? event.location?.workspaceID
        GlobalBus.emit("event", {
          directory: event.location?.directory ?? ctx?.directory,
          project: ctx?.project?.id,
          workspace: workspaceID,
          payload: { id: event.id, type: event.type, properties: event.data },
        })
        if (event.durable === undefined) return
        GlobalBus.emit("event", {
          directory: event.location?.directory ?? ctx?.directory,
          project: ctx?.project?.id,
          workspace: workspaceID,
          payload: {
            type: "sync",
            syncEvent: {
              id: event.id,
              type: EventV2.versionedType(event.type, event.durable.version),
              seq: event.durable.seq,
              aggregateID: event.durable.aggregateID,
              data: event.data,
            },
          },
        })
      }),
    )
    yield* Effect.addFinalizer(() => unsubscribe)

    return Service.of({ ...events, publish })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2.node] })

export * as EventV2Bridge from "./event-v2-bridge"
