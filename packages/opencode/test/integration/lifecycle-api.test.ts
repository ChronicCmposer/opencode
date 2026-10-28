// Lifecycle API contracts.
//
// Three contracts are pinned here:
//   1. `assertNotBusy` fails closed: only a proven-dead worker is idle.
//   2. The watchdog env is default-on: malformed values become 60s (only an
//      explicit 0 disables) and warn.
//   3. The production hub's resolver path (not the services directly) cancels a
//      blocked permission/question ask and clears its pending list when the
//      session is cancelled.

import { describe, expect, test } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Effect, Layer, Context } from "effect"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Permission } from "@/permission"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { SessionID } from "@/session/schema"
import { ensureProductionSchema } from "@/session/db/schema"
import { SessionStatus } from "@/session/status"
import {
  DEFAULT_RUN_IDLE_TIMEOUT_MS,
  productionSessionHubLayer,
  resolveRunIdleTimeoutMs,
  runIdleTimeoutWarning,
  SessionHubService,
} from "@/session/worker/hub"
import { createHubProxyClient, HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import { SESSION_WORKER_IDLE_TIMEOUT_ENV } from "@/session/worker/pool"
import { requireInstance, withTmpdirInstance } from "../fixture/fixture"
import { sid } from "../lib/async"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { createFakeExecutorPool } from "../lib/fake-worker-pool"

const it = testEffect(Layer.empty)

// ---------------------------------------------------------------------------
// Watchdog env: default-on, malformed -> 60s, only explicit 0 disables.
// ---------------------------------------------------------------------------

describe("watchdog env resolution", () => {
  test("malformed falls back to 60s and warns; only explicit 0 disables", () => {
    expect(resolveRunIdleTimeoutMs({ env: {} })).toBe(DEFAULT_RUN_IDLE_TIMEOUT_MS)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "0" } })).toBe(0)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "250" } })).toBe(250)
    for (const malformed of ["abc", "-1", "1.5", "NaN", "Infinity"]) {
      expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: malformed } })).toBe(
        DEFAULT_RUN_IDLE_TIMEOUT_MS,
      )
      expect(runIdleTimeoutWarning({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: malformed } })).toContain(
        `invalid ${SESSION_WORKER_IDLE_TIMEOUT_ENV}: ${malformed}`,
      )
    }
    expect(runIdleTimeoutWarning({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "0" } })).toBeUndefined()
    expect(runIdleTimeoutWarning({ env: {} })).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// OpenAPI/SDK: the abort outcome advertises the typed boot mode.
// ---------------------------------------------------------------------------

type AbortModeSchema = {
  readonly properties?: { readonly mode?: { readonly enum?: ReadonlyArray<string> } }
  readonly anyOf?: ReadonlyArray<AbortModeSchema>
}
type AbortOperation = {
  readonly post?: {
    readonly responses?: Record<string, { readonly content?: Record<string, { readonly schema?: AbortModeSchema }> }>
  }
}

const abortModes = async (): Promise<ReadonlyArray<string>> => {
  const document = (await Bun.file(new URL("../../../sdk/openapi.json", import.meta.url)).json()) as {
    paths: Record<string, AbortOperation>
  }
  const operation = document.paths["/session/{sessionID}/abort"]
  const schema = operation?.post?.responses?.["200"]?.content?.["application/json"]?.schema
  const variants = schema?.anyOf ?? (schema === undefined ? [] : [schema])
  return variants.flatMap((variant) => variant.properties?.mode?.enum ?? [])
}

test("the generated OpenAPI abort outcome includes the typed boot mode", async () => {
  const modes = await abortModes()
  expect(modes).toContain("boot")
  expect(modes).toEqual(expect.arrayContaining(["cooperative", "terminated", "completed"]))
})

// ---------------------------------------------------------------------------
// Resolver path: a blocked ask is cancelled through the production hub's
// `onCancelRequested` resolver, not by calling the service directly.
// ---------------------------------------------------------------------------

const permissionAsk = (sessionID: SessionID): PermissionV1.AskInput => ({
  sessionID,
  permission: "bash",
  patterns: ["*"],
  metadata: {},
  always: ["*"],
  ruleset: [],
})

const questionAsk = (sessionID: SessionID) => ({
  sessionID,
  questions: [
    {
      question: "Proceed?",
      header: "Proceed",
      options: [{ label: "Yes", description: "Continue" }],
    },
  ],
})

const authorityLayer = LayerNode.compile(
  LayerNode.group([Permission.node, Question.node, EventV2Bridge.node, CrossSpawnSpawner.node]),
)

const baseMocks = Layer.mergeAll(
  Layer.mock(Config.Service, { get: () => Effect.succeed({ model: "test/model" } as never) }),
  Layer.mock(Auth.Service, {}),
  Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}) }),
  Layer.mock(Provider.Service, {
    defaultModel: () => Effect.succeed({ providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("model") }),
  }),
  RuntimeFlags.layer({ experimentalSessionWorkers: true }),
  Layer.mock(SessionStatus.Service, {
    get: () => Effect.succeed({ type: "idle" } as SessionStatus.Info),
    list: () => Effect.succeed(new Map()),
    set: () => Effect.void,
  }),
)

const productionLayer = (databasePath: string, pool: ReturnType<typeof createFakeExecutorPool>) =>
  productionSessionHubLayer({
    count: 1,
    databasePath,
    poolFactory: () => pool,
  }).pipe(Layer.provideMerge(authorityLayer), Layer.provide(baseMocks))

describe("permission/question resolver cancel", () => {
  it.live("cancelling a session rejects its blocked asks through the resolver and clears them", () =>
    withTmpdirInstance()(
      Effect.gen(function* () {
        const instance = yield* requireInstance
        const sessionID = sid("ses_resolver_cancel")
        const pool = createFakeExecutorPool(1, { mode: "blocking", value: { sessionID } })
        const filename = `${instance.directory}/lifecycle-api.sqlite`
        // The hub's boot gate refuses a missing database: in production the app
        // Database layer has already created and migrated the file.
        ensureProductionSchema({ filename, liveWorkers: 0 })
        const layer = productionLayer(filename, pool)
        const channel = new MessageChannel()

        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(layer)
            const hub = Context.get(context, SessionHubService)
            const permission = Context.get(context, Permission.Service)
            const question = Context.get(context, Question.Service)
            // Keep the worker owned by an in-flight run, exactly as a worker
            // blocked on a permission ask is. Dispatch without awaiting so the
            // ask can be issued while the run is still parked.
            yield* Effect.forkScoped(
              Effect.promise(() => hub.ensureRunning(sessionID)).pipe(
                Effect.provide(context),
                Effect.provideService(InstanceRef, instance),
              ),
            )
            yield* pollWithTimeout(
              Effect.sync(() => (pool.spawned[0] === undefined ? undefined : true)),
              "dispatch never spawned a worker",
            )
            // The dispatcher owns the worker id (the fake pool assigns its own to
            // the handle); the hub's registry is keyed by the dispatcher's id.
            const workerID = 1
            expect(hub.sessionOfWorker(workerID)).toBe(sessionID)
            hub.attachHubProxyPort(workerID, channel.port1)
            const client = createHubProxyClient(channel.port2)
            yield* Effect.gen(function* () {
              const permissionRequest = client.call("permission.ask", [permissionAsk(sessionID)])
              const questionRequest = client.call("question.ask", [questionAsk(sessionID)])
              yield* pollWithTimeout(
                Effect.gen(function* () {
                  const [permissions, questions] = yield* Effect.all([permission.list(), question.list()])
                  return permissions.length === 1 && questions.length === 1 ? true : undefined
                }),
                "asks never became pending",
              )

              // The resolver fires at cancel start, before any grace window.
              yield* Effect.promise(() => hub.cancelWithReport(sessionID))

              const permissionError = yield* Effect.promise(() =>
                permissionRequest.then(
                  () => undefined,
                  (error: unknown) => error,
                ),
              )
              const questionError = yield* Effect.promise(() =>
                questionRequest.then(
                  () => undefined,
                  (error: unknown) => error,
                ),
              )
              expect(permissionError).toBeInstanceOf(HubProxyRemoteError)
              expect((permissionError as HubProxyRemoteError).tag).toBe("PermissionRejectedError")
              expect(questionError).toBeInstanceOf(HubProxyRemoteError)
              expect((questionError as HubProxyRemoteError).tag).toBe("QuestionRejectedError")
              expect(yield* permission.list()).toEqual([])
              expect(yield* question.list()).toEqual([])
            }).pipe(
              Effect.ensuring(Effect.sync(() => client.dispose())),
              Effect.ensuring(Effect.promise(() => hub.terminateAll())),
            )
          }),
        )
        channel.port1.close()
        channel.port2.close()
      }),
    ),
  )
})
