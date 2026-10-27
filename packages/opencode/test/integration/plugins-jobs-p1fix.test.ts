// P1 fix — worker-owned foreground jobs are promotable, and the worker plugin
// bridge runs `chat.params`/`chat.headers` exactly once per request.
//
// The promotion half boots the real `bootstrapSessionWorker` over a transferred
// hub-proxy `MessagePort`, serves the production `buildHubProxyHandlers` map on
// the hub end, and drives the worker-side `createHubSyncedBackgroundJobs`
// wrapper against a real `BackgroundJob` registry. That is the whole path:
// worker bootstrap → control-port bridge → hub authority → hub registry.
//
// The hook half counts invocations: resolving a bridged worker's init must not
// invoke `chat.params`/`chat.headers` eagerly, while a per-request trigger runs
// the real plugin once.

import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { Agent } from "@/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Plugin } from "@/plugin"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { ToolRegistry } from "@/tool/registry"
import { TaskTool } from "@/tool/task"
import { Truncate } from "@/tool/truncate"
import { SessionSpawnProxyService, type SessionSpawnRequest, type SessionSpawnResult } from "@/session/worker/subagent"
import {
  buildHubProxyHandlers,
  createWorkerJobMirror,
  createLazyExecutorWorkerPool,
  productionSessionHubNode,
  SessionHubService,
  type ProductionPoolFactory,
} from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/hub-proxy-wire"
import { resolveWorkerPlugins, type WorkerModel } from "@/session/worker/init"
import {
  NonContainerReplacementError,
  createWorkerPluginHost,
  type ChatParamsOutput,
} from "@/session/worker/plugin-host"
import {
  bootstrapSessionWorker,
  createHubSyncedBackgroundJobs,
  hubProxyPluginTrigger,
  type SessionWorkerInit,
} from "@/session/worker/session-worker"
import { instantRunner, recordingHubAuthorities, serveHubProxyFor } from "../lib/hub-proxy"
import { productionServiceReplacements } from "../lib/production-deps"
import { pollWithTimeout } from "../lib/effect"
import { sid } from "../lib/async"
import { workerSnapshot } from "../lib/snapshot"
import { disposeAllInstances, withTmpdirInstance } from "../fixture/fixture"
import { seed, spawnResult, stubOps, toolContext } from "../session/worker/characterization/task-harness"

const model: WorkerModel = {
  providerID: "mock",
  modelID: "mock-model",
  apiId: "mock-api",
  npm: "@ai-sdk/openai-compatible",
  baseUrl: "http://127.0.0.1:0/v1",
  apiKey: "sk-mock",
  capabilities: { temperature: true, reasoning: false },
  limit: { context: 128_000, output: 4096 },
  options: { topK: 5 },
}

const init = (): SessionWorkerInit => ({
  snapshot: workerSnapshot(),
  env: { values: {} },
  role: "session",
  capabilities: { sessionWorkerPorts: true },
  model: {
    providerID: "mock",
    modelID: "mock-model",
    apiId: "mock-api",
    baseUrl: "http://127.0.0.1:0/v1",
    apiKey: "sk-mock",
    capabilities: { temperature: true, reasoning: false },
    limit: { context: 128_000, output: 4096 },
  },
})

/** A fake worker scope that exposes the transferred hub-proxy port. */
function workerScope() {
  const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
  return {
    frames,
    scope: {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    },
  }
}

/** Poll an Effect predicate; every wait is bounded so a broken mirror fails loud. */
function waitUntil(predicate: Effect.Effect<boolean | undefined>, message: string): Promise<boolean> {
  return Effect.runPromise(pollWithTimeout(predicate, message))
}

async function buildRegistry() {
  const scope = Effect.runSync(Scope.make())
  const layer = Layer.effect(BackgroundJob.Service, CoreBackgroundJob.makeWithRetention({ maxTerminalJobs: 8 }))
  const context = Effect.runSync(Layer.buildWithScope(layer, scope))
  const registry = Context.get(context, BackgroundJob.Service)
  const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(Effect.provide(effect, context))
  return {
    registry,
    run,
    close: () => Effect.runPromise(Scope.close(scope, Exit.void)),
  }
}

describe("foreground job promotion over the worker control port", () => {
  test("a worker job is listed, promoted, and settled through the hub registry", async () => {
    const registry = await buildRegistry()
    const mirror = createWorkerJobMirror()
    const base = recordingHubAuthorities()
    const handlers = buildHubProxyHandlers(
      { ...base.authorities, backgroundJob: mirror.authorityFor(registry.run) },
      instantRunner,
    )
    const { frames, scope } = workerScope()
    let jobs: BackgroundJob.Interface | undefined
    const bootstrap = bootstrapSessionWorker(init(), scope, {
      observeBackgroundJobs: (observed) => {
        jobs = observed
      },
    })
    const hubProxy = frames.find((frame) => (frame.data as { type?: string }).type === "hub-proxy")
    expect(hubProxy).toBeDefined()
    const port = (hubProxy!.data as { port?: unknown }).port
    expect(port).toBeInstanceOf(MessagePort)
    // Serve the production handler map on the worker's transferred port.
    const disposeServer = serveHubProxy(handlers, port as MessagePort)
    expect(jobs).toBeDefined()
    const release = Effect.runSync(Deferred.make<void>())
    const promoted = Effect.runSync(Deferred.make<void>())

    try {
      const job = await Effect.runPromise(
        jobs!.start({
          id: "ses_child",
          type: "task",
          title: "inspect bug",
          metadata: { parentSessionId: "ses_parent", notificationID: "ses_child" },
          parentID: "ses_parent",
          onPromote: Deferred.succeed(promoted, undefined).pipe(Effect.asVoid),
          run: Deferred.await(release).pipe(Effect.as("done")),
        }),
      )
      expect(job.status).toBe("running")

      await waitUntil(
        Effect.map(registry.registry.list(), (list) =>
          list.some((entry) => entry.id === "ses_child") ? true : undefined,
        ),
        "the worker job never reached the hub mirror",
      )

      const list = await registry.run(registry.registry.list())
      const mirrored = list.find((entry) => entry.id === "ses_child")
      expect(mirrored?.metadata?.parentSessionId).toBe("ses_parent")
      expect(mirrored?.metadata?.background).not.toBe(true)

      const promotedInfo = await registry.run(registry.registry.promote("ses_child"))
      expect(promotedInfo?.metadata?.background).toBe(true)
      const promotionObserved = await Effect.runPromise(Deferred.await(promoted).pipe(Effect.timeoutOption("1 second")))
      expect(promotionObserved._tag).toBe("Some")

      // The local worker registry observes the hub's promotion.
      await waitUntil(
        Effect.map(jobs!.get("ses_child"), (local) => (local?.metadata?.background === true ? true : undefined)),
        "the worker did not apply the hub promotion locally",
      )

      await Effect.runPromise(Deferred.succeed(release, undefined))
      await waitUntil(
        Effect.map(registry.registry.get("ses_child"), (settled) =>
          settled?.status === "completed" ? true : undefined,
        ),
        "the hub mirror never observed the worker settlement",
      )
    } finally {
      disposeServer()
      bootstrap.dispose()
      await registry.close()
    }
  })
})

describe("bridged request hooks run once", () => {
  test("init defers chat.params/chat.headers and the bridge runs each exactly once", async () => {
    const counts = { init: 0, request: 0 }
    const hooks = {
      "chat.params": () => {
        counts.init += 1
      },
      "chat.headers": () => {
        counts.init += 1
      },
    }
    const plugins = await Effect.runPromise(
      resolveWorkerPlugins({ model, sources: [{ name: "counting", hooks }], deferRequestHooks: true }),
    )
    expect(counts.init).toBe(0)
    const served = serveHubProxyFor(
      buildHubProxyHandlers(
        {
          ...recordingHubAuthorities().authorities,
          pluginTrigger: async (_name, _input, output) => {
            counts.request += 1
            return output
          },
        },
        instantRunner,
      ),
    )
    const host = createWorkerPluginHost(plugins, { hubHook: hubProxyPluginTrigger(served.client) })
    try {
      const params: ChatParamsOutput = { options: {} }
      await host.chatParams({ model: { id: "mock-model" } }, params)
      await host.chatHeaders({ model: { id: "mock-model" } }, { headers: {} })
      expect(counts.request).toBe(2)
      expect(counts.init).toBe(0)
    } finally {
      served.dispose()
    }
  })

  test("an eager (unbridged) init still resolves the boot diff once", async () => {
    let initCalls = 0
    await Effect.runPromise(
      resolveWorkerPlugins({
        model,
        sources: [
          {
            name: "counting",
            hooks: {
              "chat.params": () => {
                initCalls += 1
              },
            },
          },
        ],
      }),
    )
    expect(initCalls).toBe(1)
  })
})

describe("non-container hook replacements", () => {
  /** A hub bridge whose hook replaces one field with a non-container value. */
  const bridged = (replacement: Record<string, unknown>) => {
    const served = serveHubProxyFor(
      buildHubProxyHandlers(
        {
          ...recordingHubAuthorities().authorities,
          pluginTrigger: async () => replacement,
        },
        instantRunner,
      ),
    )
    return { host: createWorkerPluginHost([], { hubHook: hubProxyPluginTrigger(served.client) }), served }
  }

  test("a non-container args replacement is applied to the caller's value", async () => {
    const { host, served } = bridged({ args: "not-a-record" })
    try {
      const before = { args: { command: "ls" } as unknown }
      await host.toolBefore({ tool: "bash" }, before)
      expect(before.args).toBe("not-a-record")
    } finally {
      served.dispose()
    }
  })

  test("a non-container parts replacement fails loud instead of being dropped", async () => {
    const { host, served } = bridged({ message: { agent: "build" }, parts: "not-an-array" })
    try {
      const output = { message: { agent: "build" }, parts: [] as unknown[] }
      await expect(host.chatMessage({}, output)).rejects.toBeInstanceOf(NonContainerReplacementError)
    } finally {
      served.dispose()
    }
  })
})

// ---------------------------------------------------------------------------
// TaskTool ↔ worker mirror: the exact production seam, end to end.
//
// The task tool writes into the worker-local registry; the production wrapper
// mirrors each running job over the control port into the hub registry, where
// the promotion endpoint (Ctrl+B) sees it. Releasing the parent session prunes
// a dead worker's mirrored jobs.
// ---------------------------------------------------------------------------

let taskSpawn: (request: SessionSpawnRequest) => Effect.Effect<SessionSpawnResult> = (request) =>
  Effect.succeed(spawnResult(request, "done"))

const taskSpawnProxyLayer = Layer.succeed(SessionSpawnProxyService, {
  spawn: (request: SessionSpawnRequest) => taskSpawn(request),
})

/** The task tool's production graph, with its local job registry replaced. */
const taskLayerWith = (jobs: BackgroundJob.Interface) =>
  LayerNode.compile(
    LayerNode.group([
      Agent.node,
      BackgroundJob.node,
      EventV2Bridge.node,
      Config.node,
      CrossSpawnSpawner.node,
      Session.node,
      SessionProjector.node,
      SessionRunState.node,
      SessionStatus.node,
      Truncate.node,
      ToolRegistry.node,
      Database.node,
      RuntimeFlags.node,
      Ripgrep.node,
    ]),
    [
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalBackgroundSubagents: true })],
      [BackgroundJob.node, Layer.succeed(BackgroundJob.Service, jobs)],
    ],
  ).pipe(Layer.provideMerge(taskSpawnProxyLayer))

/** One worker-local registry wired to one hub mirror registry over a MessagePort. */
async function buildMirroredTaskJobs() {
  const worker = await buildRegistry()
  const hub = await buildRegistry()
  const channel = new MessageChannel()
  const mirror = createWorkerJobMirror()
  const handlers = buildHubProxyHandlers(
    { ...recordingHubAuthorities().authorities, backgroundJob: mirror.authorityFor(hub.run) },
    instantRunner,
  )
  const disposeServer = serveHubProxy(handlers, channel.port2)
  const client = createHubProxyClient(channel.port1)
  return {
    worker,
    hub,
    mirror,
    jobs: createHubSyncedBackgroundJobs(worker.registry, client),
    close: async () => {
      client.dispose()
      disposeServer()
      channel.port1.close()
      channel.port2.close()
      await worker.close()
      await hub.close()
    },
  }
}

describe("TaskTool over the worker job mirror", () => {
  test("a foreground task is visible on the hub and Ctrl+B promotion crosses the port", async () => {
    const mirrored = await buildMirroredTaskJobs()
    try {
      await Effect.runPromise(
        Effect.scoped(
          withTmpdirInstance()(
            Effect.gen(function* () {
              const context = yield* Layer.build(taskLayerWith(mirrored.jobs))
              yield* Effect.gen(function* () {
                const started = yield* Deferred.make<string>()
                const release = yield* Deferred.make<void>()
                // Promotion installs the completion notification. Waiting for
                // it also drains the notification fiber forked into the layer
                // scope, so the scope close cannot race a still-pending fiber.
                const notified = yield* Deferred.make<void>()
                const prompts: string[] = []
                taskSpawn = (request) =>
                  Effect.gen(function* () {
                    yield* Deferred.succeed(started, request.childID ?? request.prompt.sessionID)
                    yield* Deferred.await(release)
                    return spawnResult(request, "promoted done")
                  })
                const { chat, assistant } = yield* seed()
                const tool = yield* TaskTool
                const def = yield* tool.init()

                const fiber = yield* def
                  .execute(
                    { description: "inspect bug", prompt: "go", subagent_type: "general" },
                    toolContext({
                      sessionID: chat.id,
                      messageID: assistant.id,
                      promptOps: stubOps({
                        onPrompt: (input) => {
                          prompts.push(input.sessionID)
                          Deferred.doneUnsafe(notified, Effect.succeed(undefined))
                        },
                      }),
                    }),
                  )
                  .pipe(Effect.forkChild)

                const spawned = yield* Deferred.await(started)
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.list(), (list) =>
                      list.some((entry) => entry.id === spawned) ? true : undefined,
                    ),
                    "the task tool's worker job never reached the hub mirror",
                  ),
                )

                const promoted = yield* Effect.promise(() =>
                  mirrored.hub.run(mirrored.hub.registry.promote(spawned)),
                )
                expect(promoted?.metadata?.background).toBe(true)

                const exit = yield* Fiber.await(fiber)
                expect(Exit.isSuccess(exit)).toBe(true)
                if (!Exit.isSuccess(exit)) throw new Error("promotion should not fail the task")
                expect(exit.value.metadata.background).toBe(true)

                yield* Deferred.succeed(release, undefined)
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.get(spawned), (settled) =>
                      settled?.status === "completed" ? true : undefined,
                    ),
                    "the hub mirror never observed the task's settlement",
                  ),
                )
                // The promoted job notifies the parent once it settles. The
                // notification is forked from the promotion observer into the
                // layer scope; await it so teardown is ordered after it.
                yield* Deferred.await(notified).pipe(Effect.timeout("5 seconds"))
                expect(prompts).toEqual([chat.id])
              }).pipe(Effect.provide(context))
            }),
          ),
        ),
      )
    } finally {
      await disposeAllInstances()
      await mirrored.close()
    }
  })

  test("releasing the parent session prunes its mirrored task job", async () => {
    const mirrored = await buildMirroredTaskJobs()
    try {
      await Effect.runPromise(
        Effect.scoped(
          withTmpdirInstance()(
            Effect.gen(function* () {
              const context = yield* Layer.build(taskLayerWith(mirrored.jobs))
              yield* Effect.gen(function* () {
                const started = yield* Deferred.make<string>()
                const release = yield* Deferred.make<void>()
                taskSpawn = (request) =>
                  Effect.gen(function* () {
                    yield* Deferred.succeed(started, request.childID ?? request.prompt.sessionID)
                    yield* Deferred.await(release)
                    return spawnResult(request, "background done")
                  })
                const { chat, assistant } = yield* seed()
                const tool = yield* TaskTool
                const def = yield* tool.init()

                const result = yield* def.execute(
                  { description: "inspect bug", prompt: "go", subagent_type: "general", background: true },
                  toolContext({ sessionID: chat.id, messageID: assistant.id, promptOps: stubOps() }),
                )
                const spawned = yield* Deferred.await(started)
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.list(), (list) =>
                      list.some((entry) => entry.id === spawned) ? true : undefined,
                    ),
                    "the background task never reached the hub mirror",
                  ),
                )

                mirrored.mirror.releaseSession(chat.id)
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.get(spawned), (pruned) =>
                      pruned?.status !== undefined && pruned.status !== "running" ? true : undefined,
                    ),
                    "releasing the parent session left a running mirror ghost",
                  ),
                )
                expect(result.metadata.background).toBe(true)

                yield* Deferred.succeed(release, undefined)
              }).pipe(Effect.provide(context))
            }),
          ),
        ),
      )
    } finally {
      await disposeAllInstances()
      await mirrored.close()
    }
  })

  test("a background resume reports background on the hub mirror and notifies once", async () => {
    const mirrored = await buildMirroredTaskJobs()
    try {
      await Effect.runPromise(
        Effect.scoped(
          withTmpdirInstance()(
            Effect.gen(function* () {
              const context = yield* Layer.build(taskLayerWith(mirrored.jobs))
              yield* Effect.gen(function* () {
                const started = yield* Deferred.make<string>()
                const release = yield* Deferred.make<void>()
                let spawns = 0
                taskSpawn = (request) =>
                  Effect.gen(function* () {
                    spawns += 1
                    if (spawns > 1) return spawnResult(request, "resumed run")
                    yield* Deferred.succeed(started, request.childID ?? request.prompt.sessionID)
                    yield* Deferred.await(release)
                    return spawnResult(request, "first")
                  })
                const prompts: string[] = []
                const injected = yield* Deferred.make<void>()
                const ops = stubOps({
                  onPrompt: (input) => {
                    prompts.push(input.sessionID)
                    Deferred.doneUnsafe(injected, Effect.succeed(undefined))
                  },
                })
                const { chat, assistant } = yield* seed()
                const tool = yield* TaskTool
                const def = yield* tool.init()

                const foreground = yield* def
                  .execute(
                    { description: "inspect bug", prompt: "go", subagent_type: "general" },
                    toolContext({ sessionID: chat.id, messageID: assistant.id, promptOps: ops }),
                  )
                  .pipe(Effect.forkChild)
                const spawned = yield* Deferred.await(started)
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.list(), (list) =>
                      list.some((entry) => entry.id === spawned) ? true : undefined,
                    ),
                    "the foreground task never reached the hub mirror",
                  ),
                )

                const resumed = yield* def.execute(
                  {
                    description: "inspect bug",
                    prompt: "continue",
                    subagent_type: "general",
                    task_id: spawned,
                    background: true,
                  },
                  toolContext({ sessionID: chat.id, messageID: assistant.id, promptOps: ops }),
                )
                expect(resumed.metadata.background).toBe(true)

                // The worker-side promotion from the extension crosses the port,
                // so the hub no longer reports the job as foreground.
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.get(spawned), (entry) =>
                      entry?.metadata?.background === true ? true : undefined,
                    ),
                    "the hub mirror still reports the resumed job as foreground",
                  ),
                )

                // A later Ctrl+B promotion observes the already-background job.
                const later = yield* Effect.promise(() =>
                  mirrored.hub.run(mirrored.hub.registry.promote(spawned)),
                )
                expect(later?.metadata?.background).toBe(true)

                yield* Deferred.succeed(release, undefined)
                const foregroundExit = yield* Fiber.await(foreground)
                expect(Exit.isSuccess(foregroundExit)).toBe(true)
                yield* Effect.promise(() =>
                  waitUntil(
                    Effect.map(mirrored.hub.registry.get(spawned), (entry) =>
                      entry?.status === "completed" ? true : undefined,
                    ),
                    "the hub mirror never observed the resumed task settlement",
                  ),
                )
                yield* Deferred.await(injected).pipe(Effect.timeout("1 second"))
                expect(prompts).toHaveLength(1)
              }).pipe(Effect.provide(context))
            }),
          ),
        ),
      )
    } finally {
      await disposeAllInstances()
      await mirrored.close()
    }
  })
})

// ---------------------------------------------------------------------------
// Production layer: the hub serves the request-hook bridge by default.
//
// `options.ports` is undefined in production, yet the default port source wires
// the hub's own `hubProxyFor`. The init must therefore defer `chat.params`, and
// one bridged request must invoke the plugin exactly once (never twice).
// ---------------------------------------------------------------------------

class ImmediateWorker {
  alive = true
  readonly results = new Map<string, { sessionID: string; value: null }>()
  constructor(readonly id: number) {}
  async execute(run: { sessionID: string }): Promise<void> {
    this.results.set(run.sessionID, { sessionID: run.sessionID, value: null })
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return false
  }
  resultFor(sessionID: string) {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.alive = false
  }
}

describe("production request-hook bridge", () => {
  test("a production hub defers init hooks and one bridged request invokes the plugin once", async () => {
    const counts = { init: 0, request: 0 }
    const hooks = {
      "chat.params": () => {
        counts.init += 1
      },
    }
    const plugin = Layer.succeed(Plugin.Service, {
      trigger: (name: string, _input: unknown, output: unknown) =>
        Effect.sync(() => {
          if (name === "chat.params") counts.request += 1
          return output
        }),
      list: () => Effect.succeed([hooks as never]),
      init: () => Effect.void,
    } as unknown as Plugin.Interface)
    const captured: { init?: SessionWorkerInit; port?: MessagePort } = {}
    const poolFactory: ProductionPoolFactory = (resolveInit, sinks) =>
      createLazyExecutorWorkerPool({
        create: async (id) => {
          captured.init = await resolveInit()
          const channel = new MessageChannel()
          sinks.onHubProxyPort(id, channel.port2)
          captured.port = channel.port1
          return new ImmediateWorker(id)
        },
      })
    const hubNode = AppNodeBuilder.build(
      productionSessionHubNode({ count: 1, poolFactory }),
      productionServiceReplacements({
        provider: {
          getProvider: () =>
            Effect.succeed({
              id: "test",
              name: "Test",
              source: "api",
              env: [],
              options: {},
              models: {},
            } as never),
          getModel: () =>
            Effect.succeed({
              id: "model",
              providerID: "test",
              api: { id: "model-api", url: "https://api.example.test/v1", npm: "@ai-sdk/openai-compatible" },
              name: "Model",
              capabilities: {},
              cost: {},
              limit: { context: 1_000, output: 1_000 },
              status: "active",
              options: {},
              headers: {},
              release_date: "2024-01-01",
            } as never),
        },
      }),
    )

    try {
      await Effect.runPromise(
        Effect.scoped(
          withTmpdirInstance()(
            Effect.gen(function* () {
              const context = yield* Layer.build(Layer.merge(hubNode, plugin))
              yield* Effect.gen(function* () {
                const hub = Context.get(context, SessionHubService)
                const sessionID = sid("ses_prod_hooks")
                yield* Effect.promise(() => hub.ensureRunning(sessionID))

                expect(captured.init).toBeDefined()
                expect(counts).toEqual({ init: 0, request: 0 })
                expect(captured.init?.plugins?.[0]?.resolved?.["chat.params"]).toBeUndefined()
                expect(captured.port).toBeInstanceOf(MessagePort)

                const client = createHubProxyClient(captured.port!)
                try {
                  const output = { options: {} }
                  yield* Effect.promise(() =>
                    client.call("pluginTrigger", ["chat.params", { sessionID, agent: "build" }, output]),
                  )
                  expect(counts).toEqual({ init: 0, request: 1 })
                } finally {
                  client.dispose()
                }
              }).pipe(Effect.provide(context))
            }),
          ),
        ),
      )
    } finally {
      await disposeAllInstances()
    }
  })
})
