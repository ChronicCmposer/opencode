import { afterEach, describe, expect, mock } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect, Fiber, Layer, Queue } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Session as SessionNs } from "@/session/session"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { SessionPaths } from "../../src/server/routes/instance/httpapi/groups/session"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

// Test isolation: the ambient harness may export a global config
// (`OPENCODE_CONFIG_CONTENT`), point at a shared config dir, or disable project
// config loading entirely. These tests own their project `opencode.json`, so
// drop the ambient overrides before any layer resolves.
delete process.env.OPENCODE_DISABLE_PROJECT_CONFIG
delete process.env.OPENCODE_CONFIG_CONTENT
delete process.env.OPENCODE_CONFIG_DIR

// `EventV2.node` is a process-global node, so compiling it here exposes the same
// pubsub the server publishes on — the event-bridge consumer test observes real
// events.
const it = testEffect(Layer.mergeAll(LayerNode.compile(SessionNs.node), LayerNode.compile(EventV2.node), httpApiLayer))

afterEach(async () => {
  mock.restore()
  await disposeAllInstances()
})

describe("session action routes", () => {
  it.instance(
    "session routes expose metadata on create, update, get, and fork",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "Content-Type": "application/json" }

        const created = yield* requestInDirectory("/session", test.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            title: "meta-session",
            metadata: { source: "sdk", trace: { id: "abc" } },
          }),
        })
        expect(created.status).toBe(200)

        const session = (yield* created.json) as SessionNs.Info
        expect(session.metadata).toEqual({ source: "sdk", trace: { id: "abc" } })

        const updated = yield* requestInDirectory(`/session/${session.id}`, test.directory, {
          method: "PATCH",
          headers,
          body: JSON.stringify({ metadata: { source: "sdk", trace: { id: "def" }, tags: ["one"] } }),
        })
        expect(updated.status).toBe(200)

        const next = (yield* updated.json) as SessionNs.Info
        expect(next.metadata).toEqual({ source: "sdk", trace: { id: "def" }, tags: ["one"] })

        const fetched = yield* requestInDirectory(`/session/${session.id}`, test.directory)
        expect(fetched.status).toBe(200)
        expect(((yield* fetched.json) as SessionNs.Info).metadata).toEqual(next.metadata)

        const forked = yield* requestInDirectory(`/session/${session.id}/fork`, test.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({}),
        })
        expect(forked.status).toBe(200)

        const fork = (yield* forked.json) as SessionNs.Info
        expect(fork.metadata).toEqual(next.metadata)

        const reset = yield* requestInDirectory(`/session/${session.id}`, test.directory, {
          method: "PATCH",
          headers,
          body: JSON.stringify({ metadata: {} }),
        })
        expect(reset.status).toBe(200)
        expect(((yield* reset.json) as SessionNs.Info).metadata).toEqual({})

        yield* SessionNs.Service.use((svc) => svc.remove(fork.id).pipe(Effect.ignore))
        yield* SessionNs.Service.use((svc) => svc.remove(session.id).pipe(Effect.ignore))
      }),
    { git: true },
  )

  it.instance(
    "abort route returns success",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const session = yield* Effect.acquireRelease(SessionNs.use.create({}), (created) =>
          SessionNs.use.remove(created.id).pipe(Effect.ignore),
        )

        const res = yield* requestInDirectory(`/session/${session.id}/abort`, test.directory, { method: "POST" })

        expect(res.status).toBe(200)
        expect(yield* res.json).toEqual({
          sessionID: session.id,
          mode: "completed",
          winner: "completion",
          acknowledged: false,
          durationMs: 0,
        })
      }),
    { git: true },
  )

  // W9 acceptance (HTTP abort route): aborting a busy session must cancel the
  // real run and report the rich cancel outcome (mode/winner/acknowledged/
  // durationMs), not the idle no-op body. Uses a hanging shell so no provider
  // model is required. A child session of the aborted parent must end idle.
  it.instance(
    "abort route reports a real cancel outcome and idles the session",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const headers = { "Content-Type": "application/json" }
        const parent = yield* Effect.acquireRelease(SessionNs.use.create({ title: "parent" }), (created) =>
          SessionNs.use.remove(created.id).pipe(Effect.ignore),
        )
        const child = yield* Effect.acquireRelease(
          SessionNs.use.create({ title: "child", parentID: parent.id }),
          (created) => SessionNs.use.remove(created.id).pipe(Effect.ignore),
        )

        // The shell route is synchronous, so run it in a fiber and cancel it via
        // the abort route before it finishes.
        const shellFiber = yield* requestInDirectory(
          SessionPaths.shell.replace(":sessionID", parent.id),
          test.directory,
          {
            method: "POST",
            headers,
            body: JSON.stringify({
              agent: "build",
              model: { providerID: "test", modelID: "test-model" },
              command: "sleep 30",
            }),
          },
        ).pipe(Effect.forkChild)

        yield* pollWithTimeout(
          Effect.gen(function* () {
            const res = yield* requestInDirectory(SessionPaths.status, test.directory)
            const statuses = (yield* res.json) as Record<string, { type: string }>
            return statuses[parent.id]?.type === "busy" ? (true as const) : undefined
          }),
          "session never became busy",
          "10 seconds",
        )

        const abort = yield* requestInDirectory(SessionPaths.abort.replace(":sessionID", parent.id), test.directory, {
          method: "POST",
        })
        expect(abort.status).toBe(200)
        const outcome = (yield* abort.json) as {
          sessionID: string
          mode: string
          winner: string
          acknowledged: boolean
          durationMs: number
        }
        expect(outcome).toMatchObject({
          sessionID: parent.id,
          mode: "cooperative",
          winner: "cancel",
        })
        // `acknowledged` records whether this request observed the worker's
        // `cancel-ack` before the outcome was decided. It is genuinely
        // flag-dependent: flag-off there is no worker protocol, so it is always
        // `false`; flag-on a real worker acknowledges the cooperative cancel, so
        // it is `true`. Assert the field's contract (a boolean) rather than one
        // mode's value so the acceptance test holds in both flag states.
        expect(typeof outcome.acknowledged).toBe("boolean")
        expect(outcome.durationMs).toBeGreaterThanOrEqual(0)

        yield* pollWithTimeout(
          Effect.gen(function* () {
            const res = yield* requestInDirectory(SessionPaths.status, test.directory)
            const statuses = (yield* res.json) as Record<string, { type: string }>
            // An idle session is absent from the map: `SessionStatus.set(idle)`
            // deletes the entry.
            return statuses[parent.id] === undefined ? (true as const) : undefined
          }),
          "parent never returned to idle after abort",
          "10 seconds",
        )

        const statuses = (yield* (yield* requestInDirectory(SessionPaths.status, test.directory)).json) as Record<
          string,
          { type: string }
        >
        expect(statuses[child.id]?.type ?? "idle").toBe("idle")

        // The aborted shell fiber must have settled (not left running).
        yield* Fiber.interrupt(shellFiber)
      }),
    { git: true, config: { formatter: false, lsp: false }, timeout: 20_000 },
  )

  // W9 (`session.cancelled` consumer): the hub publishes `session.cancelled`
  // with the resolved cancel outcome, but no client surface consumed it. This
  // guards that the event bridge is a working consumer channel for the exact
  // payload the hub emits. A first-class TUI/app handler is requested separately.
  it.instance(
    "session.cancelled reaches an event-bridge consumer",
    () =>
      Effect.gen(function* () {
        const events = yield* EventV2.Service
        const received = yield* Queue.unbounded<{ type: string; data: unknown }>()
        const unsubscribe = yield* events.listen((event) =>
          Effect.sync(() => Queue.offerUnsafe(received, { type: event.type, data: event.data })),
        )
        yield* Effect.addFinalizer(() => unsubscribe)

        // Exactly the payload `hub.ts` publishes on a winning cancel.
        yield* events.publishLive({
          id: EventV2.ID.create(),
          type: SessionV1.Event.Cancelled.type,
          data: { sessionID: "ses_w9_cancelled", mode: "cooperative", durationMs: 5, acknowledged: true },
        })

        const event = yield* Queue.take(received).pipe(
          Effect.timeoutOrElse({
            duration: "2 seconds",
            orElse: () =>
              Effect.fail(new Error("session.cancelled was not delivered to an event-bridge consumer")),
          }),
        )
        expect(event.type).toBe(SessionV1.Event.Cancelled.type)
        expect(event.data).toMatchObject({
          sessionID: "ses_w9_cancelled",
          mode: "cooperative",
          acknowledged: true,
        })
      }),
    { git: true },
  )

  it.instance(
    "experimental background route is a no-op without synchronous subagents",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        const session = yield* Effect.acquireRelease(SessionNs.use.create({}), (created) =>
          SessionNs.use.remove(created.id).pipe(Effect.ignore),
        )

        const res = yield* requestInDirectory(`/experimental/session/${session.id}/background`, test.directory, {
          method: "POST",
        })

        expect(res.status).toBe(200)
        expect(yield* res.json).toBe(false)
      }),
    { git: true },
  )
})
