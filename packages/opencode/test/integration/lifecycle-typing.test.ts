// Lifecycle typing: blocked permission/question requests must be cancelled
// across the worker/hub wire, and a non-responsive worker must fail typed within
// the configured run-reply watchdog budget.

import { describe, expect, test } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Effect, Fiber } from "effect"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { Permission } from "@/permission"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Question } from "@/question"
import { buildHubProxyHandlers, createProductionSessionHub, DEFAULT_RUN_IDLE_TIMEOUT_MS, resolveRunIdleTimeoutMs } from "@/session/worker/hub"
import { createTestBarrier } from "../lib/run-barrier-test"
import { createHubProxyClient, HubProxyRemoteError, serveHubProxy } from "@/session/worker/hub-proxy-wire"
import { SessionWorkerReplyTimeoutError } from "@/session/worker/execute"
import { SESSION_WORKER_IDLE_TIMEOUT_ENV } from "@/session/worker/pool"
import { SessionID } from "@/session/schema"
import { sid } from "../lib/async"
import { testEffect, pollWithTimeout } from "../lib/effect"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { recordingHubAuthorities } from "../lib/hub-proxy"

const lifecycleLayer = LayerNode.compile(
  LayerNode.group([Permission.node, Question.node, EventV2Bridge.node, CrossSpawnSpawner.node]),
)
const itInstance = testEffect(lifecycleLayer)

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

describe("permission/question cancel", () => {
  itInstance.instance("permission.cancelSession fails and removes the pending request", () =>
    Effect.gen(function* () {
      const permission = yield* Permission.Service
      const sessionID = SessionID.make("ses_permission_cancel")
      const fiber = yield* permission.ask(permissionAsk(sessionID)).pipe(Effect.forkScoped)

      yield* pollWithTimeout(
        Effect.map(permission.list(), (pending) => (pending.length === 1 ? pending : undefined)),
        "permission request never became pending",
      )

      if (permission.cancelSession === undefined) throw new Error("permission has no cancelSession")
      yield* permission.cancelSession(sessionID)

      expect(yield* permission.list()).toEqual([])
      expect((yield* Fiber.await(fiber))._tag).toBe("Failure")
    }),
  )

  itInstance.instance("question.cancelSession fails and removes the pending request", () =>
    Effect.gen(function* () {
      const question = yield* Question.Service
      const sessionID = SessionID.make("ses_question_cancel")
      const fiber = yield* question.ask(questionAsk(sessionID)).pipe(Effect.forkScoped)

      yield* pollWithTimeout(
        Effect.map(question.list(), (pending) => (pending.length === 1 ? pending : undefined)),
        "question request never became pending",
      )

      if (question.cancelSession === undefined) throw new Error("question has no cancelSession")
      yield* question.cancelSession(sessionID)

      expect(yield* question.list()).toEqual([])
      expect((yield* Fiber.await(fiber))._tag).toBe("Failure")
    }),
  )

  itInstance.instance("a blocked permission ask is cancelled across the hub-proxy wire", () =>
    Effect.gen(function* () {
      const permission = yield* Permission.Service
      const question = yield* Question.Service
      const instance = yield* InstanceRef
      const channel = new MessageChannel()
      const authorities = recordingHubAuthorities({ permission, question })
      const runner = <A, E>(effect: Effect.Effect<A, E>) =>
        Effect.runPromise(Effect.provideService(effect, InstanceRef, instance))
      const dispose = serveHubProxy(buildHubProxyHandlers(authorities.authorities, runner), channel.port2)
      const client = createHubProxyClient(channel.port1)
      const sessionID = SessionID.make("ses_permission_wire")

      try {
        const request = client.call("permission.ask", [permissionAsk(sessionID)])
        yield* pollWithTimeout(
          Effect.map(permission.list(), (pending) => (pending.length === 1 ? pending : undefined)),
          "wire permission request never became pending",
        )

        if (permission.cancelSession === undefined) throw new Error("permission has no cancelSession")
        yield* permission.cancelSession(sessionID)

        const rejection = yield* Effect.promise(() =>
          request.then(
            () => undefined,
            (error: unknown) => error,
          ),
        )
        expect(rejection).toBeInstanceOf(HubProxyRemoteError)
        expect((rejection as HubProxyRemoteError).tag).toBe("PermissionRejectedError")
        expect(yield* permission.list()).toEqual([])
      } finally {
        client.dispose()
        dispose()
        channel.port1.close()
        channel.port2.close()
      }
    }),
  )

  itInstance.instance("a blocked question ask is cancelled across the hub-proxy wire", () =>
    Effect.gen(function* () {
      const permission = yield* Permission.Service
      const question = yield* Question.Service
      const instance = yield* InstanceRef
      const channel = new MessageChannel()
      const authorities = recordingHubAuthorities({ permission, question })
      const runner = <A, E>(effect: Effect.Effect<A, E>) =>
        Effect.runPromise(Effect.provideService(effect, InstanceRef, instance))
      const dispose = serveHubProxy(buildHubProxyHandlers(authorities.authorities, runner), channel.port2)
      const client = createHubProxyClient(channel.port1)
      const sessionID = SessionID.make("ses_question_wire")

      try {
        const request = client.call("question.ask", [questionAsk(sessionID)])
        yield* pollWithTimeout(
          Effect.map(question.list(), (pending) => (pending.length === 1 ? pending : undefined)),
          "wire question request never became pending",
        )

        if (question.cancelSession === undefined) throw new Error("question has no cancelSession")
      yield* question.cancelSession(sessionID)

        const rejection = yield* Effect.promise(() =>
          request.then(
            () => undefined,
            (error: unknown) => error,
          ),
        )
        expect(rejection).toBeInstanceOf(HubProxyRemoteError)
        expect((rejection as HubProxyRemoteError).tag).toBe("QuestionRejectedError")
        expect(yield* question.list()).toEqual([])
      } finally {
        client.dispose()
        dispose()
        channel.port1.close()
        channel.port2.close()
      }
    }),
  )
})

// A worker that completes the transport handshake path but never answers a run
// or a liveness ping, modelling a wedged event loop.
class NonResponsiveWorker {
  onmessage: ((event: { data: unknown }) => void) | null = null
  onerror: ((event: { message: string; error?: unknown }) => void) | null = null
  readonly posted: unknown[] = []

  postMessage(message: unknown): void {
    this.posted.push(message)
  }

  terminate(): Promise<number> {
    return Promise.resolve(0)
  }
}

describe("run-reply watchdog", () => {
  test("the watchdog is on by default and an explicit 0 disables it", () => {
    expect(resolveRunIdleTimeoutMs({ env: {} })).toBe(DEFAULT_RUN_IDLE_TIMEOUT_MS)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "250" } })).toBe(250)
    expect(resolveRunIdleTimeoutMs({ env: { [SESSION_WORKER_IDLE_TIMEOUT_ENV]: "0" } })).toBe(0)
    expect(resolveRunIdleTimeoutMs({ runIdleTimeoutMs: 0, env: {} })).toBe(0)
  })

  test("a non-responsive worker fails typed within the configured budget", async () => {
    const original = (globalThis as { Worker?: unknown }).Worker
    ;(globalThis as { Worker?: unknown }).Worker = NonResponsiveWorker
    const hub = createProductionSessionHub({ ...{ barrier: createTestBarrier() },
      init: () => sessionWorkerInit(),
      entry: "fake://non-responsive",
      workKind: "instant",
      runIdleTimeoutMs: 100,
    })
    try {
      const outcome = await Promise.race([
        hub.ensureRunning(sid("ses_non_responsive")).then(
          () => "resolved" as const,
          (error: unknown) => error,
        ),
        new Promise<"hung">((resolve) => setTimeout(() => resolve("hung"), 3_000)),
      ])
      expect(outcome).toBeInstanceOf(SessionWorkerReplyTimeoutError)
    } finally {
      await hub.terminateAll()
      ;(globalThis as { Worker?: unknown }).Worker = original
    }
  })
})
