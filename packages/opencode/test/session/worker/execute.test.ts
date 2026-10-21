// Phase 5.1–5.4 — session execution seam (plan tasks 5.1–5.4, 5.6).
//
// The run-state machine (`Runner` per session key) is a portable unit: the hub
// runs it in-process when the rollout flag is off, the worker runs it when the
// flag is on. These tests exercise the worker-local machine and the executor
// wire protocol directly (no threads), plus the rollout flag's default.

import { describe, expect, test } from "bun:test"
import { ConfigProvider, Effect, Exit, Layer, Scope } from "effect"
import type { SessionID } from "@/session/schema"
import { RuntimeFlags } from "@/effect/runtime-flags"
import {
  createSessionExecutor,
  createSessionRunMachine,
  DEFAULT_WORK_KIND,
  SessionBusyError,
  UnknownWorkKindError,
  type SessionExecutionResult,
  type SessionWorkFactory,
} from "@/session/worker/execute"
import { testEffect } from "../../lib/effect"

const sid = (value: string) => value as SessionID
const standaloneScope = () => Effect.runSync(Scope.make())

/** A promise gate plus a `started` signal, both plain (no cross-runtime Effect). */
function gate() {
  let open!: () => void
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  let started!: () => void
  const startedPromise = new Promise<void>((resolve) => {
    started = resolve
  })
  return { opened, open, startedPromise, started }
}

const result = (sessionID: string, value: unknown): SessionExecutionResult => ({ sessionID, value })

/** A work factory whose single "echo" kind returns `kind`; unknown kinds throw. */
const echoWork: SessionWorkFactory = (kind) => {
  if (kind !== "echo") throw new UnknownWorkKindError({ message: `unknown kind: ${kind}`, kind })
  return (sessionID) => Effect.succeed(result(sessionID, kind))
}

describe("5.2 worker-local run-state machine", () => {
  test("per-key serialization: concurrent ensureRunning for one key joins a single run", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    let executions = 0
    const work = Effect.gen(function* () {
      executions += 1
      yield* Effect.sleep("20 millis")
      return result("ses_1", executions)
    })

    const [a, b] = await Promise.all([
      Effect.runPromise(machine.ensureRunning(sid("ses_1"), work)),
      Effect.runPromise(machine.ensureRunning(sid("ses_1"), work)),
    ])

    expect(a).toEqual(b)
    expect(executions).toBe(1)
    expect(machine.busy(sid("ses_1"))).toBe(false)
  })

  test("different keys run concurrently", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    const g = gate()
    const work = (tag: string): Effect.Effect<SessionExecutionResult> =>
      Effect.gen(function* () {
        yield* Effect.sync(() => g.started())
        yield* Effect.promise(() => g.opened)
        return result(tag, tag)
      })

    const first = Effect.runPromise(machine.ensureRunning(sid("ses_a"), work("ses_a")))
    const second = Effect.runPromise(machine.ensureRunning(sid("ses_b"), work("ses_b")))
    await g.startedPromise

    expect(machine.busy(sid("ses_a"))).toBe(true)
    expect(machine.busy(sid("ses_b"))).toBe(true)

    g.open()
    const [a, b] = await Promise.all([first, second])
    expect(a.sessionID).toBe("ses_a")
    expect(b.sessionID).toBe("ses_b")
  })

  test("assertNotBusy fails worker-local with SessionBusyError while a run is active", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    const g = gate()
    const running = Effect.runPromise(
      machine.ensureRunning(
        sid("ses_1"),
        Effect.gen(function* () {
          yield* Effect.sync(() => g.started())
          yield* Effect.promise(() => g.opened)
          return result("ses_1", "done")
        }),
      ),
    )
    await g.startedPromise

    expect(machine.busy(sid("ses_1"))).toBe(true)
    await expect(Effect.runPromise(machine.assertNotBusy(sid("ses_1")))).rejects.toBeInstanceOf(SessionBusyError)

    g.open()
    await running
    expect(machine.busy(sid("ses_1"))).toBe(false)
    await Effect.runPromise(machine.assertNotBusy(sid("ses_1")))
  })

  test("startShell rejects RunnerBusy while a run is active", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    const g = gate()
    const running = Effect.runPromise(
      machine.ensureRunning(
        sid("ses_1"),
        Effect.gen(function* () {
          yield* Effect.sync(() => g.started())
          yield* Effect.promise(() => g.opened)
          return result("ses_1", "run")
        }),
      ),
    )
    await g.startedPromise

    const exit = await Effect.runPromise(machine.startShell(sid("ses_1"), Effect.succeed(result("ses_1", "shell"))).pipe(Effect.exit))
    expect(Exit.isFailure(exit)).toBe(true)

    g.open()
    await running
    const shell = await Effect.runPromise(machine.startShell(sid("ses_1"), Effect.succeed(result("ses_1", "shell"))))
    expect(shell.value).toBe("shell")
  })

  test("cancel interrupts a running session", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    const g = gate()
    const running = Effect.runPromise(
      machine.ensureRunning(
        sid("ses_1"),
        Effect.gen(function* () {
          yield* Effect.sync(() => g.started())
          return yield* Effect.never.pipe(Effect.as(result("ses_1", "never")))
        }),
      ),
    )
    await g.startedPromise
    await Effect.runPromise(machine.cancel(sid("ses_1")))
    await expect(running).rejects.toBeDefined()
    expect(machine.busy(sid("ses_1"))).toBe(false)
  })

  test("cancelAll cancels every active session", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    const g = gate()
    const work = (tag: string): Effect.Effect<SessionExecutionResult> =>
      Effect.gen(function* () {
        yield* Effect.sync(() => g.started())
        return yield* Effect.never.pipe(Effect.as(result(tag, tag)))
      })
    const a = Effect.runPromise(machine.ensureRunning(sid("ses_a"), work("ses_a")))
    const b = Effect.runPromise(machine.ensureRunning(sid("ses_b"), work("ses_b")))
    await g.startedPromise

    await Effect.runPromise(machine.cancelAll)
    await expect(a).rejects.toBeDefined()
    await expect(b).rejects.toBeDefined()
    expect(machine.busy(sid("ses_a"))).toBe(false)
    expect(machine.busy(sid("ses_b"))).toBe(false)
  })
})

describe("5.1 executor protocol", () => {
  test("ping answers pong", async () => {
    const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope: standaloneScope() }), work: echoWork })
    expect(await executor.handle({ type: "ping", id: 7 })).toEqual({ type: "pong", id: 7 })
  })

  test("run answers done with the work result", async () => {
    const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope: standaloneScope() }), work: echoWork })
    const out = await executor.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "echo" })
    expect(out).toEqual({ type: "done", id: 2, result: result("ses_1", "echo") })
  })

  test("an unknown kind answers an error carrying the tag", async () => {
    const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope: standaloneScope() }), work: echoWork })
    const out = await executor.handle({ type: "run", id: 3, sessionID: sid("ses_1"), kind: "missing" })
    expect(out.type).toBe("error")
    if (out.type === "error") expect(out.tag).toBe("UnknownWorkKindError")
  })

  test("assertNotBusy reflects worker-local state", async () => {
    const machine = createSessionRunMachine({ scope: standaloneScope() })
    const executor = createSessionExecutor({ machine, work: echoWork })
    expect(await executor.handle({ type: "assertNotBusy", id: 1, sessionID: sid("ses_1") })).toEqual({
      type: "busy",
      id: 1,
      busy: false,
    })

    const g = gate()
    const slowWork: SessionWorkFactory = () => (sessionID) =>
      Effect.gen(function* () {
        yield* Effect.sync(() => g.started())
        yield* Effect.promise(() => g.opened)
        return result(sessionID, "slow")
      })
    const slow = createSessionExecutor({ machine, work: slowWork })
    const pending = slow.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "slow" })
    await g.startedPromise

    expect(await slow.handle({ type: "assertNotBusy", id: 3, sessionID: sid("ses_1") })).toEqual({
      type: "busy",
      id: 3,
      busy: true,
    })

    g.open()
    expect((await pending).type).toBe("done")
  })

  test("two runs for one key share a single execution", async () => {
    let executions = 0
    const work: SessionWorkFactory = () => (sessionID) =>
      Effect.gen(function* () {
        executions += 1
        yield* Effect.sleep("20 millis")
        return result(sessionID, executions)
      })
    const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope: standaloneScope() }), work })

    const [a, b] = await Promise.all([
      executor.handle({ type: "run", id: 1, sessionID: sid("ses_1"), kind: "shared" }),
      executor.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "shared" }),
    ])

    expect(a.type).toBe("done")
    expect(b.type).toBe("done")
    if (a.type === "done" && b.type === "done") expect(a.result).toEqual(b.result)
    expect(executions).toBe(1)
  })

  test("cancel answers cancelled", async () => {
    const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope: standaloneScope() }), work: echoWork })
    expect(await executor.handle({ type: "cancel", id: 9, sessionID: sid("ses_1") })).toEqual({ type: "cancelled", id: 9 })
  })

  test("DEFAULT_WORK_KIND is the production placeholder kind", () => {
    expect(DEFAULT_WORK_KIND).toBe("noop")
  })
})

describe("5.6 rollout flag", () => {
  const it = testEffect(RuntimeFlags.layer())
  it.live("experimentalSessionWorkers defaults off", () =>
    Effect.gen(function* () {
      const flags = yield* RuntimeFlags.Service
      expect(flags.experimentalSessionWorkers).toBe(false)
    }),
  )

  const envIt = testEffect(
    RuntimeFlags.Service.layer.pipe(
      Layer.provide(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ OPENCODE_EXPERIMENTAL_SESSION_WORKERS: "true" })),
      ),
      Layer.orDie,
    ),
  )
  envIt.live("OPENCODE_EXPERIMENTAL_SESSION_WORKERS enables the pool", () =>
    Effect.gen(function* () {
      const flags = yield* RuntimeFlags.Service
      expect(flags.experimentalSessionWorkers).toBe(true)
    }),
  )
})
