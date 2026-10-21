// Phase 6.7 — per-session child scope + directory state disposal tests.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { createDirectoryState, createSessionScopeHost } from "@/session/worker/session-scope"

describe("session scope host", () => {
  test("closes the child scope and runs finalizers when the session ends", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()

    const value = await Effect.runPromise(
      host.run(
        "s1",
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s1")))
          return 42
        }),
      ),
    )

    expect(value).toBe(42)
    expect(finalized).toEqual(["s1"])
    expect(host.open).toBe(0)
    expect(host.closed).toBe(1)
  })

  test("finalizers still run when the session fails", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()

    await expect(
      Effect.runPromise(
        host.run(
          "s1",
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s1")))
            return yield* Effect.fail(new Error("boom"))
          }),
        ),
      ),
    ).rejects.toThrow("boom")

    expect(finalized).toEqual(["s1"])
    expect(host.closed).toBe(1)
  })

  test("reusing the worker for a second session does not leak the first scope", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()

    await Effect.runPromise(
      host.run(
        "s1",
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s1")))
        }),
      ),
    )
    await Effect.runPromise(
      host.run(
        "s2",
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s2")))
        }),
      ),
    )

    expect(finalized).toEqual(["s1", "s2"])
    expect(host.open).toBe(0)
    expect(host.closed).toBe(2)
  })

  test("closeAll disposes every open session scope", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()
    const fiber = Effect.runFork(
      host.run(
        "s1",
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push("s1")))
          return yield* Effect.never
        }),
      ),
    )

    await Effect.runPromise(Effect.yieldNow)
    expect(host.open).toBe(1)
    await Effect.runPromise(host.closeAll())
    expect(finalized).toEqual(["s1"])
    expect(host.open).toBe(0)
    fiber.interruptUnsafe(0)
  })
})

describe("directory state", () => {
  test("builds once per directory and disposes on change", async () => {
    const built: string[] = []
    const disposed: string[] = []
    const state = createDirectoryState<{ directory: string }>({
      build: (directory) => {
        built.push(directory)
        return { directory }
      },
      dispose: (value) => {
        disposed.push(value.directory)
      },
    })

    expect(await state.use("/a", (value) => value.directory)).toBe("/a")
    expect(await state.use("/a", (value) => value.directory)).toBe("/a")
    expect(built).toEqual(["/a"])

    expect(await state.use("/b", (value) => value.directory)).toBe("/b")
    expect(disposed).toEqual(["/a"])
    expect(built).toEqual(["/a", "/b"])

    await state.dispose()
    expect(disposed).toEqual(["/a", "/b"])
    expect(state.current).toBeUndefined()
  })

  test("a second use in the same directory does not rebuild", async () => {
    let builds = 0
    const state = createDirectoryState({ build: () => ++builds })
    await state.use("/a", () => undefined)
    await state.use("/a", () => undefined)
    expect(builds).toBe(1)
  })
})
