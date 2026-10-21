// Phase 6.7 — per-session child scope + directory state disposal tests.

import { describe, expect, test } from "bun:test"
import { Effect, Fiber } from "effect"
import { WorkerCache } from "@/session/worker/cache"
import { createDirectoryCache, createDirectoryState, createSessionScopeHost } from "@/session/worker/session-scope"

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

  test("a concurrent second run for the same session is rejected, not silently leaked", async () => {
    const host = createSessionScopeHost()
    const fiber = Effect.runFork(host.run("s1", Effect.never))

    await Effect.runPromise(Effect.yieldNow)
    expect(host.open).toBe(1)

    await expect(Effect.runPromise(host.run("s1", Effect.void))).rejects.toThrow(/already open for s1/)
    expect(host.open).toBe(1)
    expect(host.closed).toBe(0)

    await Effect.runPromise(Fiber.interrupt(fiber))
    expect(host.open).toBe(0)
    expect(host.closed).toBe(1)
  })

  test("concurrent sessions in one worker each get their own scope and all finalizers run", async () => {
    const finalized: string[] = []
    const host = createSessionScopeHost()

    const session = (id: string) =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Effect.sync(() => finalized.push(id)))
        yield* Effect.yieldNow
        return id
      })

    const results = await Promise.all([
      Effect.runPromise(host.run("a", session("a"))),
      Effect.runPromise(host.run("b", session("b"))),
      Effect.runPromise(host.run("c", session("c"))),
    ])

    expect(results).toEqual(["a", "b", "c"])
    expect(finalized.sort()).toEqual(["a", "b", "c"])
    expect(host.open).toBe(0)
    expect(host.closed).toBe(3)
  })

  test("a finished session scope does not leak into the next concurrent run", async () => {
    const open: string[] = []
    const host = createSessionScopeHost()

    const hold = (id: string) =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Effect.sync(() => open.splice(open.indexOf(id), 1)))
        open.push(id)
        yield* Effect.never
      })

    const a = Effect.runFork(host.run("a", hold("a")))
    await Effect.runPromise(Effect.yieldNow)
    expect(host.open).toBe(1)

    const b = Effect.runFork(host.run("b", hold("b")))
    await Effect.runPromise(Effect.yieldNow)
    expect(host.open).toBe(2)

    await Effect.runPromise(Fiber.interrupt(a))
    expect(host.open).toBe(1)
    expect(open).toEqual(["b"])

    await Effect.runPromise(Fiber.interrupt(b))
    expect(host.open).toBe(0)
    expect(open).toEqual([])
    expect(host.closed).toBe(2)
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

  test("a built undefined value is still present and disposed", async () => {
    const disposed: string[] = []
    const state = createDirectoryState<undefined>({
      build: () => undefined,
      dispose: (_value, directory) => void disposed.push(directory),
    })

    await state.use("/a", () => undefined)
    expect(state.current).toBe("/a")

    await state.use("/b", () => undefined)
    expect(disposed).toEqual(["/a"])

    await state.dispose()
    expect(disposed).toEqual(["/a", "/b"])
    expect(state.current).toBeUndefined()
  })

  test("concurrent switches are serialized so builds never interleave", async () => {
    const events: string[] = []
    const state = createDirectoryState<{ directory: string }>({
      build: async (directory) => {
        events.push(`build:start:${directory}`)
        await new Promise((resolve) => setTimeout(resolve, 5))
        events.push(`build:end:${directory}`)
        return { directory }
      },
      dispose: (value) => void events.push(`dispose:${value.directory}`),
    })

    const [a, b] = await Promise.all([state.use("/a", (value) => value.directory), state.use("/b", (value) => value.directory)])

    expect(a).toBe("/a")
    expect(b).toBe("/b")
    expect(events).toEqual(["build:start:/a", "build:end:/a", "dispose:/a", "build:start:/b", "build:end:/b"])
  })

  test("current reflects the new directory before the build resolves", async () => {
    let enterBuild: (() => void) | undefined
    const buildEntered = new Promise<void>((resolve) => {
      enterBuild = resolve
    })
    let release: (() => void) | undefined
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const state = createDirectoryState<{ directory: string }>({
      build: async (directory) => {
        enterBuild?.()
        await gate
        return { directory }
      },
    })

    const pending = state.use("/a", (value) => value.directory)
    await buildEntered
    expect(state.current).toBe("/a")

    release?.()
    expect(await pending).toBe("/a")
  })
})

describe("directory state under concurrency (step 30)", () => {
  test("concurrent use of the same directory builds exactly once", async () => {
    let builds = 0
    const state = createDirectoryState<number>({
      build: async () => {
        builds += 1
        await new Promise((resolve) => setTimeout(resolve, 5))
        return builds
      },
    })

    const results = await Promise.all([
      state.use("/a", (value) => value),
      state.use("/a", (value) => value),
      state.use("/a", (value) => value),
    ])

    expect(builds).toBe(1)
    expect(results).toEqual([1, 1, 1])
  })

  test("concurrent switches keep exactly one live state (no leak)", async () => {
    const live = new Set<string>()
    const state = createDirectoryState<string>({
      build: (directory) => {
        live.add(directory)
        return directory
      },
      dispose: (directory) => void live.delete(directory),
    })

    await Promise.all([state.use("/a", (value) => value), state.use("/b", (value) => value), state.use("/c", (value) => value)])

    expect(live.size).toBe(1)
    expect(state.current).toBe("/c")
    await state.dispose()
    expect(live.size).toBe(0)
  })

  test("disposing while a build is in flight does not resurrect the state", async () => {
    let enterBuild: (() => void) | undefined
    const buildEntered = new Promise<void>((resolve) => {
      enterBuild = resolve
    })
    let releaseBuild: (() => void) | undefined
    const buildGate = new Promise<void>((resolve) => {
      releaseBuild = resolve
    })
    const disposed: string[] = []
    const state = createDirectoryState<{ directory: string }>({
      build: async (directory) => {
        enterBuild?.()
        await buildGate
        return { directory }
      },
      dispose: (value) => void disposed.push(value.directory),
    })

    const pending = state.use("/a", (value) => value.directory)
    await buildEntered
    const disposing = state.dispose()
    releaseBuild?.()

    expect(await pending).toBe("/a")
    await disposing
    expect(state.current).toBeUndefined()
    expect(disposed).toEqual(["/a"])
  })
})

describe("directory cache integration (step 30)", () => {
  test("a per-directory cache is built once and isolated across directories", async () => {
    const state = createDirectoryCache()

    const value = await state.use("/a", (cache) => {
      cache.defineImmutable("agent/plan", "plan-a")
      return cache.get("agent/plan")
    })
    expect(value).toBe("plan-a")

    // Same directory: no rebuild, same cache instance.
    const again = await state.use("/a", (cache) => cache.get("agent/plan"))
    expect(again).toBe("plan-a")

    // Switching directories disposes the old cache and builds a fresh one.
    const other = await state.use("/b", (cache) => cache.get("agent/plan"))
    expect(other).toBeUndefined()

    await state.dispose()
    expect(state.current).toBeUndefined()
  })

  test("a disposed directory cache is cleared, not retained", async () => {
    let disposedCache: WorkerCache | undefined
    const state = createDirectoryState<WorkerCache>({
      build: () => new WorkerCache(),
      dispose: (cache) => {
        disposedCache = cache
        cache.clear()
      },
    })

    await state.use("/a", (cache) => cache.defineImmutable("k", "v"))
    await state.use("/b", () => undefined)

    expect(disposedCache?.size).toBe(0)
  })
})
