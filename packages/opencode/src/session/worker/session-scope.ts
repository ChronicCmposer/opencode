// Phase 6.7 — per-session child Scope + directory-keyed state disposal
// (plan decision line 59).
//
// A worker is reused across sessions. Each session runs under its own child
// `Scope`, so every resource it acquires (subscriptions, streams, child
// processes) is released when the session ends — even if the worker survives.
// Separately, state keyed by the instance *directory* is disposed when the
// worker's directory changes, so a reused worker never serves a session in
// directory B from state built for directory A.

import { Effect, Exit, Scope } from "effect"
import { WorkerCache, type WorkerCacheOptions } from "./cache"

/** Raised when a second scope is requested for a session that is still open. */
export class ConcurrentSessionScopeError extends Error {
  constructor(readonly sessionID: string) {
    super(`session scope already open for ${sessionID}`)
    this.name = "ConcurrentSessionScopeError"
  }
}

export interface SessionScopeHost {
  /** Sessions currently open. */
  readonly open: number
  /** Sessions whose scope has been closed (finalizers run). */
  readonly closed: number
  /**
   * Run `effect` under a fresh child scope, closing it (and its finalizers) on
   * exit. Re-entrant use of the same `sessionID` fails with
   * `ConcurrentSessionScopeError` so `open`/`closed` can never drift.
   */
  run<A, E>(
    sessionID: string,
    effect: Effect.Effect<A, E, Scope.Scope>,
  ): Effect.Effect<A, E | ConcurrentSessionScopeError, never>
  /** Close every open session scope (worker shutdown). */
  closeAll(): Effect.Effect<void>
}

export function createSessionScopeHost(): SessionScopeHost {
  const scopes = new Map<string, Scope.Closeable>()
  const active = new Set<string>()
  let open = 0
  let closed = 0

  const run = <A, E>(
    sessionID: string,
    effect: Effect.Effect<A, E, Scope.Scope>,
  ): Effect.Effect<A, E | ConcurrentSessionScopeError, never> =>
    // `Effect.suspend` reserves the session synchronously, before `Scope.make`
    // can yield, so two concurrent runs of the same session can never both win.
    Effect.suspend((): Effect.Effect<A, E | ConcurrentSessionScopeError, never> => {
      if (active.has(sessionID)) return Effect.fail(new ConcurrentSessionScopeError(sessionID))
      active.add(sessionID)

      return Effect.gen(function* () {
        const scope = yield* Scope.make()
        scopes.set(sessionID, scope)
        open += 1

        const close = Effect.suspend((): Effect.Effect<void> => {
          if (!scopes.has(sessionID)) return Effect.void
          scopes.delete(sessionID)
          open -= 1
          closed += 1
          return Scope.close(scope, Exit.void)
        })

        return yield* effect.pipe(
          Effect.provideService(Scope.Scope, scope),
          Effect.onExit(() => close),
        )
      }).pipe(Effect.onExit(() => Effect.sync(() => active.delete(sessionID))))
    })

  const closeAll = (): Effect.Effect<void> =>
    Effect.gen(function* () {
      for (const [sessionID, scope] of [...scopes.entries()]) {
        scopes.delete(sessionID)
        open -= 1
        closed += 1
        yield* Scope.close(scope, Exit.void)
      }
    })

  return {
    get open() {
      return open
    },
    get closed() {
      return closed
    },
    run,
    closeAll,
  }
}

export interface DirectoryState<A> {
  readonly current: string | undefined
  /** Build/return the state for `directory`, disposing the previous directory's state. */
  use<B>(directory: string, select: (value: A) => B | Promise<B>): Promise<B>
  /** Dispose the current directory's state. */
  dispose(): Promise<void>
}

export interface DirectoryStateOptions<A> {
  readonly build: (directory: string) => A | Promise<A>
  readonly dispose?: (value: A, directory: string) => void | Promise<void>
}

/**
 * A directory's state is either still building or ready. Presence is carried by
 * the entry itself, never by the value, so a legitimately `undefined` value is
 * still tracked and disposed.
 */
type DirectoryEntry<A> =
  | { readonly directory: string; readonly ready: false }
  | { readonly directory: string; readonly ready: true; readonly value: A }

/**
 * Directory-keyed state holder. Switching directories disposes the old value
 * before building the new one, so a reused worker never leaks state across
 * instance directories. `use` calls are serialized through a queue and the new
 * directory is published before the async `build`, so a concurrent reader never
 * observes the previous directory mid-switch.
 */
export function createDirectoryState<A>(options: DirectoryStateOptions<A>): DirectoryState<A> {
  let current: DirectoryEntry<A> | undefined
  let queue: Promise<unknown> = Promise.resolve()

  /** Serialize `work` behind every in-flight `use`/`dispose` so switches never interleave. */
  const enqueue = <T>(work: () => Promise<T>): Promise<T> => {
    const result = queue.then(work)
    queue = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  /** Drop the live entry and run its finalizer. Callers must hold the queue. */
  const disposeCurrent = async (): Promise<void> => {
    if (current === undefined) return
    const entry = current
    current = undefined
    if (entry.ready) await options.dispose?.(entry.value, entry.directory)
  }

  const use = <B>(directory: string, select: (value: A) => B | Promise<B>): Promise<B> =>
    enqueue(async () => {
      if (current !== undefined && current.directory === directory && current.ready) {
        return select(current.value)
      }
      await disposeCurrent()
      current = { directory, ready: false }
      let value: A
      try {
        value = await options.build(directory)
      } catch (error) {
        current = undefined
        throw error
      }
      current = { directory, ready: true, value }
      return select(value)
    })

  return {
    get current() {
      return current?.directory
    },
    use,
    // Public disposal joins the same queue, so a dispose racing an in-flight build
    // runs after it and cannot be undone by the build publishing afterwards.
    dispose: () => enqueue(disposeCurrent),
  }
}

/**
 * A per-directory {@link WorkerCache}. Concurrent sessions in one worker share
 * the directory's cache (built once); switching directories disposes it, so a
 * reused worker never serves directory B from state built for directory A.
 */
export function createDirectoryCache(options: WorkerCacheOptions = {}): DirectoryState<WorkerCache> {
  return createDirectoryState<WorkerCache>({
    build: () => new WorkerCache(options),
    dispose: (cache) => cache.clear(),
  })
}

export * as WorkerSessionScope from "./session-scope"
