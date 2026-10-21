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

export interface SessionScopeHost {
  /** Sessions currently open. */
  readonly open: number
  /** Sessions whose scope has been closed (finalizers run). */
  readonly closed: number
  /** Run `effect` under a fresh child scope, closing it (and its finalizers) on exit. */
  run<A, E>(sessionID: string, effect: Effect.Effect<A, E, Scope.Scope>): Effect.Effect<A, E, never>
  /** Close every open session scope (worker shutdown). */
  closeAll(): Effect.Effect<void>
}

export function createSessionScopeHost(): SessionScopeHost {
  const scopes = new Map<string, Scope.Closeable>()
  let open = 0
  let closed = 0

  const run = <A, E>(sessionID: string, effect: Effect.Effect<A, E, Scope.Scope>): Effect.Effect<A, E, never> =>
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      scopes.set(sessionID, scope)
      open += 1

      const close = Effect.suspend((): Effect.Effect<void> => {
        if (scopes.get(sessionID) !== scope) return Effect.void
        scopes.delete(sessionID)
        open -= 1
        closed += 1
        return Scope.close(scope, Exit.void)
      })

      return yield* effect.pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.onExit(() => close),
      )
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
 * Directory-keyed state holder. Switching directories disposes the old value
 * before building the new one, so a reused worker never leaks state across
 * instance directories.
 */
export function createDirectoryState<A>(options: DirectoryStateOptions<A>): DirectoryState<A> {
  let currentDirectory: string | undefined
  let currentValue: A | undefined

  const dispose = async (): Promise<void> => {
    if (currentDirectory === undefined || currentValue === undefined) return
    const directory = currentDirectory
    const value = currentValue
    currentDirectory = undefined
    currentValue = undefined
    await options.dispose?.(value, directory)
  }

  const use = async <B>(directory: string, select: (value: A) => B | Promise<B>): Promise<B> => {
    if (directory !== currentDirectory) {
      await dispose()
      currentValue = await options.build(directory)
      currentDirectory = directory
    }
    return select(currentValue as A)
  }

  return {
    get current() {
      return currentDirectory
    },
    use,
    dispose,
  }
}

export * as WorkerSessionScope from "./session-scope"
