import { Effect, Scope } from "effect"
import { make, type Dispatcher, type DispatcherOptions } from "@/session/worker/dispatch"

/** Build a dispatcher synchronously; its scope is closed by the dispatcher's own terminateAll. */
export function createDispatcher(options: DispatcherOptions): Dispatcher {
  return Effect.runSync(make(options).pipe(Scope.provide(Scope.makeUnsafe())))
}
