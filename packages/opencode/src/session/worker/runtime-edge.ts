// The worker subsystem's single Effect-to-JS boundary.
//
// Every internal worker path composes in Effect. The subsystem's outer APIs,
// however, are Promise- and callback-shaped (Bun `onmessage`, the dispatcher,
// the hub, the pool adapters), so effectful work that must cross back out runs
// through these three functions instead of scattering `Effect.run*` calls
// across the subsystem. Keeping the boundary in one place makes the crossings
// auditable: everything else is Effect composition.
import { Effect, type Fiber } from "effect"

export const WorkerRuntime = {
  runPromise: <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect),
  runSync: <A, E>(effect: Effect.Effect<A, E>): A => Effect.runSync(effect),
  runFork: <A, E>(effect: Effect.Effect<A, E>): Fiber.Fiber<A, E> => Effect.runFork(effect),
}
