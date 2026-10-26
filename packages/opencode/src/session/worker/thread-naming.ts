// Injectable session-worker thread-naming seam.
//
// `thread-name.ts` owns the pure grammar; this module owns the small lifecycle
// state machine a worker drives on bind, run start, every terminal
// (settle/cancel/interrupt), and dispose. It performs no I/O: the consumer
// injects `setName`, so production passes `setThreadName` while tests record
// names without touching `/proc`.
//
// C1 wiring (`session-worker.ts`):
//   boot                 -> bind(init.sessionID or the worker slot)
//   execute onRunStart   -> bind(sessionID, true)
//   execute onIdle       -> onIdle()
//   rebind a session     -> bind(newSessionID, false)
//   dispose              -> onDispose()

import { sessionThreadName } from "./thread-name"

export interface ThreadNamerOptions {
  /** Sink for the computed name. Production passes `setThreadName`. */
  readonly setName: (name: string) => void
}

export interface ThreadNamer {
  /**
   * The single lifecycle entry point: bind `sessionID` and advertise its name.
   * `running` marks a live run (`!`); boot and rebind pass `false`.
   */
  bind(sessionID: string, running?: boolean): void
  /** The bound session reached a terminal state: advertise its idle name. */
  onIdle(): void
  /** Drop the binding so later lifecycle calls are no-ops. Never throws. */
  onDispose(): void
}

/** Create the lifecycle seam over the pure `sessionThreadName` grammar. */
export function createThreadNamer(options: ThreadNamerOptions): ThreadNamer {
  let boundSessionID: string | undefined

  const apply = (sessionID: string, running: boolean): void => {
    boundSessionID = sessionID
    options.setName(sessionThreadName(sessionID, { running }))
  }

  return {
    bind(sessionID, running = false) {
      // A run proves which session this worker serves; binding here keeps the
      // next `onIdle()` honest even when boot did not know the session.
      apply(sessionID, running)
    },
    onIdle() {
      if (boundSessionID === undefined) return
      options.setName(sessionThreadName(boundSessionID))
    },
    onDispose() {
      boundSessionID = undefined
    },
  }
}

export * as SessionThreadNaming from "./thread-naming"
