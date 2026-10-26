// Injectable session-worker thread-naming seam.
//
// `thread-name.ts` owns the pure grammar; this module owns the small lifecycle
// state machine a worker drives on bind, run start, every terminal
// (settle/cancel/interrupt), and dispose. It performs no I/O: the consumer
// injects `setName`, so production passes `setThreadName` while tests record
// names without touching `/proc`.
//
// C1 wiring (`session-worker.ts`):
//   boot                 -> onBind(init.sessionID)
//   execute onRunStart   -> onRunStart(sessionID)
//   execute onIdle       -> onIdle()
//   rebind a session     -> onBind(newSessionID) (already emits idle), then onIdle()
//   dispose              -> onDispose()

import { sessionThreadName } from "./thread-name"

export interface ThreadNamerOptions {
  /** Sink for the computed name. Production passes `setThreadName`. */
  readonly setName: (name: string) => void
}

export interface ThreadNamer {
  /** Bind `sessionID` and advertise its idle name. */
  onBind(sessionID: string): void
  /** A run started for `sessionID`: bind it and advertise the running name. */
  onRunStart(sessionID: string): void
  /** The bound session reached a terminal state: advertise its idle name. */
  onIdle(): void
  /** Drop the binding so later lifecycle calls are no-ops. Never throws. */
  onDispose(): void
}

/** Create the lifecycle seam over the pure `sessionThreadName` grammar. */
export function createThreadNamer(options: ThreadNamerOptions): ThreadNamer {
  let boundSessionID: string | undefined

  return {
    onBind(sessionID) {
      boundSessionID = sessionID
      options.setName(sessionThreadName(sessionID))
    },
    onRunStart(sessionID) {
      // A run proves which session this worker serves; rebinding here keeps the
      // next `onIdle()` honest even when the caller did not bind first.
      boundSessionID = sessionID
      options.setName(sessionThreadName(sessionID, { running: true }))
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
