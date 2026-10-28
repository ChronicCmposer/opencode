// Cancel reporter: reconstruct terminal settlement for a session and its cascade.
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { type LiveEventPublisher } from "@/event-v2-bridge"
import type { SessionID } from "@/session/schema"
import { type CancelledMode, type Dispatcher } from "./dispatch"
import { type CancelOutcome } from "./cancel"
import { workerLogMessage, type WorkerLogger } from "./log"

/** A descendant cancelled by the subagent cascade, and whether it needs a fallback idle. */
export interface CancelledDescendant {
  readonly sessionID: SessionID
  /**
   * `true` when the descendant's run did NOT settle its own worker — it was
   * queued, never admitted, or grace-terminated — so no worker published its
   * terminal idle and the caller must write the fallback idle. `false` when the
   * run settled cooperatively or completed, so its worker published the idle.
   */
  readonly needsFallbackIdle: boolean
}

/**
 * The result of cancelling a session and cascading to its subagent children.
 * The dispatcher discards a descendant's own outcome, so the hub reconstructs
 * the terminal modes it observed and reports which sessions still need an idle.
 */
export interface CancelReport {
  /** The requested session's outcome; `undefined` when it was not admitted. */
  readonly outcome: CancelOutcome | undefined
  /** Every descendant the cascade reached, plus any linked but unsettled child. */
  readonly descendants: ReadonlyArray<CancelledDescendant>
}

/** The cancel reporter's collaborators: the dispatcher (lazy) and the child graph. */
interface CancelReporterOptions {
  readonly dispatcher: () => Dispatcher
  readonly childrenOf: (sessionID: SessionID) => ReadonlyArray<SessionID>
  readonly events?: LiveEventPublisher
  /** Per-session publisher, so a cancel carries the cancelled session's location. */
  readonly eventPublisherFor?: (sessionID: SessionID) => LiveEventPublisher | undefined
  /**
   * Invoked at the START of a cancel, before the grace window begins, for the
   * session and each descendant. The production layer aborts the session's
   * in-flight plugin tools here, so a hub-executed tool unwinds immediately
   * instead of only after the grace window has elapsed.
   */
  readonly onCancelRequested?: (sessionID: SessionID) => void
  readonly log: WorkerLogger
}

interface CancelReporter {
  /** Record a dispatcher-reported terminal mode so the report can reconstruct it. */
  readonly recordCancelled: (sessionID: string, mode: CancelledMode) => void
  readonly cancelWithReport: (sessionID: SessionID) => Promise<CancelReport>
}

/**
 * Reconstruct the terminal settlement of a cancelled session and its subagent
 * cascade. The dispatcher cascades children-first but discards a descendant's own
 * outcome, so this records each mode it reports and derives which descendants
 * still need a fallback idle (a cooperative settle publishes its own; a queued or
 * grace-terminated one does not).
 */
export function createCancelReporter(options: CancelReporterOptions): CancelReporter {
  const cancelledModes = new Map<string, CancelledMode>()

  /**
   * Every descendant of `sessionID` (children-first, depth-first), as the
   * dispatcher's own cascade would visit them. A linked child that was never
   * admitted is included, so it still receives a fallback idle.
   */
  const descendantsOf = (sessionID: SessionID): SessionID[] => {
    const descendants: SessionID[] = []
    const visit = (id: SessionID): void => {
      for (const child of options.childrenOf(id)) {
        visit(child)
        descendants.push(child)
      }
    }
    visit(sessionID)
    return descendants
  }

  /** A cancel mode that implies the worker already published its terminal idle. */
  const workerPublishedIdle = (mode: CancelledMode | undefined): boolean =>
    mode === "cooperative" || mode === "completed"

  const cancelWithReport = async (sessionID: SessionID): Promise<CancelReport> => {
    const descendants = descendantsOf(sessionID)
    // Drop a prior request's recorded modes so its outcome is never reused.
    cancelledModes.delete(sessionID)
    for (const descendant of descendants) cancelledModes.delete(descendant)

    // Signal BEFORE the grace window: an in-flight plugin tool must see its
    // `ctx.abort` fire at the moment of the cancel, not only once the grace has
    // elapsed. The cascade mirrors the dispatcher's children-first order.
    options.onCancelRequested?.(sessionID)
    for (const descendant of descendants) options.onCancelRequested?.(descendant)

    // The dispatcher cascades to the descendants itself (children-first); the
    // recorded modes below reconstruct each descendant's settlement.
    const outcome = await options.dispatcher().cancelWithGrace(sessionID)

    // Report the terminal outcome to every SSE client. `SessionV1.Event.Cancelled`
    // is non-durable, and the live-only publisher refuses a durable definition, so
    // this can never re-commit the session state already written by the worker.
    //
    // Publish only when a cancel actually won. A `completion` outcome means the
    // run finished on its own during the grace window; telling the client it was
    // cancelled would be a lie (and would race the run's own idle/completion).
    // The hub deliberately emits no `session.status: idle` here — `run-state.ts`
    // owns the canonical cancel-idle transition.
    if (outcome !== undefined && outcome.winner === "cancel") {
      const publisher = options.eventPublisherFor?.(sessionID) ?? options.events
      publisher?.publishDefinition(SessionV1.Event.Cancelled, {
        sessionID: outcome.sessionID,
        // The durable event schema predates the typed boot outcome; a boot
        // cancel is a terminate of the partial isolate from its perspective.
        mode: outcome.mode === "boot" ? "terminated" : outcome.mode,
        durationMs: outcome.durationMs,
        acknowledged: outcome.acknowledged,
      })
    }
    const reportDescendants = descendants.map((child) => ({
      sessionID: child,
      needsFallbackIdle: !workerPublishedIdle(cancelledModes.get(child)),
    }))
    options.log(workerLogMessage("cancel cascade"), {
      level: "INFO",
      sessionID,
      descendants: reportDescendants.length,
      needsFallbackIdle: reportDescendants.filter((descendant) => descendant.needsFallbackIdle).length,
    })
    return { outcome, descendants: reportDescendants }
  }

  return {
    recordCancelled: (sessionID, mode) => {
      cancelledModes.set(sessionID, mode)
    },
    cancelWithReport,
  }
}
