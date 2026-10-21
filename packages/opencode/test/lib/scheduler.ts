// Deterministic scheduler for tests that exercise timer-based grace periods
// (e.g. the cancel grace controller). Extracted from the per-file
// `createTestScheduler` helper.
//
// `schedule(delayMs, run)` records a timer instead of arming a real one, so the
// test decides exactly when it fires. `fire`/`fireDue` run the pending timers in
// schedule order and return how many ran; a cancel handle marks a timer
// cancelled so it is skipped. The scheduler never touches wall-clock time — the
// sequence of outcomes is fully determined by the test (Law of Atomic
// Predictability).

/** A recorded timer. */
export interface RecordingTimer {
  readonly delayMs: number
  readonly run: () => void
  /** Set once the returned cancel handle (or `cancelAll`) ran. */
  readonly cancelled: boolean
}

/** The signature a recording scheduler's `schedule` exposes. */
export type SchedulerFn = (delayMs: number, run: () => void) => unknown

export interface RecordingScheduler<S extends SchedulerFn = (delayMs: number, run: () => void) => () => void> {
  /** The captured scheduler function; hand this to the code under test. */
  readonly schedule: S
  /** Every timer ever recorded, in schedule order. */
  readonly timers: ReadonlyArray<RecordingTimer>
  /** Timers that have not been cancelled. */
  pending(): ReadonlyArray<RecordingTimer>
  /** Run every pending timer (schedule order); returns how many ran. */
  fire(): number
  /** Run pending timers whose `delayMs` is `<= delayMs`; returns how many ran. */
  fireDue(delayMs: number): number
  /** Mark every pending timer cancelled. */
  cancelAll(): void
}

/** Create a recording scheduler parameterised by the schedule function type `S`. */
export function createRecordingScheduler<S extends SchedulerFn = (delayMs: number, run: () => void) => () => void>(): RecordingScheduler<S> {
  const timers: RecordingTimer[] = []

  const schedule = ((delayMs: number, run: () => void) => {
    const timer: RecordingTimer = { delayMs, run, cancelled: false }
    timers.push(timer)
    return () => {
      // `RecordingTimer.cancelled` is readonly to consumers; this is the sole writer.
      ;(timer as { cancelled: boolean }).cancelled = true
    }
  }) as unknown as S

  const runPending = (shouldRun: (timer: RecordingTimer) => boolean): number => {
    let ran = 0
    for (const timer of timers) {
      if (timer.cancelled || !shouldRun(timer)) continue
      ;(timer as { cancelled: boolean }).cancelled = true
      timer.run()
      ran++
    }
    return ran
  }

  return {
    schedule,
    get timers() {
      return timers.slice()
    },
    pending: () => timers.filter((timer) => !timer.cancelled),
    fire: () => runPending(() => true),
    fireDue: (delayMs) => runPending((timer) => timer.delayMs <= delayMs),
    cancelAll: () => {
      for (const timer of timers) (timer as { cancelled: boolean }).cancelled = true
    },
  }
}
