// Polling helper for tests that must wait on a real-world side effect (a worker
// reaching a state, a file appearing, a port draining) instead of sleeping a
// fixed delay.
//
// `pollUntil` repeatedly awaits a predicate until it returns `true`, then
// resolves. On timeout it fails loudly with a caller-supplied message (Law of
// "Fail Fast, Fail Loud"): a flaky wait must surface as a descriptive failure,
// never as a silent hang or a false pass.

export interface PollUntilOptions {
  /** Total time to keep polling before failing. Defaults to 2000 ms. */
  readonly timeoutMs?: number
  /** Delay between attempts. Defaults to 10 ms. */
  readonly intervalMs?: number
  /** Failure message used when the predicate never becomes true. */
  readonly message?: string
}

/**
 * Resolve once `predicate()` is `true`, retrying every `intervalMs` until
 * `timeoutMs` elapses. The predicate may be sync or async.
 */
export async function pollUntil(
  predicate: () => boolean | Promise<boolean>,
  options: PollUntilOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 2000
  const intervalMs = options.intervalMs ?? 10
  const deadline = Date.now() + timeoutMs

  for (;;) {
    if (await predicate()) return
    if (Date.now() >= deadline) {
      throw new Error(options.message ?? `pollUntil timed out after ${timeoutMs}ms`)
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }
}
