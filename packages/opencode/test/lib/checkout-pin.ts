/**
 * Checkout-pin verification (plan task 3.1).
 *
 * The plan declares the checkout must sit on tag `v1.19.3`. Reading a revision
 * is a boundary concern: the raw git output is parsed into a trusted revision
 * string, and {@link assertCheckoutPin} compares that trusted value against the
 * declared pin. The comparison is pure so the repository-level assertion and
 * the unit-level assertions share one fail-loud contract.
 */

export interface DeclaredPin {
  readonly tag: string
  readonly revision: string
}

/** Declared pin from `.parallel-implement/multicore-session-parallelism.md`. */
export const DECLARED_PIN: DeclaredPin = {
  tag: "v1.19.3",
  revision: "e3c4eb242585dcd77be1a867391b7c25fd47ce52",
}

/** Default ceiling for a read-only git call; a hung git fails instead of blocking. */
export const DEFAULT_GIT_TIMEOUT_MS = 10_000

export interface RevisionReadOptions {
  /** Kill the git invocation after this many ms. Defaults to `DEFAULT_GIT_TIMEOUT_MS`. */
  readonly timeoutMs?: number
}

/** Build the descriptive detail for a failed git read, preferring real stderr. */
export function revisionFailureDetail(input: {
  readonly exitCode: number | null
  readonly signalCode: string | null
  readonly stderr: string
  readonly timeoutMs: number
}): string {
  const stderr = input.stderr.trim()
  if (stderr.length > 0) return stderr
  if (input.signalCode !== null) return `terminated by ${input.signalCode} after ${input.timeoutMs}ms`
  return `exit code ${input.exitCode}`
}

/**
 * Resolves a git ref to its commit revision. Read-only and shell-free, with a
 * bounded timeout so a hung git fails loudly instead of blocking the suite.
 * Throws a descriptive error if git cannot resolve the ref, so a broken checkout
 * fails loudly instead of silently comparing against empty output.
 */
export function readRevision(ref: string, cwd?: string, options: RevisionReadOptions = {}): string {
  const timeoutMs = options.timeoutMs ?? DEFAULT_GIT_TIMEOUT_MS
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`git read timeoutMs must be a positive number, got: ${String(timeoutMs)}`)
  }
  const proc = Bun.spawnSync({
    cmd: ["git", "rev-parse", "--verify", ref],
    cwd,
    stdout: "pipe",
    stderr: "pipe",
    timeout: timeoutMs,
  })
  if (proc.exitCode !== 0) {
    const detail = revisionFailureDetail({
      exitCode: proc.exitCode,
      signalCode: proc.signalCode ?? null,
      stderr: proc.stderr.toString(),
      timeoutMs,
    })
    throw new Error(`git rev-parse --verify ${ref} failed: ${detail}`)
  }
  return proc.stdout.toString().trim()
}

/** Reads the revision the working tree is checked out at (`HEAD`). */
export function readCheckoutRevision(cwd?: string): string {
  return readRevision("HEAD", cwd)
}

/** Resolves the commit a tag points at, peeled through annotated-tag objects. */
export function readTagRevision(tag: string, cwd?: string): string {
  return readRevision(`refs/tags/${tag}^{commit}`, cwd)
}

/**
 * Asserts that `actualRevision` equals the declared pin. Returns on success so
 * callers stay flat; throws immediately with both revisions and the tag on
 * divergence (Law of "Fail Fast, Fail Loud").
 */
export function assertCheckoutPin(actualRevision: string, pin: DeclaredPin = DECLARED_PIN): void {
  if (actualRevision === pin.revision) return
  throw new Error(
    `Checkout revision ${actualRevision || "<empty>"} does not match declared pin ${pin.tag} (${pin.revision})`,
  )
}
