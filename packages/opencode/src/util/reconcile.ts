// Reconcile a fresh array back into an existing array reference.
//
// A hub-proxied `messages.transform` / `system.transform` returns a brand-new
// array (the rewrite cannot cross the control port in place). Call sites that
// must keep mutating the original array — because other holders still reference
// it — use `reconcileInPlace` to copy the fresh contents back without changing
// the identity of the target.

/**
 * Copy every element of `next` into `target`, in place, and return `target`.
 * When `next` is already `target` there is nothing to copy and the call is a
 * no-op. The elements of `next` are not mutated.
 */
export function reconcileInPlace<T>(target: T[], next: readonly T[]): T[] {
  if (next !== target) target.splice(0, target.length, ...next)
  return target
}

export * as Reconcile from "./reconcile"
