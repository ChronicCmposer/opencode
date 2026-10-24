// Shared typed view over a Bun worker's global scope.
//
// Every fixture worker used to hand-roll the same `interface WorkerScope` and
// cast `globalThis as unknown as WorkerScope`. Centralising the shape keeps the
// cast and the message plumbing identical everywhere, so a worker only declares
// what is specific to it: its inbound/outbound message types.

/** The subset of the worker global scope the fixture workers rely on. */
export interface WorkerScope {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void
  onmessage: ((event: { data: unknown }) => void) | null
  close(): void
}

/** This worker's global scope, typed once for every fixture worker. */
export const scope = globalThis as unknown as WorkerScope

/** Post one frame to the parent thread that spawned this worker. */
export function postToParent(message: unknown, transfer?: readonly Transferable[]): void {
  scope.postMessage(message, transfer)
}

/** Subscribe to inbound frames, unwrapping the `MessageEvent` to its `data`. */
export function onMessage<TInbound>(handler: (message: TInbound) => void): void {
  scope.onmessage = (event) => handler(event.data as TInbound)
}
