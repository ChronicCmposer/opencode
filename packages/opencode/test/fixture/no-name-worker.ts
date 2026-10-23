// Spawn-name probe fixture.
//
// Deliberately does NOT call `setThreadName`. It exists to prove a worker's OS
// thread `comm` is set from `WorkerOptions.name` at creation, not from any
// in-thread self-naming: this worker posts the executor `ready` frame and then
// idles, so whatever its `comm` reads afterwards must have been its birth name.

const scope = globalThis as unknown as { postMessage(message: unknown): void }

scope.postMessage({ type: "ready", threadId: 0 })
setInterval(() => {}, 1000)
