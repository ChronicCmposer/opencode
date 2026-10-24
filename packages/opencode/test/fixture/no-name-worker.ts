// Spawn-name probe fixture.
//
// Deliberately does NOT call `setThreadName`. It exists to prove a worker's OS
// thread `comm` is set from `WorkerOptions.name` at creation, not from any
// in-thread self-naming: this worker posts the executor `ready` frame and then
// idles, so whatever its `comm` reads afterwards must have been its birth name.

import { postToParent } from "./worker-scope"

postToParent({ type: "ready", threadId: 0 })

// Keep the worker alive so the test can read its OS thread `comm` after `ready`;
// teardown is the only thing that stops it (same keep-alive idiom as
// `mcp-lsp-descendants.ts`).
setInterval(() => {}, 1 << 30)
