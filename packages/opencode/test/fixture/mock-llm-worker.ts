// Phase 3.5 worker entry point (plan task 3.5).
//
// Dependency-free: it imports only the worker-side SSE consumer, which uses
// `fetch` and a text decoder — no Effect, no model SDK. The hub delivers a
// typed bootstrap payload over the `init` message (Bun has no `workerData`);
// this worker replies `ready`, then answers each `chat` with plain JSON.

import { installMockLlmWorker, type MockLlmWorkerScope } from "../lib/worker-mock-llm"

const scope = globalThis as unknown as MockLlmWorkerScope

installMockLlmWorker(scope)
