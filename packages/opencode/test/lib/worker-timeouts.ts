// Timeout budgets for tests that drive REAL session-worker isolates.
//
// A worker boot, one model turn or a crash round-trip is normally sub-second,
// but the suite may run while the host is saturated (other builds, editors, CI
// shards). Bun's raw `bun test` runner defaults to a 5s per-test budget and the
// real-worker fixtures originally used 3s-10s fail-fast values; under load those
// fired even though every assertion still held. These values are generous upper
// bounds, not expectations: a healthy run finishes far below them, while a
// genuinely wedged worker still fails.
//
// Keep these in the test tree only. Production defaults (30s transport, 60s run
// idle) are unchanged.

/** Per-test budget used when a test does not pass its own timeout. */
export const DEFAULT_TEST_TIMEOUT_MS = 60_000

/** Hub/executor transport timeout; also the worker boot deadline in tests. */
export const REAL_WORKER_TRANSPORT_TIMEOUT_MS = 60_000

/** Run-reply watchdog handed to a real-worker hub in tests. */
export const REAL_WORKER_IDLE_TIMEOUT_MS = 60_000

/** Explicit per-test budget for cases that boot several real workers. */
export const REAL_WORKER_TEST_TIMEOUT_MS = 120_000

/** Explicit per-test budget for a case whose real shell/worker teardown can stall. */
export const REAL_WORKER_SLOW_TEST_TIMEOUT_MS = 180_000
