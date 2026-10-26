// Polling helper (finding #137 consolidation).
//
// The implementation now lives in `./async` with the other async primitives;
// this module is the stable import path used by existing suites. New code may
// import `pollUntil` from either module — they are the same function.

export { pollUntil, type PollUntilOptions } from "./async"
