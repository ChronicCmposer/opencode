// A broker-worker entry that loads but never completes the port-pairing
// handshake: it never posts `{ type: "ready" }`, so the hub's transport never
// becomes live.
//
// Used by `broker.test.ts` to prove `request()` cannot hang forever waiting for
// a transport that an incapable or dead worker will never provide (the
// crash-before-ready deadlock): the per-request timeout must still fire.

setInterval(() => {}, 1 << 30)
