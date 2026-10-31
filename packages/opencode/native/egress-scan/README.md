# egress-scan

A single-pass, allocation-free, strict RFC 8259 validating scanner for events-port byte frames, exposed as a Node-API addon. It replaces JSON parsing plus schema validation on the hub main thread. The JS decoder in `src/session/egress/scan.ts` accepts and rejects the same inputs.

## Build

- Dev and tests: `bun run script/build-egress-scan.ts` writes `build/egress-scan.node` for the current host (`clang -shared -undefined dynamic_lookup` on macOS). `loadNativeEgressScan()` picks it up from there; with no file present the JS decoder runs.
- Release: `script/build.ts` compiles it with the system C compiler (`cc`, `clang` or `gcc`; `-O3 -fPIC -shared`) when the build host is linux arm64 and the target is the glibc linux arm64 binary, then embeds it. A missing compiler fails the build. Every other target skips it.
- `OPENCODE_EGRESS_SCAN=js` forces the JS decoder.

`include/` vendors the Node-API headers (MIT, from Node.js). The addon is context-aware (`NAPI_MODULE_INIT`) and `scan.c` keeps no globals, so it is safe in worker threads.

## Contract

`scan(bytes: Uint8Array)` scans the buffer in place and returns the `NativeScanResult` of `src/session/egress/native-scan.ts`: `{ ok: true, kind, ... }` or `{ ok: false, code, offset, message, key? }`. A non-`Uint8Array` argument throws `TypeError`. The properties object is never built; the caller gets its byte range.

Frame: `[u32 LE envelopeLength][envelope JSON][properties JSON]`. Limits: 4 MiB total (`too-large`), nesting depth 512 (`too-deep`), 65536 bytes per object key (`key-too-long`). Both segments must be strict JSON: no BOM, no trailing data, strict UTF-8 (no overlongs, UTF-8-encoded surrogates or code points above U+10FFFF; `invalid-utf8`), strict number grammar; lone surrogate `\u` escapes are accepted as in `JSON.parse`.

Envelope (`bad-envelope`, or `unsupported-version` when `v` is not 1): `port` is `"events"`; `id` a non-negative integer; the last duplicate key wins and unknown keys are ignored.

- `event`: `payload.event.id` and `.type` strings; optional integer `generation` and `ordinal`; `aggregateID` (non-empty string) and `seq` (non-negative integer) both or neither. Then a non-empty properties segment follows.
- `end`: no payload. `resync`: optional `pattern`, `aggregateID`, `seq`, `inclusive`. `ack`: optional non-negative integer `count`. These have no trailing bytes.

Properties: any valid JSON value. A key spelled `__proto__`, `constructor` or `prototype` (compared after unescaping) at any depth is `dangerous-key`, with the key and its offset. `isBlobReference` follows `isEventBlobReference` in `worker/frame.ts`: `__eventBlobPosition` is an object with a string `aggregateID` and number `seq`, and `frameBytes` is a number; other keys are allowed. Top-level string `sessionID` and `aggregateID` are returned.
