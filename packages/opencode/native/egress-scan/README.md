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

## Rules

The JS decoder, the C scanner and the fuzz oracle (`test/session/egress/scan-fuzz.test.ts`) implement exactly these; `test/session/egress/scan-corpus.ts` is the shared table both decoders run.

| Topic | Rule |
| --- | --- |
| Envelope shape | Must be an object (`bad-envelope`). `v` must be the number 1, a missing or other `v` is `unsupported-version`. `port` is `"events"`, `kind` one of `event`, `end`, `resync`, `ack` (`bad-envelope`). `payload`, when present, is an object. |
| Integers | `id`, `generation`, `ordinal`, `count`: safe integers (`id`, `count` also >= 0), as `Schema.Int`. `seq`: any integer >= 0. `aggregateID`: non-empty string. `inclusive`: boolean. A malformed field is `bad-envelope` where the kind reads it; other kinds ignore it. |
| `event` | `payload.event.id` and `.type` strings; `aggregateID` and `seq` both or neither. A non-empty properties segment follows. |
| `end` | No `payload` at all (not even `{}`) and no trailing bytes. |
| `resync` / `ack` | Optional payload; an empty payload object decodes as no payload. Same field rules; no trailing bytes (`bad-envelope`, also for any non-event envelope followed by data). |
| Duplicate keys | The last wins everywhere, as `JSON.parse`; unknown keys are ignored. |
| Dangerous keys | `__proto__`, `constructor`, `prototype`, compared after unescaping, at any depth, in the envelope and in properties (`dangerous-key`). |
| Depth | The root container is depth 1 in each segment; more than 512 is `too-deep`. |
| Key length | Measured in decoded UTF-8 bytes (an escaped lone surrogate counts 3); more than 65536 is `key-too-long`. |
| Strings | Lone surrogate `\u` escapes are accepted, as `JSON.parse`. A leading BOM is `invalid-json`. |
| Blob reference | `__eventBlobPosition` object with string `aggregateID` and number `seq`, and number `frameBytes`, taking the last duplicate. |
| Precedence | Per segment, the first stage that fails wins: invalid UTF-8 anywhere, then JSON syntax (including trailing data), then limits (`too-deep`, `dangerous-key`, `key-too-long`, strongest first). The envelope is checked before its structure, which comes before properties. The C scanner keeps scanning past a limit violation, using a heap bitmap beyond depth 512, to honour this. The one known gap: JS reports `too-deep` for input nested beyond what `JSON.parse` can parse, whatever follows. |
