# W1: Reduced worker provider consumes the real model descriptor

Phases 2–3: stop synthesizing a stub model in the worker; consume the REAL
serialized descriptor so worker LLM request params equal flag-off.

## Ownership (strict)
- `packages/opencode/src/session/worker/layer.ts`
- `packages/opencode/test/session/worker/layer.test.ts`
- `packages/opencode/test/session/worker/layer-runtime.test.ts`

## Checklist
- [x] Recon: layer.ts reduced provider/sdk/plugin; init.ts; session-worker.ts; transform/request
- [x] RED: failing parity tests (layer.test.ts + layer-runtime.test.ts)
- [x] GREEN 1: `reducedProviderInfo` reads descriptor `providerOptions` (drop `options:{}`)
- [x] GREEN 1: `reducedProviderModel` reads descriptor capabilities/limit/options/variants (drop hardcoded temperature:true/reasoning:false/interleaved:false/limit.output:4096/options:{})
- [x] GREEN 3: `createWorkerLanguageSdk` preserves providerOptions + includeUsage + timeout fetch + headers
- [x] GREEN 4: `reducedLlmLayer` applies resolved plugin effect (injectable WorkerPluginHost)
- [x] Verify: `bun test test/session/worker/layer.test.ts test/session/worker/layer-runtime.test.ts` green
- [x] Verify: `bun run typecheck` 0 errors in owned files
- [x] Report

## Findings
- Frozen descriptor `SessionWorkerModel` (sibling, session-worker.ts): identity +
  `capabilities{temperature,reasoning,interleaved?,toolcall?,attachment?,[k]}` +
  `limit{context,output}` + `options?` + `variants?` + `providerOptions?`.
- `WorkerModelConfig` in layer.ts is the local consumer type; new fields are
  OPTIONAL so non-owned fixtures (layer-session/parity-harness/session-worker tests)
  keep compiling. Descriptor values win when present.
- Temperature gate: `session/llm/request.ts:124-126` → `provider/transform.ts:530-547`
  forces 1.0 for glm/minimax/kimi/gemini when `capabilities.temperature` is true.
  Descriptor `temperature:false` must yield `temperature: undefined`.
- `maxOutputTokens` = `min(limit.output, outputTokenMax)` (`transform.ts:1481`).
- Plugin hooks: worker-hosted `WorkerPluginHost` (`plugin-host.ts`) is the seam;
  `reducedLlmLayer` replaces Plugin with a no-op today (`layer.ts:1147`).

## Results
- DONE. `layer.ts` now consumes the real descriptor.
- Tests (2 owned files): `layer.test.ts` 38 pass / 0 fail;
  `layer-runtime.test.ts` 14 pass / 0 fail. Combined 52 pass / 0 fail.
- New parity tests: descriptor capabilities/limit/options/variants/providerOptions
  carried; `temperature` OMITTED for a `temperature:false` glm-4.6 descriptor
  (would otherwise be 1.0); `limit.output` used (not 4096); worker-hosted plugin
  `chat.params` temperature applied to the outgoing request.
- Typecheck: 0 errors in the three owned files.
- No regressions: `layer-session.test.ts` + `session-worker.test.ts` 38 pass,
  `parity-harness.test.ts` 15 pass.
- BLOCKER (sibling-owned, not edited): `SessionWorkerModel` reshape broke
  `test/lib/session-worker-init.ts` (line 22 `model`), `test/lib/session-worker-init.test.ts`
  (lines 30, 35), `test/session/worker/pool.test.ts` (lines 277, 359) — 5
  typecheck errors + 2 runtime failures in `pool.test.ts` about the old `model`
  field vs new `modelID`/`providerOptions` shape.
- FOLLOW-UP (sibling-owned): production `session-worker.ts:475` calls
  `reducedLlmLayer(init.snapshot, config)` with no `WorkerPluginHost`, so the
  degraded `ModelClient` path still uses the no-op plugin. Pass a host (or run
  the full graph's real Plugin node) to activate plugin `chat.params`/`chat.headers`
  there. The seam + test are in place.

