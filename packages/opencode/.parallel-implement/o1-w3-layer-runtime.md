# o1-w3 — layer-runtime.test.ts to the writer-backed Drizzle reality

## Scope
Only edit `packages/opencode/test/session/worker/layer-runtime.test.ts`.
No git commits.

## Recon (done)
- `rg -n "WorkerDatabaseHandle" packages/opencode` → 3 refs, ALL inside
  `test/session/worker/layer-runtime.test.ts` (lines 31, 59, 88). No other file
  imports it. So only this file needs updating for compilation.
- New reality (`src/session/worker/layer.ts`):
  - `writerDatabaseLayer(writer: WriterClientHandle): Layer.Layer<Database.Service>`
    builds a REAL Drizzle `Database.Interface` via
    `EffectDrizzleSqlite.makeWithDefaults()` + `writerSqlClientLayer(writerSqlHandle(writer))`.
  - `Database.Interface.db` is `EffectSQLiteDatabase` — `select/insert/update/delete/transaction`
    return Effects.
  - `WriterClientHandle` = `{ query, publish }` + optional `begin/txQuery/commit/rollback`.
  - `boundaryReplacements(snapshot, writer?)` uses `writerDatabaseLayer` when a writer is
    supplied, else `databaseBoundaryLayer()` (loud throw).
- Superseding coverage: `layer.test.ts` already has a thorough "o1-w2" section
  (CRUD round-trip, transaction atomicity, no-cast typecheck, missing-primitive
  failure, runnable-graph `Session.get` + durable `EventV2.publish`).

## Decision
KEEP the file (do NOT remove). `layer.test.ts` does NOT cover steps 24 (hub-proxy
authorities), 25 (reduced in-worker Provider), or 26 (LLM.stream ModelClient) —
those live only here. Only step 23 was obsolete, so rewrite just that section to
the writer-backed Drizzle surface.

## Plan
- Replace step-23 raw-SQL assertions with:
  1. CRUD round-trip through `writerDatabaseLayer` over a real `bun:sqlite` writer.
  2. `boundaryReplacements(snapshot, writer)` installs a working writer proxy
     (Drizzle CRUD) instead of the throwing stub.
- Swap `WorkerDatabaseHandle` import for `WriterClientHandle`.
- Keep steps 24/25/26 untouched.

## Progress log
- [x] Recon
- [x] Implement step 23 rewrite
- [x] bun test test/session/worker/layer-runtime.test.ts
- [x] bun run typecheck (package + repo-wide)
- [x] rg WorkerDatabaseHandle == empty

## Results
- File KEPT, step 23 rewritten. Steps 24/25/26 untouched.
- Imports: added `Database as BunDatabase` (bun:sqlite), `eq, sql` (drizzle-orm),
  `integer, sqliteTable, text` (drizzle-orm/sqlite-core); swapped
  `WorkerDatabaseHandle` → `WriterClientHandle`.
- Step 23 tests (2):
  1. "yields a real Drizzle Database that round-trips CRUD through the writer"
  2. "boundaryReplacements installs the writer proxy, not the throwing stub"
- `bun test test/session/worker/layer-runtime.test.ts`:
  **10 pass, 0 fail, 15 expect() calls** (file total; 2 of them the rewritten step 23).
- `bun run typecheck` in packages/opencode: 0 errors.
- Repo-wide `bun run typecheck` (turbo): **30 successful, 30 total**.
- `rg -n "WorkerDatabaseHandle" packages/opencode`: NO REFERENCES.

## Blockers
None.

## Notes for orchestrator
- Initial typecheck caught `native.run("COMMIT")` returning `Changes` (not `void`);
  wrapped commit/rollback bodies in blocks. Fixed.
- No git commits made. Only `test/session/worker/layer-runtime.test.ts` edited
  (plus this progress file under `.parallel-implement/`).
