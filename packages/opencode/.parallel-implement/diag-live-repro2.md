# Live reproduction #2 — packaged binary, `OPENCODE_EXPERIMENTAL_SESSION_WORKERS=on`

Repo: `/var/lib/opencode/dev/opencode` · Branch: `v1` · HEAD: `4fb995ce42`
Artifact binary: `packages/opencode/dist/opencode-linux-arm64/bin/opencode`
Binary version: `0.0.0-v1-202610220038` (first build) then `1.19.3` (rebuilt for zen, see note)
Env: `PATH=$HOME/.bun/bin:$PATH`, bun 1.3.14. Raw logs: `/tmp/opencode/repro2-*.log` and `/tmp/opencode/repro2/result-*.json(.server.log)`.

## Verdict

- **(a) gibberish / degenerate model loop — NOT reproduced.** With the packaged binary, flag ON, a real model (`opencode/deepseek-v4.1-flash`), across enumeration, multi-turn, concurrent and tool-call runs, output was clean: no repeated tokens/phrases, no runaway length, no degenerate loop. Streamed `message.part.delta` frames reconstruct the final text exactly (no duplication). The mock provider is a scripted echo and cannot produce gibberish; it showed only correct streaming. Caveat: the model that most plausibly produced the symptom — the user's local `llm` provider (`glm-4.7-flash` with `interleaved: "reasoning_content"`, `qwen3-coder-30b-a3b`, `north-mini-code-1.0` at `https://llm/v1`) — is **unreachable from this host** (TCP 443 to `llm` times out), so it was not tested.
- **(b) termination / lifecycle failure — REPRODUCED, flag-ON-specific.** Abort itself works cleanly. The defect is on **normal completion → next run in the same session**: the session's writer/DB stays locked past `message.completed`, so a prompt issued in that window fails with `SQLiteError: database is locked` (`LockTimeoutError`). `POST /prompt_async` still returns **204**, the run dies silently (no assistant text, no client-visible error; error appears only in the server log). Reproduced **4/4** with a long first prompt and **1/3** with a short first prompt; **0 occurrences with the flag OFF** (immediate reuse, 0 locks).

## Build / environment notes

- Built per instruction: `cd packages/opencode && bun run script/build.ts --single --skip-install`. Verified `--version`, smoke gate passed.
- **Zen version gate:** the dev binary (`0.0.0-v1-*`) is rejected by the zen gateway ("OpenCode 1.18.0 or newer is required"). Rebuilt once with `OPENCODE_VERSION=1.19.3` (build-time env only, no tracked-source change) so real zen models work. Smoke gate still passed (`Smoke test passed: 1.19.3`).
- Isolated data dir per run (never touched the real `:4096` DB): separate `XDG_DATA_HOME`/`OPENCODE_DB` under `/tmp/opencode/repro2/home-<tag>`; `auth.json` copied in for real runs. `OPENCODE_CONFIG_CONTENT` carries the provider config. `OPENCODE_PURE=1`, project config disabled.

## (a) gibberish — evidence

Real model `opencode/deepseek-v4.1-flash`, flag ON, prompts: enumerate 1..400, multi-turn (5 turns), 4 concurrent sessions (`TOKEN-<i>`), tool-call task, 3× enumeration.

| scenario | result |
|---|---|
| enumeration 1..400 (ON, 3 runs) | `chars=1491/1491/1890, maxRepeat=0, charRun=0, errors=[]` (1491 = comma list w/o spaces, so `words=1`; 1890 = spaces) |
| enumeration (OFF, 3 runs) | identical distribution, `maxRepeat=0` |
| concurrent 4 sessions (ON) | each returned its own exact token (`TOKEN-0..3`); no cross-talk; ~4.3 s |
| multi-turn (ON, after idle-corrected wait) | turn responses correct: `"Got it, Zephyr."` → `"Your name is Zephyr."`; hammer 12 turns `N0..N11` all `ok=true` |
| tool-call (ON & OFF) | both: `step-start, tool:bash, step-finish, step-start, text, step-finish`, final text `DONE` |
| delta reconstruction (ON, real) | `message.part.delta` sequence == final assistant text exactly (1890/1890); a second 156-char part is a separate (reasoning/step) part, no duplicate content |

Mock provider (`test/test-model`, chunks `["Hel","lo"," wor","ld"]`): final text `Hello world`, 4 delta frames reconstruct to `Hello world` exactly.

So the gibberish symptom did not appear on the packaged flag-ON path with a real zen model. It is **not** an artifact of duplicate/looped delta streaming. Not tested: the local `llm` endpoint models.

## (b) termination / lifecycle — evidence

### Abort works
`POST /session/:id/abort` mid-run (`LONG` essay, abort ~4–5 s in):

```json
{"sessionID":"ses_...","mode":"cooperative","winner":"cancel","acknowledged":true,"durationMs":31..105}
```

- generation stops; assistant message carries `MessageAbortedError`/`Aborted`; session goes idle;
- a subsequent run in a **new** session succeeds (`hello world`);
- reusing the **same** session after abort also succeeded (`hello world` in ~2.9 s);
- no leftover processes; worker-pool threads stable (baseline ~15 → ~27–31 after pool spawn, unchanged after abort/second run; workers are in-process **threads**, no child PIDs; driver-killed server leaves nothing behind).

`normal completion → cancel a later run` (flag ON): normal run `hello world`; later long run aborted `winner=cancel acknowledged=true`; second run `hello world`. No failure.

### The actual failure: same-session reuse right after completion

Sequence that reproduces it (this is exactly what a UI does when the user sends the next message as soon as the response finishes):

1. `prompt_async` a run; poll `GET /session/:id/message` until the assistant message has `time.completed`;
2. **immediately** `prompt_async` again in the **same** session;
3. observe.

`race` scenario, real model, `RUNS=3`, comparing flag ON vs OFF:

```
ON  [race 0] ps=204 delayMs=10    gotSecond=false
    [race 1] ps=204 delayMs=9     gotSecond=true
    [race 2] ps=204 delayMs=6284  gotSecond=false   <- blocked ~6.3s
    server: "database is locked" x1, "prompt_async failed" x1

OFF [race 0] ps=204 delayMs=13 gotSecond=true
    [race 1] ps=204 delayMs=7  gotSecond=true
    [race 2] ps=204 delayMs=16 gotSecond=true
    server: 0 locks, 0 failures
```

With a longer first prompt (`LONG` essay), flag ON failed **4/4**:

```
ON [race 0..3] delayMs≈5101..5130 gotSecond=false  (all four)
server log (x4):
level=ERROR message="prompt_async failed" sessionID=ses_...
  cause="Cause([Die(effect/sql/SqlError: Failed to execute statement
   (cause: effect/sql/SqlError/LockTimeoutError: Failed to execute statement
   (cause: SQLiteError: database is locked)))])"
```

Earlier occurrence (first observed, `multiturn`, flag ON): turn 0 completed in ~3 s, turn 1 stalled the full 60 s wait with **0 chars**; identical `LockTimeoutError`. Flag OFF same test: turn 1 completed in ~1.1 s with `"Your name is Zephyr."` (see `/tmp/opencode/repro2-run-off-twoturn.log`).

Key properties of the failure:
- `prompt_async` may block seconds in the request (ON: ~5–6.3 s; OFF: ~10 ms) and **still return 204**;
- the scheduled run then dies: no assistant text, no `session.error` surfaced to the client (only server-side `prompt_async failed`);
- it is intermittent for short first runs (1/3), near-deterministic for long first runs (4/4);
- flag OFF never reproduces it.

Interpretation: the flag-ON worker/writer path leaves a **pinned writer transaction / DB write lock** alive for a window that extends **past** `message.completed` (until the run fully finalizes / the writer releases). A next prompt landing in that window times out (`LockTimeoutError`, ~5 s) and its run is dropped. The message-completion signal is therefore not a safe "session free" signal under flag ON. (The `tail` probe measured the *idle* gap as only ~32–48 ms ON vs 0 ms OFF, so the vulnerable window is the run-finalization window in which `message.completed` fires before the run truly exits — not the idle gap itself.)

## (c) Commands

```bash
export PATH="$HOME/.bun/bin:$PATH"
REPO=/var/lib/opencode/dev/opencode
BIN=$REPO/packages/opencode/dist/opencode-linux-arm64/bin/opencode

# build (version-gate rebuild for real zen models only):
cd $REPO/packages/opencode && bun run script/build.ts --single --skip-install
# OPENCODE_VERSION=1.19.3 bun run script/build.ts --single --skip-install

# driver (writes /tmp/opencode/repro2/result-<TAG>.json[.server.log]):
export REPRO_DIR=/tmp/opencode/repro2 BIN=$BIN
export MODEL=opencode/deepseek-v4.1-flash MODEL_KIND=real

# (a) gibberish
PORT=4240 FLAG=on  SCENARIO=gibberish RUNS=3 TAG=on-gibb  bun run /tmp/opencode/repro2/drive.ts
PORT=4241 FLAG=off SCENARIO=gibberish RUNS=3 TAG=off-gibb bun run /tmp/opencode/repro2/drive.ts
PORT=4105 FLAG=on  SCENARIO=concurrent RUNS=4 TAG=on-concurrent bun run /tmp/opencode/repro2/drive.ts
PORT=4140 FLAG=on  SCENARIO=tools TAG=on-tools4 bun run /tmp/opencode/repro2/drive.ts

# (b) termination
PORT=4103 FLAG=on  SCENARIO=abort TAG=on-abort bun run /tmp/opencode/repro2/drive.ts
PORT=4200 FLAG=on  SCENARIO=normal-then-cancel TAG=on-ntc bun run /tmp/opencode/repro2/drive.ts
PORT=4230 FLAG=on  SCENARIO=race RUNS=3 TAG=on-race3 bun run /tmp/opencode/repro2/drive.ts
PORT=4231 FLAG=off SCENARIO=race RUNS=3 TAG=off-race3 bun run /tmp/opencode/repro2/drive.ts

# manual smoke (isolated server on :4098) — env in /tmp/opencode/repro2/env.sh
source /tmp/opencode/repro2/env.sh
$BIN serve --hostname 127.0.0.1 --port 4098 --print-logs --log-level DEBUG
```

Driver: `/tmp/opencode/repro2/drive.ts` (spawns the packaged binary, subscribes `/global/event`, creates sessions, prompts via `/prompt_async`, waits for `session.idle` events, extracts assistant text, detects repeats, aborts, records `/proc/<pid>/status` threads + children).

## (d) Cleanup

- No repro servers left running (`pgrep -af "port 41[0-9][0-9]"` → none); only the pre-existing production server on `:4096` remains.
- Per-run isolated homes `/tmp/opencode/repro2/home-*` removed (driver `rmSync`s them; all cleared by hand too). Real DB at `/var/lib/opencode/.local/share/opencode/opencode.db` never touched.
- `/tmp/opencode/repro2-*.log` and `/tmp/opencode/repro2/result-*.json(.server.log)` retained as evidence. `events.json` dumps deleted (large).
- No tracked source modified. `git status` should show only `.parallel-implement/` (this artifact) untracked.

## Residual uncertainty

- Could not test the user's actual local `llm` models (`glm-4.7-flash` etc.) — endpoint down — so (a) is "not reproduced on the available real model + mock", not "cannot happen".
- The (b) lock window length varied (short run → intermittent; long run → near-certain); exact trigger is the prompt landing between `message.completed` and run finalization. Under flag OFF the same sequence never locked.
