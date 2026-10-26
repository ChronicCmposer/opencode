CREATE TABLE IF NOT EXISTS tool_call (
  call_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  tool TEXT NOT NULL,
  side_effecting INTEGER NOT NULL DEFAULT 1 CHECK (side_effecting IN (0, 1)),
  state TEXT NOT NULL CHECK (state IN ('started', 'finished')),
  outcome TEXT,
  time_started INTEGER NOT NULL,
  time_finished INTEGER,
  PRIMARY KEY (call_id, session_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS tool_call_unique_call_session ON tool_call (call_id, session_id);

CREATE TABLE IF NOT EXISTS worker_crash (
  crash_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  worker_id INTEGER NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('boot', 'materialize', 'stream', 'tool', 'commit')),
  last_committed_message_id TEXT,
  in_flight_call_ids TEXT NOT NULL,
  retryable INTEGER NOT NULL DEFAULT 0 CHECK (retryable = 0),
  reason TEXT NOT NULL,
  time_created INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS worker_crash_session ON worker_crash (session_id, time_created);
