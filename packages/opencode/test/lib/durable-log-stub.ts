// A real (but inert) `DurableLog.Service` for EventV2 layer wiring in tests that
// only publish live events and use a database stub. EventV2 requires the service;
// a no-op connection makes `workerLease`/`blobTable` false without a filesystem.

import { Layer } from "effect"
import { DurableLog } from "@opencode-ai/core/durable-log"

const inertConnection: DurableLog.Connection = {
  all: () => [],
  run: () => ({ changes: 0 }),
  exec: () => {},
}

export const durableLogStub: Layer.Layer<DurableLog.Service> = DurableLog.layer({
  connection: inertConnection,
  ownerID: "test-durable-log-stub",
})
