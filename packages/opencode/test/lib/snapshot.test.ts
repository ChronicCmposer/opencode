// Lock the `workerSnapshot` fixture to the `WorkerSnapshot` contract.
//
// `WorkerSnapshot` carried a `workerCount` diagnostic until the layer graph
// removed it (the hub is the only place a worker count exists). The fixture
// must not resurrect the dead field, and an override that names it must be a
// type error rather than silently riding along in the spread.

import { describe, expect, test } from "bun:test"
import type { WorkerSnapshot } from "@/session/worker/layer"
import { workerSnapshot } from "./snapshot"

describe("workerSnapshot fixture", () => {
  test("omits the removed workerCount field", () => {
    const snapshot: WorkerSnapshot = workerSnapshot()

    expect("workerCount" in snapshot).toBe(false)
  })

  test("rejects a workerCount override at the type boundary", () => {
    // @ts-expect-error `workerCount` is not part of WorkerSnapshot
    const snapshot: WorkerSnapshot = workerSnapshot({ workerCount: 2 })

    expect(snapshot.epoch).toBe(1)
  })
})
