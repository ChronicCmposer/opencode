// Parity matrix registration.
//
// The harness (`test/lib/parity-matrix`) is complete; the production driver for
// every cell is not. Each cell is therefore registered explicitly: a cell with a
// driver runs, a cell waiting on an owner is a `todo` with the owner named, and
// the worker-only crash phases are on the explicit list below. No cell is
// silently omitted.

import { describe, expect, test } from "bun:test"
import {
  PARITY_MODES,
  assertNoSilentSkips,
  createParityMatrix,
  planMatrix,
  selectParityCells,
  selectionFromEnv,
  unavailable,
  workerOnlyCases,
  type CellCapability,
  type ParityCell,
} from "../../lib/parity-matrix"

const matrix = createParityMatrix()
const workerOnly = workerOnlyCases(matrix)

function capability(cell: ParityCell): CellCapability {
  if (cell.db === "file") {
    return unavailable("writer-db", "the matrix driver does not open a file-backed database yet")
  }
  if (cell.plugins === "supported") {
    return unavailable("plugin", "the matrix driver does not install a supported plugin yet")
  }
  if (cell.plugins === "unsupported") {
    return unavailable("plugin", "the matrix driver does not assert fatal unsupported-hook startup yet")
  }
  if (cell.mode === "worker") {
    return unavailable("test-infra", "the production worker matrix driver is not wired yet")
  }
  return unavailable("test-infra", "the production in-process matrix driver is not wired yet")
}

const plan = planMatrix(matrix, capability)
assertNoSilentSkips(plan, workerOnly)

describe("parity matrix cell registration", () => {
  test("every cell is run, todo, or explicitly worker-only", () => {
    expect(plan).toHaveLength(matrix.length)
    expect(plan.length).toBe(PARITY_MODES.length * 2 * 3 * 5)
    assertNoSilentSkips(plan, workerOnly)
  })

  test("the worker-only list is explicit and names the owning agent", () => {
    expect(workerOnly).toHaveLength(24)
    expect(workerOnly.every((entry) => entry.owner === "w3-crash")).toBe(true)
    expect(workerOnly.every((entry) => entry.reason.includes("worker isolate lifecycle"))).toBe(true)
    for (const entry of workerOnly) {
      expect(matrix.some((cell) => cell.id === entry.id)).toBe(true)
    }
  })

  test("the PR subset and a single cell are selectable", () => {
    expect(selectParityCells(matrix, { subset: "pr" })).toHaveLength(12)
    expect(selectParityCells(matrix, { ids: ["worker:file:supported:commit"] })).toHaveLength(1)
    expect(selectionFromEnv({ OPENCODE_PARITY_SUBSET: "pr" })).toEqual({ subset: "pr" })
  })

  for (const entry of plan) {
    test.todo(
      `${entry.cell.id} — ${entry.disposition} — owner ${entry.owner}: ${entry.reason}`,
    )
  }
})
