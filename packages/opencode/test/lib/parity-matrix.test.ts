import { describe, expect, test } from "bun:test"
import { Effect, Layer, ManagedRuntime, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import {
  PARITY_CRASHES,
  PARITY_DATABASES,
  PARITY_MODES,
  PARITY_PLUGINS,
  assertCellParity,
  assertNoSilentSkips,
  assertWellFormedMatrix,
  available,
  cellID,
  createParityMatrix,
  parityPair,
  parseCellID,
  planMatrix,
  runMatrixCell,
  selectParityCells,
  selectionFromEnv,
  statusEvents,
  unavailable,
  workerOnlyCases,
  type MatrixFixture,
} from "./parity-matrix"
import type { ParityRunContext, ParityRunner } from "./parity-session"
import { createEventSink, type SinkEvent } from "./event-sink"

const LiveDelta = EventV2.define({
  type: "message.part.delta",
  schema: { delta: Schema.String },
})

const databaseStub = Layer.succeed(Database.Service, { db: {} as never })
const eventLayer = EventV2.layerWith().pipe(Layer.provide(databaseStub))

const script = { name: "matrix-synthetic", prompt: "hello" }

/** Publish deltas through a sink bound to a fresh runtime, exactly once each. */
async function runDeltas(context: ParityRunContext, deltas: ReadonlyArray<string>): Promise<void> {
  const runtime = ManagedRuntime.make(eventLayer)
  try {
    context.capture(runtime)
    for (const delta of deltas) {
      await runtime.runPromise(EventV2.Service.use((events) => events.publish(LiveDelta, { delta })))
    }
  } finally {
    await runtime.dispose()
  }
}

interface FixtureSides {
  readonly off: ReadonlyArray<string>
  readonly on: ReadonlyArray<string>
  readonly durableOnWorker?: boolean
}

/** A deterministic fixture: each side streams its own script and optionally records a row. */
function deltaFixture(sides: FixtureSides): MatrixFixture {
  const run: ParityRunner = async (context) => {
    await runDeltas(context, context.flag ? sides.on : sides.off)
    if (context.flag && sides.durableOnWorker !== false) {
      context.durable.add("message", [
        { id: `msg_${context.flag ? "on" : "off"}`, time: context.flag ? 99 : 1, text: "hello" },
      ])
    }
    if (!context.flag) {
      context.durable.add("message", [{ id: "msg_off", time: 1, text: "hello" }])
    }
  }
  return { script, run: () => run }
}

describe("parity matrix dimensions and selection", () => {
  test("the full matrix is 2 x 2 x 3 x 5, unique, and self-consistent", () => {
    const matrix = createParityMatrix()
    expect(matrix).toHaveLength(PARITY_MODES.length * PARITY_DATABASES.length * PARITY_PLUGINS.length * PARITY_CRASHES.length)
    expect(new Set(matrix.map((cell) => cell.id)).size).toBe(matrix.length)
    assertWellFormedMatrix(matrix)
  })

  test("cell ids round-trip and malformed ids fail loud", () => {
    const cell = parseCellID(cellID("worker", "file", "supported", "tool"))
    expect(cell).toMatchObject({ mode: "worker", db: "file", plugins: "supported", crash: "tool" })
    expect(cell.id).toBe("worker:file:supported:tool")

    expect(() => parseCellID("worker:file:supported")).toThrow(/expected mode:db:plugins:crash/)
    expect(() => parseCellID("worker:file:nope:none")).toThrow(/invalid parity plugins/)
  })

  test("the PR subset keeps the happy path of every other dimension", () => {
    const pr = selectParityCells(createParityMatrix(), { subset: "pr" })
    expect(pr).toHaveLength(PARITY_MODES.length * PARITY_DATABASES.length * PARITY_PLUGINS.length)
    expect(pr.every((cell) => cell.crash === "none")).toBe(true)
  })

  test("every cell resolves to the in-process/worker pair that shares its coordinates", () => {
    const pair = parityPair(parseCellID("worker:file:supported:tool"))
    expect(pair.inProcess.id).toBe("in-process:file:supported:tool")
    expect(pair.worker.id).toBe("worker:file:supported:tool")
  })

  test("a cell is selectable by every dimension", () => {
    const selected = selectParityCells(createParityMatrix(), {
      modes: ["worker"],
      databases: ["file"],
      plugins: ["unsupported"],
      crashes: ["commit"],
    })
    expect(selected.map((cell) => cell.id)).toEqual(["worker:file:unsupported:commit"])
  })

  test("an empty selection fails loud instead of silently passing", () => {
    expect(() => selectParityCells(createParityMatrix(), { ids: [] })).toThrow(/matched no cells/)
    expect(() => selectParityCells(createParityMatrix(), { modes: ["worker"], crashes: [] })).toThrow(/matched no cells/)
  })

  test("selection reads the cell environment without accepting a third subset", () => {
    expect(selectionFromEnv({ OPENCODE_PARITY_SUBSET: "pr" })).toEqual({ subset: "pr" })
    expect(selectionFromEnv({ OPENCODE_PARITY_CELLS: "worker:memory:none:none" })).toEqual({
      ids: ["worker:memory:none:none"],
    })
    expect(() => selectionFromEnv({ OPENCODE_PARITY_SUBSET: "nightly" })).toThrow(/expected "pr" or "full"/)
  })

  test("a duplicate or self-inconsistent matrix entry fails loud", () => {
    const cell = parseCellID("worker:memory:none:none")
    expect(() => assertWellFormedMatrix([cell, cell])).toThrow(/duplicate parity cell/)
    expect(() => assertWellFormedMatrix([{ ...cell, id: "worker:file:none:none" }])).toThrow(
      /does not match its fields/,
    )
  })
})

describe("parity matrix planning never skips silently", () => {
  test("available cells run and crash-phase in-process cells are explicitly worker-only", () => {
    const matrix = createParityMatrix()
    const plan = planMatrix(matrix, () => available)
    expect(plan).toHaveLength(matrix.length)
    const crashNoneCells = PARITY_MODES.length * PARITY_DATABASES.length * PARITY_PLUGINS.length
    const workerCrashCells = PARITY_DATABASES.length * PARITY_PLUGINS.length * (PARITY_CRASHES.length - 1)
    expect(plan.filter((entry) => entry.disposition === "run")).toHaveLength(crashNoneCells + workerCrashCells)
    expect(plan.filter((entry) => entry.disposition === "worker-only")).toHaveLength(workerCrashCells)
    assertNoSilentSkips(plan, workerOnlyCases(matrix))
  })

  test("a worker-only cell that is not on the explicit list is rejected", () => {
    const matrix = createParityMatrix()
    const plan = planMatrix(matrix, () => available)
    expect(() => assertNoSilentSkips(plan, [])).toThrow(/worker-only but not on the explicit list/)
  })

  test("unavailable cells become todos with a named owner and reason", () => {
    const matrix = createParityMatrix()
    const plan = planMatrix(matrix, () => unavailable("session-root", "production driver not landed"))
    const todos = plan.filter((entry) => entry.disposition === "todo")
    expect(todos).toHaveLength(36)
    expect(todos.every((entry) => entry.owner === "session-root")).toBe(true)
    assertNoSilentSkips(plan, workerOnlyCases(matrix))
  })

  test("an unavailable cell without an owner or reason is rejected at construction", () => {
    expect(() => unavailable("", "reason")).toThrow(/needs a named owner/)
    expect(() => unavailable("plugin", "  ")).toThrow(/needs a reason/)
  })
})

describe("parity matrix comparison", () => {
  test("identical event and durable sequences compare equal after normalization", async () => {
    const cell = parseCellID("in-process:memory:none:none")
    const result = await runMatrixCell(
      cell,
      deltaFixture({
        off: ["a", "b"],
        on: ["a", "b"],
      }),
    )

    expect(result.parity.off.error).toBeUndefined()
    expect(result.parity.on.error).toBeUndefined()
    expect(result.events.equal).toBe(true)
    expect(result.statuses.equal).toBe(true)
    expect(result.durable.equal).toBe(true)
    expect(result.equal).toBe(true)
    expect(assertCellParity(result)).toBe(result)
  })

  test("a missing worker event is reported, not swallowed", async () => {
    const cell = parseCellID("worker:memory:none:none")
    const result = await runMatrixCell(
      cell,
      deltaFixture({
        off: ["a", "b"],
        on: ["a"],
      }),
    )

    expect(result.equal).toBe(false)
    expect(result.events.missingInOn.map((event) => event.type)).toEqual(["message.part.delta"])
    expect(() => assertCellParity(result)).toThrow(/event sequence diverged/)
  })

  test("durable rows ignore generated ids and clocks but keep behavioral fields", async () => {
    const off = [{ table: "message", row: { id: "msg_off", time: 1, text: "hello" } }]
    const on = [{ table: "message", row: { id: "msg_worker_9", time: 99, text: "hello" } }]
    const mismatch = [{ table: "message", row: { id: "msg_worker_9", time: 99, text: "different" } }]
    const durableFixture = (workerRows: typeof on): MatrixFixture => ({
      script,
      run: () => (context) => {
        const rows = context.flag ? workerRows : off
        for (const entry of rows) context.durable.add(entry.table, [entry.row])
      },
    })

    const same = await runMatrixCell(parseCellID("worker:memory:none:none"), durableFixture(on))
    const different = await runMatrixCell(parseCellID("worker:memory:none:none"), durableFixture(mismatch))

    expect(same.durable.equal).toBe(true)
    expect(same.equal).toBe(true)
    expect(different.durable.equal).toBe(false)
    expect(different.equal).toBe(false)
  })

  test("status events are compared as their own ordered subsequence", () => {
    const events: ReadonlyArray<SinkEvent> = [
      { id: "evt_1", type: "message.part.delta", properties: { delta: "a" } },
      { id: "evt_2", type: "session.status", properties: { status: { type: "busy" } } },
      { id: "evt_3", type: "session.idle", properties: {} },
    ]
    expect(statusEvents(events).map((event) => event.type)).toEqual(["session.status", "session.idle"])
  })
})
