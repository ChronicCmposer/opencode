import { describe, expect, test } from "bun:test"

import { SchemaMismatch, LiveWorkersError, MissingQueryInTransactionError } from "@/session/db/schema"
import { TableAllowlistDriftError } from "@/session/db/ownership"
import { CoalescerEndedError } from "@/session/egress/coalesce"
import { DurableOrderError } from "@/session/egress/receiver"
import { WorkerCrashRecordError } from "@/session/ledger/crash"
import { LifecycleRpcTimeoutError } from "@/session/ledger/lifecycle"
import {
  StaleWorkerGenerationError,
  ToolCallAlreadyStartedError,
  ToolCallLedgerError,
  ToolCallNotStartedError,
} from "@/session/ledger/tool-call"
import { SessionExecutorRequestError, SessionWorkerReplyTimeoutError } from "@/session/worker/execute"
import { FrameTooLargeError, ProtocolError, UnsupportedFrameVersionError } from "@/session/worker/frame"
import { ForeignLifecycleReportError } from "@/session/worker/hub"
import { InvalidPluginToolArgsError, UnknownPluginToolError } from "@/session/worker/hub-proxy-authorities"
import { HubProxyRemoteError } from "@/session/worker/hub-proxy-wire"
import { PluginToolIDCollisionError, SessionLayerBoundaryError } from "@/session/worker/layer"
import { SessionLayerViolationError } from "@/session/worker/node-table"
import { DuplicatePluginError, NonContainerReplacementError } from "@/session/worker/plugin-host"
import { PluginToolNotSerializableError } from "@/session/worker/plugin-tool-wire"
import { MissingWorkerPortError } from "@/session/worker/session-worker"
import { SessionWorkerDeadError } from "@/session/worker/supervision"
import { SpawnGateAbortedError, SpawnGateTimeoutError } from "@/session/worker/worker-pools"

/**
 * Every worker-surface error that was converted to `Schema.TaggedErrorClass`
 * keeps its class name, its legacy stable `.tag` where callers read one, and its
 * exact human-readable message while gaining a namespaced `_tag`.
 */
describe("worker error idiom", () => {
  test("frame errors stay a ProtocolError family with namespaced tags", () => {
    const protocol = new ProtocolError("bad frame")
    expect(protocol._tag).toBe("WorkerFrame.Protocol")
    expect(protocol.tag).toBe("PROTOCOL")
    expect(protocol.message).toBe("bad frame")

    const version = new UnsupportedFrameVersionError(2)
    expect(String(version._tag)).toBe("WorkerFrame.UnsupportedVersion")
    expect(version.name).toBe("UnsupportedFrameVersionError")
    expect(version.message).toContain("unsupported worker frame version")
    expect(version).toBeInstanceOf(ProtocolError)

    const large = new FrameTooLargeError(10, 5)
    expect(String(large._tag)).toBe("WorkerFrame.TooLarge")
    expect(large.name).toBe("FrameTooLargeError")
    expect(large.message).toBe("worker frame is 10 bytes (limit 5); the frame was rejected, not truncated")
    expect(large).toBeInstanceOf(ProtocolError)
  })

  test("ledger errors keep the legacy tag and gain distinct namespaced tags", () => {
    const generic = new ToolCallLedgerError("bad ledger")
    expect(generic._tag).toBe("ToolCallLedger.Error")
    expect(generic.tag).toBe("TOOL_CALL_LEDGER")
    expect(generic.message).toBe("bad ledger")

    const duplicate = new ToolCallAlreadyStartedError("call_1", "ses_1")
    expect(String(duplicate._tag)).toBe("ToolCallLedger.AlreadyStarted")
    expect(duplicate.tag).toBe("TOOL_CALL_LEDGER")
    expect(duplicate.message).toBe("tool call call_1 is already recorded for session ses_1")
    expect(duplicate).toBeInstanceOf(ToolCallLedgerError)

    const missing = new ToolCallNotStartedError("call_2", "ses_1")
    expect(String(missing._tag)).toBe("ToolCallLedger.NotStarted")
    expect(missing.message).toBe("tool call call_2 was never started for session ses_1")
    expect(missing).toBeInstanceOf(ToolCallLedgerError)

    const stale = new StaleWorkerGenerationError("ses_1", "worker_a", undefined)
    expect(String(stale._tag)).toBe("ToolCallLedger.StaleGeneration")
    expect(stale.message).toBe("worker generation worker_a is stale for session ses_1: current lease owner is none")
    expect(stale).toBeInstanceOf(ToolCallLedgerError)
  })

  test("schema gate errors carry the stable WorkerDb tags", () => {
    const mismatch = new SchemaMismatch(
      { version: 1, contentHash: "aaa" },
      { version: 2, contentHash: "bbb" },
    )
    expect(mismatch._tag).toBe("WorkerDb.SchemaMismatch")
    expect(mismatch.tag).toBe("WorkerDb.SchemaMismatch")
    expect(mismatch.message).toBe("schema mismatch: expected version 1/aaa, observed 2/bbb")

    const live = new LiveWorkersError(3)
    expect(live._tag).toBe("WorkerDb.LiveWorkers")
    expect(live.tag).toBe("WorkerDb.LiveWorkers")
    expect(live.message).toBe("schema migration requires zero live workers, found 3")

    const missing = new MissingQueryInTransactionError()
    expect(missing._tag).toBe("WorkerDb.MissingQueryInTransaction")
    expect(missing.message).toContain("queryInTransaction")

    const drift = new TableAllowlistDriftError({
      sessionScoped: { missing: ["a"], unexpected: [] },
      tables: { missing: [], unexpected: ["b"] },
    })
    expect(drift._tag).toBe("WorkerDb.TableAllowlistDrift")
    expect(drift.tag).toBe("WorkerDb.TableAllowlistDrift")
    expect(drift.message).toContain("session_id tables changed without regenerating the allowlist")
  })

  test("worker lifecycle, supervision, and build errors are tagged", () => {
    const reply = new SessionWorkerReplyTimeoutError(25)
    expect(reply._tag).toBe("SessionExecutor.ReplyTimeout")
    expect(reply.message).toBe("timed out waiting for a session worker reply after 25ms")

    const request = new SessionExecutorRequestError(4, "WorkerCrashedError", "worker failed")
    expect(String(request._tag)).toBe("WorkerCrashedError")
    expect(request.tag).toBe("WorkerCrashedError")
    expect(request.message).toBe("worker failed")

    const untagged = new SessionExecutorRequestError(5, undefined, "unclassified")
    expect(String(untagged._tag)).toBe("SessionExecutorRequestError")

    const dead = new SessionWorkerDeadError("ses_2")
    expect(dead._tag).toBe("SessionSupervision.WorkerDead")
    expect(dead.message).toContain("dead after repeated worker crashes")

    const crash = new WorkerCrashRecordError("bad crash")
    expect(crash._tag).toBe("WorkerCrash.Record")
    expect(crash.tag).toBe("WORKER_CRASH_RECORD")
    expect(crash.message).toBe("bad crash")

    const rpc = new LifecycleRpcTimeoutError("commit", 5000)
    expect(rpc._tag).toBe("Lifecycle.RpcTimeout")
    expect(rpc.tag).toBe("LIFECYCLE_RPC_TIMEOUT")
    expect(rpc.message).toBe("lifecycle RPC commit was not acknowledged within 5000ms")

    const foreign = new ForeignLifecycleReportError(7, undefined, "ses_other")
    expect(foreign._tag).toBe("WorkerHub.ForeignLifecycleReport")
    expect(foreign.message).toBe("worker 7 is bound to no session; refusing a lifecycle report for ses_other")

    const port = new MissingWorkerPortError("egress")
    expect(port._tag).toBe("WorkerSession.MissingPort")
    expect(port.message).toBe('session worker is missing the "egress" MessagePort')
  })

  test("spawn gate, egress, and layer errors are tagged", () => {
    const timeout = new SpawnGateTimeoutError(100)
    expect(timeout._tag).toBe("WorkerPools.SpawnGateTimeout")
    expect(timeout.message).toContain("no worker spawn permit became available within 100ms")

    const aborted = new SpawnGateAbortedError()
    expect(aborted._tag).toBe("WorkerPools.SpawnGateAborted")

    const ended = new CoalescerEndedError()
    expect(ended._tag).toBe("Egress.CoalescerEnded")
    expect(ended.message).toBe("coalescer has ended")

    const order = new DurableOrderError("gap", { aggregateID: "ses_1", seq: 3 }, { aggregateID: "ses_1", seq: 1 })
    expect(order._tag).toBe("Egress.DurableOrder")
    expect(order.message).toBe("gap durable egress: got ses_1#3, last ses_1#1")

    const boundary = new SessionLayerBoundaryError("Database", "query")
    expect(boundary._tag).toBe("WorkerLayer.Boundary")
    expect(boundary.message).toContain('session worker boundary "Database" cannot use "query"')

    const violation = new SessionLayerViolationError({
      excluded: ["Database"],
      missingClients: [],
      unknown: [],
      ok: false,
    })
    expect(violation._tag).toBe("WorkerLayer.NodeViolation")
    expect(violation.message).toContain("excluded nodes present: Database")

    const collision = new PluginToolIDCollisionError("read", "reserved")
    expect(collision._tag).toBe("PluginTool.IDCollision")
    expect(collision.message).toBe('plugin tool id "read" collides with a locally registered tool')
  })

  test("plugin errors are tagged and stay clone-safe", () => {
    const duplicate = new DuplicatePluginError("dup")
    expect(duplicate._tag).toBe("WorkerPlugins.DuplicatePlugin")
    expect(duplicate.message).toBe("duplicate plugin: dup")

    const replacement = new NonContainerReplacementError("parts", "string")
    expect(replacement._tag).toBe("WorkerPlugins.NonContainerReplacement")
    expect(replacement.message).toBe(
      'plugin replaced "parts" with a string; the field requires a matching container',
    )

    const serializable = new PluginToolNotSerializableError("compress", "metadata")
    expect(serializable._tag).toBe("PluginTool.NotSerializable")
    expect(serializable.message).toBe('plugin tool "compress" returned a non-serializable metadata')

    const invalid = new InvalidPluginToolArgsError("compress", "bad shape")
    expect(invalid._tag).toBe("PluginTool.InvalidArgs")
    expect(invalid.message).toBe('invalid args for plugin tool "compress": bad shape')

    // The registry's stale-descriptor refresh matches this exact wire tag.
    const unknown = new UnknownPluginToolError("compress")
    expect(unknown._tag).toBe("HubProxyAuthorities.UnknownPluginTool")
    expect(unknown.name).toBe("UnknownPluginToolError")
    expect(unknown.toolID).toBe("compress")
    expect(unknown.message).toBe('unknown plugin tool "compress": no loaded plugin exposes that tool id')
    expect(unknown).toBeInstanceOf(Error)

    const remote = new HubProxyRemoteError("remote failed", { tag: "SessionNotFound", data: { id: "ses_1" } })
    expect(remote._tag).toBe("HubProxy.Remote")
    expect(remote.tag).toBe("SessionNotFound")
    expect(remote.message).toBe("remote failed")
  })
})
