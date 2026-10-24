// Process-role probe fixture.
//
// Runs in its OWN isolate so the per-isolate module state in
// `@/effect/process-role` is fresh: the fixture observes the default role, flips
// it once, and reports the resulting `sessionWorkerNodes` decision. This proves
// both the set-once semantics and that the graph gate reads the process role —
// without mutating the shared main-test-process module state.

import { Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { processRole, setProcessRole } from "@/effect/process-role"
import { sessionWorkerNodes } from "@/session/worker/nodes"
import { postToParent } from "./worker-scope"

const hubNode = LayerNode.make({
  name: "@opencode/SessionHub",
  layer: Layer.empty as Layer.Layer<never>,
  deps: [] as const,
})

/** Capture a `setProcessRole` outcome instead of letting it crash the isolate. */
function capture(fn: () => void): string | null {
  try {
    fn()
    return null
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}

const defaultRole = processRole()
const nodesBefore = sessionWorkerNodes({ hubNode, enabled: true }).length

setProcessRole("session")
const roleAfter = processRole()
const nodesAfter = sessionWorkerNodes({ hubNode, enabled: true }).length

// A conflicting re-set must fail loud; re-setting the same role is a no-op.
const conflict = capture(() => setProcessRole("writer"))
const sameRole = capture(() => setProcessRole("session"))

// Protocol uniformity: announce readiness before the one-shot result so every
// worker fixture shares the same `ready` -> payload shape.
postToParent({ type: "ready" })
postToParent({ type: "result", defaultRole, nodesBefore, roleAfter, nodesAfter, conflict, sameRole })
