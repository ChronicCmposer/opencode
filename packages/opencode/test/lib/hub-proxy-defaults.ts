import type { HubProxyHandlers } from "@/session/worker/hub-proxy-wire"

/**
 * Hub answers for a project with no MCP servers and snapshots disabled. Every
 * real worker turn reads MCP instructions and tracks a snapshot through the hub,
 * so harnesses that serve only the authorities a test exercises spread these in.
 */
export const noMcpNoSnapshotHubProxy: HubProxyHandlers = {
  "mcp.clients": () => ({}),
  "mcp.instructions": () => [],
  "mcp.resources": () => ({}),
  "mcp.resourceTemplates": () => ({}),
  "mcp.readResource": () => undefined,
  "snapshot.track": () => undefined,
  "snapshot.patch": (hash) => ({ hash, files: [] }),
  "snapshot.restore": () => undefined,
  "snapshot.revert": () => undefined,
  "snapshot.diff": () => "",
  "snapshot.diffFull": () => [],
}
