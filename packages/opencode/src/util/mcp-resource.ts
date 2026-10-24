// Shared MCP resource attachment policy. Both the prompt-side resolver
// (`session/prompt.ts`) and the tool-side formatter (`session/tools.ts`) must
// agree on which binary resource MIME types are attachable, the size ceiling,
// and how a base64 payload's decoded size is reported to the model.

/** Binary MCP resources larger than this are summarised, never attached. */
export const MAX_MCP_RESOURCE_BLOB_BYTES = 10 * 1024 * 1024

/** MIME types that may be attached to a message as a binary file part. */
export const SUPPORTED_MCP_RESOURCE_ATTACHMENT_MIMES: ReadonlySet<string> = new Set([
  "application/pdf",
  "image/gif",
  "image/jpeg",
  "image/png",
  "image/webp",
])

/**
 * Decoded byte size of a base64 payload. Whitespace is ignored and padding is
 * discounted, so a line-wrapped blob reports the same size as a compact one.
 */
export function mcpResourceBase64Size(value: string): number {
  const trimmed = value.replace(/\s/g, "")
  const padding = trimmed.endsWith("==") ? 2 : trimmed.endsWith("=") ? 1 : 0
  return Math.max(0, Math.floor((trimmed.length * 3) / 4) - padding)
}

/** Human-readable byte size used in omission notices. */
export function formatMcpResourceBytes(value: number): string {
  if (value < 1024) return `${value} B`
  if (value < 1024 * 1024) return `${Math.ceil(value / 1024)} KB`
  return `${Math.ceil(value / (1024 * 1024))} MB`
}

export * as McpResource from "./mcp-resource"
