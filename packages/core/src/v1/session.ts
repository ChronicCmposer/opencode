export * as SessionV1 from "./session"

import { Schema } from "effect"
import * as SessionV1Schema from "@opencode-ai/schema/session-v1"
import { NamedError } from "../util/error"

export {
  AgentPart,
  AgentPartInput,
  Assistant,
  CompactionPart,
  Event,
  FilePart,
  FilePartInput,
  FilePartSource,
  FileSource,
  Format,
  Info,
  MessageID,
  OutputFormatJsonSchema,
  OutputFormatText,
  Part,
  PartID,
  PatchPart,
  Range,
  ReasoningPart,
  ResourceSource,
  RetryPart,
  SessionInfo,
  SnapshotPart,
  StepFinishPart,
  StepStartPart,
  SubtaskPart,
  SubtaskPartInput,
  SymbolSource,
  TextPart,
  TextPartInput,
  ToolPart,
  ToolState,
  ToolStateCompleted,
  ToolStateError,
  ToolStatePending,
  ToolStateRunning,
  User,
  WithParts,
} from "@opencode-ai/schema/session-v1"

// The V1 error field contracts live in `@opencode-ai/schema`; the runtime
// facade derives its callable error classes from those canonical schemas
// rather than restating the fields, so the wire contract and the runtime class
// can never drift apart.
export const OutputLengthError = NamedError.create("MessageOutputLengthError", SessionV1Schema.OutputLengthError.data)
export const AuthError = NamedError.create("ProviderAuthError", SessionV1Schema.AuthError.data)
export const AbortedError = NamedError.create("MessageAbortedError", SessionV1Schema.AbortedError.data)
export const StructuredOutputError = NamedError.create(
  "StructuredOutputError",
  SessionV1Schema.StructuredOutputError.data,
)
export const APIError = NamedError.create("APIError", SessionV1Schema.APIError.data)
export type APIError = Schema.Schema.Type<typeof APIError.Schema>
export const ContextOverflowError = NamedError.create(
  "ContextOverflowError",
  SessionV1Schema.ContextOverflowError.data,
)
export const ContentFilterError = NamedError.create("ContentFilterError", SessionV1Schema.ContentFilterError.data)
export const IncompleteStreamError = NamedError.create(
  "IncompleteStreamError",
  SessionV1Schema.IncompleteStreamError.data,
)
export const MaxStepsExceededError = NamedError.create(
  "MaxStepsExceededError",
  SessionV1Schema.MaxStepsExceededError.data,
)
