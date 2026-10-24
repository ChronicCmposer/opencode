import { Effect, Layer, Schema } from "effect"
import type { unhandled } from "effect/Types"
import { HttpServerResponse } from "effect/unstable/http"
import { HttpApiError, HttpApiMiddleware } from "effect/unstable/httpapi"
import { InvalidRequestError } from "../errors"

// Effect's Issue formatter recursively dumps the rejected `actual` value with
// no truncation, so a 5KB invalid array produces a ~360KB string. Cap to keep
// 4xx responses small and avoid mirroring entire request payloads (which may
// contain secrets) into the response body and log file.
const REASON_LIMIT = 1024
function truncateReason(reason: string) {
  if (reason.length <= REASON_LIMIT) return reason
  return reason.slice(0, REASON_LIMIT) + `… (${reason.length - REASON_LIMIT} more chars)`
}

// Default Respondable returns an empty 400 body. Match the NamedError shape
// used by other 4xx/5xx so the SDK's `wrapClientError` extracts `.data.message`.
export class SchemaErrorMiddleware extends HttpApiMiddleware.Service<SchemaErrorMiddleware>()(
  "@opencode/HttpApiSchemaError",
  {
    error: InvalidRequestError,
  },
) {}

const respond = (
  error: HttpApiError.HttpApiSchemaError,
  context: { readonly endpoint: { readonly path: string } },
): Effect.Effect<HttpServerResponse.HttpServerResponse, InvalidRequestError, never> => {
  const reason = truncateReason(error.cause.message)
  const response = context.endpoint.path.startsWith("/api/")
    ? Effect.fail(
        new InvalidRequestError({
          message: reason,
          kind: error.kind,
        }),
      )
    : Effect.succeed(
        HttpServerResponse.jsonUnsafe(
          { name: "BadRequest", data: { message: reason, kind: error.kind } },
          { status: 400 },
        ),
      )
  return Effect.logWarning("schema rejection", { kind: error.kind, reason }).pipe(Effect.andThen(response))
}

// A schema rejection can surface two ways:
//   1. A framework-typed `HttpApiSchemaError` failure (param/query/payload decode
//      or response encode), caught by `Effect.catch`.
//   2. A `SchemaError` defect thrown synchronously by a boundary decoder that
//      validates untrusted persisted data with `Schema.decodeUnknownSync`
//      (e.g. session message/part hydration), caught by `Effect.catchDefect`.
// Both are the same class of failure and must render the same NamedError-shaped
// 400, so both route through `respond`. Non-schema defects and typed errors are
// re-raised unchanged.
export const schemaErrorLayer = Layer.succeed(SchemaErrorMiddleware, (httpEffect, options) =>
  Effect.catch(
    Effect.catchDefect(httpEffect, (defect) =>
      Schema.isSchemaError(defect)
        ? Effect.fail(new HttpApiError.HttpApiSchemaError({ kind: "Body", cause: defect }))
        : Effect.die(defect),
    ),
    (
      error,
    ): Effect.Effect<HttpServerResponse.HttpServerResponse, unhandled | InvalidRequestError, never> =>
      HttpApiError.HttpApiSchemaError.is(error) ? respond(error, options) : Effect.fail(error),
  ),
)
