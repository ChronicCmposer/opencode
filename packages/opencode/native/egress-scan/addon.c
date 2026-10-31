#define NAPI_VERSION 8
#include <node_api.h>

#include <stdlib.h>

#include "scan.h"

#define SMALL_STRING_UNITS 1024

static const char *const kind_names[] = {"event", "end", "resync", "ack"};
static const char *const code_names[] = {"",
                                         "too-large",
                                         "bad-header",
                                         "invalid-json",
                                         "invalid-utf8",
                                         "too-deep",
                                         "key-too-long",
                                         "dangerous-key",
                                         "bad-envelope",
                                         "unsupported-version"};

static int set(napi_env env, napi_value object, const char *name, napi_value value) {
  return napi_set_named_property(env, object, name, value) == napi_ok;
}

static int set_number(napi_env env, napi_value object, const char *name, double number) {
  napi_value value;
  return napi_create_double(env, number, &value) == napi_ok && set(env, object, name, value);
}

static int set_text(napi_env env, napi_value object, const char *name, const char *text) {
  napi_value value;
  return napi_create_string_utf8(env, text, NAPI_AUTO_LENGTH, &value) == napi_ok && set(env, object, name, value);
}

static int set_bool(napi_env env, napi_value object, const char *name, int flag) {
  napi_value value;
  return napi_get_boolean(env, flag != 0, &value) == napi_ok && set(env, object, name, value);
}

static int set_optional_number(napi_env env, napi_value object, const char *name, const egress_number *n) {
  return !n->present || set_number(env, object, name, n->value);
}

static int set_optional_span(napi_env env, napi_value object, const char *name, const uint8_t *data,
                             const egress_span *span) {
  if (!span->present) return 1;
  napi_value value;
  const uint8_t *src = data + span->start;
  if (!span->escaped) {
    return napi_create_string_utf8(env, (const char *)src, span->length, &value) == napi_ok && set(env, object, name, value);
  }
  uint16_t small[SMALL_STRING_UNITS];
  uint16_t *units = small;
  if (span->length > SMALL_STRING_UNITS) {
    units = malloc(span->length * sizeof(uint16_t));
    if (units == NULL) return 0;
  }
  size_t count = egress_decode_utf16(src, span->length, units);
  int ok = napi_create_string_utf16(env, (const char16_t *)units, count, &value) == napi_ok && set(env, object, name, value);
  if (units != small) free(units);
  return ok;
}

static napi_value scan(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1) {
    napi_throw_type_error(env, NULL, "scan expects a Uint8Array");
    return NULL;
  }
  napi_typedarray_type type;
  size_t length;
  void *data;
  if (napi_get_typedarray_info(env, argv[0], &type, &length, &data, NULL, NULL) != napi_ok || type != napi_uint8_array) {
    napi_throw_type_error(env, NULL, "scan expects a Uint8Array");
    return NULL;
  }

  egress_scan_result r;
  egress_scan((const uint8_t *)data, length, &r);

  napi_value out;
  if (napi_create_object(env, &out) != napi_ok) return NULL;
  int ok = set_bool(env, out, "ok", r.ok);
  if (!r.ok) {
    ok = ok && set_text(env, out, "code", code_names[r.code]) && set_number(env, out, "offset", (double)r.offset) &&
         set_text(env, out, "message", r.message);
    if (ok && r.key != NULL) ok = set_text(env, out, "key", r.key);
    return ok ? out : NULL;
  }
  const uint8_t *bytes = (const uint8_t *)data;
  ok = ok && set_text(env, out, "kind", kind_names[r.kind]) && set_optional_number(env, out, "id", &r.id) &&
       set_optional_span(env, out, "eventID", bytes, &r.event_id) &&
       set_optional_span(env, out, "eventType", bytes, &r.event_type) &&
       set_optional_number(env, out, "generation", &r.generation) &&
       set_optional_number(env, out, "ordinal", &r.ordinal) &&
       set_optional_span(env, out, "aggregateID", bytes, &r.aggregate_id) && set_optional_number(env, out, "seq", &r.seq) &&
       set_optional_span(env, out, "pattern", bytes, &r.pattern) && (!r.has_inclusive || set_bool(env, out, "inclusive", r.inclusive)) &&
       set_optional_number(env, out, "count", &r.count);
  if (ok && r.kind == EGRESS_KIND_EVENT) {
    ok = set_number(env, out, "propertiesStart", (double)r.properties_start) &&
         set_number(env, out, "propertiesEnd", (double)r.properties_end) &&
         set_bool(env, out, "isBlobReference", r.is_blob_reference) &&
         set_optional_span(env, out, "propertiesSessionID", bytes, &r.properties_session_id) &&
         set_optional_span(env, out, "propertiesAggregateID", bytes, &r.properties_aggregate_id);
  }
  return ok ? out : NULL;
}

NAPI_MODULE_INIT() {
  napi_value fn;
  if (napi_create_function(env, "scan", NAPI_AUTO_LENGTH, scan, NULL, &fn) != napi_ok) return NULL;
  if (napi_set_named_property(env, exports, "scan", fn) != napi_ok) return NULL;
  return exports;
}
