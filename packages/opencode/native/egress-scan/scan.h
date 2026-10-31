#ifndef EGRESS_SCAN_H
#define EGRESS_SCAN_H

#include <stddef.h>
#include <stdint.h>

#define EGRESS_SCAN_MAX_BYTES 4194304u
#define EGRESS_SCAN_MAX_DEPTH 512
#define EGRESS_SCAN_MAX_KEY_BYTES 65536
#define EGRESS_SCAN_FRAME_VERSION 1

typedef enum {
  EGRESS_CODE_NONE = 0,
  EGRESS_CODE_TOO_LARGE,
  EGRESS_CODE_BAD_HEADER,
  EGRESS_CODE_INVALID_JSON,
  EGRESS_CODE_INVALID_UTF8,
  EGRESS_CODE_TOO_DEEP,
  EGRESS_CODE_KEY_TOO_LONG,
  EGRESS_CODE_DANGEROUS_KEY,
  EGRESS_CODE_BAD_ENVELOPE,
  EGRESS_CODE_UNSUPPORTED_VERSION
} egress_code;

typedef enum {
  EGRESS_KIND_EVENT = 0,
  EGRESS_KIND_END,
  EGRESS_KIND_RESYNC,
  EGRESS_KIND_ACK
} egress_kind;

/* A string value as the raw bytes between its quotes; `escaped` strings need egress_decode_utf16. */
typedef struct {
  int present;
  int escaped;
  size_t start;
  size_t length;
} egress_span;

typedef struct {
  int present;
  double value;
} egress_number;

typedef struct {
  int ok;

  egress_code code;
  size_t offset;
  const char *message;
  const char *key; /* the offending key for EGRESS_CODE_DANGEROUS_KEY; static storage */

  egress_kind kind;
  egress_number id;
  egress_span event_id;
  egress_span event_type;
  egress_number generation;
  egress_number ordinal;
  egress_span aggregate_id;
  egress_number seq;
  egress_span pattern;
  int has_inclusive;
  int inclusive;
  egress_number count;

  size_t properties_start;
  size_t properties_end;
  int is_blob_reference;
  egress_span properties_session_id;
  egress_span properties_aggregate_id;
} egress_scan_result;

/* One pass over one events byte frame. Allocation-free and reentrant. */
void egress_scan(const uint8_t *data, size_t length, egress_scan_result *result);

/* Decode a validated string body to UTF-16 (lone surrogate escapes preserved). `out` holds at least `length` units. */
size_t egress_decode_utf16(const uint8_t *src, size_t length, uint16_t *out);

#endif
