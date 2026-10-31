#include "scan.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  const uint8_t *p;
  size_t end; /* end of the segment being scanned */
  egress_scan_result *r;
  /* The strongest limit violation (too-deep > dangerous-key > key-too-long) seen in this segment. Scanning goes on
     so that a syntax or UTF-8 error anywhere in the segment still wins, as it does for the JS decoder. */
  egress_code pending_code;
  size_t pending_offset;
  const char *pending_message;
  const char *pending_key;
} state;

static int fail(state *s, egress_code code, size_t offset, const char *message) {
  s->r->ok = 0;
  s->r->code = code;
  s->r->offset = offset;
  s->r->message = message;
  return 0;
}

static int limit_rank(egress_code code) {
  return code == EGRESS_CODE_TOO_DEEP ? 3 : code == EGRESS_CODE_DANGEROUS_KEY ? 2 : code == EGRESS_CODE_KEY_TOO_LONG ? 1 : 0;
}

static void note_limit(state *s, egress_code code, size_t offset, const char *message, const char *key) {
  if (limit_rank(code) <= limit_rank(s->pending_code)) return;
  s->pending_code = code;
  s->pending_offset = offset;
  s->pending_message = message;
  s->pending_key = key;
}

/* End of a segment that is valid JSON: report the pending limit violation, if any. */
static int finish_segment(state *s) {
  if (s->pending_code == EGRESS_CODE_NONE) return 1;
  fail(s, s->pending_code, s->pending_offset, s->pending_message);
  s->r->key = s->pending_key;
  return 0;
}

static void skip_ws(state *s, size_t *i) {
  size_t k = *i;
  while (k < s->end) {
    uint8_t c = s->p[k];
    if (c != ' ' && c != '\n' && c != '\r' && c != '\t') break;
    k++;
  }
  *i = k;
}

static int is_digit(uint8_t c) { return c >= '0' && c <= '9'; }

static int hex_value(uint8_t c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

/* Length of the well-formed UTF-8 sequence at p[k] (lead byte >= 0x80), or 0. */
static size_t utf8_sequence(const uint8_t *p, size_t k, size_t e) {
  uint8_t c = p[k];
  size_t n;
  uint8_t lo = 0x80, hi = 0xBF;
  if (c >= 0xC2 && c <= 0xDF) {
    n = 1;
  } else if (c >= 0xE0 && c <= 0xEF) {
    n = 2;
    if (c == 0xE0) lo = 0xA0;
    else if (c == 0xED) hi = 0x9F;
  } else if (c >= 0xF0 && c <= 0xF4) {
    n = 3;
    if (c == 0xF0) lo = 0x90;
    else if (c == 0xF4) hi = 0x8F;
  } else {
    return 0;
  }
  if (k + n >= e) return 0;
  if (p[k + 1] < lo || p[k + 1] > hi) return 0;
  for (size_t j = 2; j <= n; j++) {
    if (p[k + j] < 0x80 || p[k + j] > 0xBF) return 0;
  }
  return n + 1;
}

/* First ill-formed UTF-8 byte in [start, end), or `end`. */
static size_t utf8_first_invalid(const uint8_t *p, size_t start, size_t end) {
  size_t k = start;
  while (k < end) {
    if (p[k] < 0x80) {
      k++;
      continue;
    }
    size_t n = utf8_sequence(p, k, end);
    if (n == 0) return k;
    k += n;
  }
  return end;
}

/* *i is at the opening quote. */
static int scan_string(state *s, size_t *i, egress_span *span) {
  const uint8_t *p = s->p;
  size_t e = s->end;
  size_t k = *i + 1;
  size_t start = k;
  int escaped = 0;
  for (;;) {
    if (k >= e) return fail(s, EGRESS_CODE_INVALID_JSON, e, "unterminated string");
    uint8_t c = p[k];
    if (c == '"') break;
    if (c == '\\') {
      escaped = 1;
      k++;
      if (k >= e) return fail(s, EGRESS_CODE_INVALID_JSON, e, "unterminated string");
      switch (p[k]) {
        case '"': case '\\': case '/': case 'b': case 'f': case 'n': case 'r': case 't':
          k++;
          break;
        case 'u':
          if (k + 4 >= e) return fail(s, EGRESS_CODE_INVALID_JSON, k - 1, "truncated unicode escape");
          for (int j = 1; j <= 4; j++) {
            if (hex_value(p[k + j]) < 0) return fail(s, EGRESS_CODE_INVALID_JSON, k - 1, "invalid unicode escape");
          }
          k += 5;
          break;
        default:
          return fail(s, EGRESS_CODE_INVALID_JSON, k - 1, "invalid escape");
      }
      continue;
    }
    if (c < 0x20) return fail(s, EGRESS_CODE_INVALID_JSON, k, "control character in string");
    if (c < 0x80) {
      k++;
      continue;
    }
    size_t n = utf8_sequence(p, k, e);
    if (n == 0) return fail(s, EGRESS_CODE_INVALID_UTF8, k, "invalid UTF-8 sequence");
    k += n;
  }
  span->present = 1;
  span->escaped = escaped;
  span->start = start;
  span->length = k - start;
  *i = k + 1;
  return 1;
}

static int scan_number(state *s, size_t *i, egress_span *span) {
  const uint8_t *p = s->p;
  size_t e = s->end;
  size_t k = *i;
  if (p[k] == '-') k++;
  if (k >= e) return fail(s, EGRESS_CODE_INVALID_JSON, k, "invalid number");
  if (p[k] == '0') {
    k++;
  } else if (p[k] >= '1' && p[k] <= '9') {
    while (k < e && is_digit(p[k])) k++;
  } else {
    return fail(s, EGRESS_CODE_INVALID_JSON, k, "invalid number");
  }
  if (k < e && p[k] == '.') {
    k++;
    if (k >= e || !is_digit(p[k])) return fail(s, EGRESS_CODE_INVALID_JSON, k, "invalid number");
    while (k < e && is_digit(p[k])) k++;
  }
  if (k < e && (p[k] == 'e' || p[k] == 'E')) {
    k++;
    if (k < e && (p[k] == '+' || p[k] == '-')) k++;
    if (k >= e || !is_digit(p[k])) return fail(s, EGRESS_CODE_INVALID_JSON, k, "invalid number");
    while (k < e && is_digit(p[k])) k++;
  }
  span->present = 1;
  span->escaped = 0;
  span->start = *i;
  span->length = k - *i;
  *i = k;
  return 1;
}

static int scan_literal(state *s, size_t *i, const char *word) {
  size_t n = strlen(word);
  if (*i + n > s->end || memcmp(s->p + *i, word, n) != 0) {
    return fail(s, EGRESS_CODE_INVALID_JSON, *i, "invalid literal");
  }
  *i += n;
  return 1;
}

/* Decode a validated string body into at most `cap` UTF-16 units; (size_t)-1 when it does not fit. */
static size_t decode_capped(const uint8_t *src, size_t n, uint16_t *out, size_t cap) {
  size_t k = 0, count = 0;
  while (k < n) {
    uint8_t c = src[k];
    if (c < 0x80 && c != '\\') {
      if (count + 1 > cap) return (size_t)-1;
      out[count++] = c;
      k++;
    } else if (c == '\\') {
      uint16_t unit;
      uint8_t x = src[k + 1];
      if (x == 'u') {
        unit = (uint16_t)((hex_value(src[k + 2]) << 12) | (hex_value(src[k + 3]) << 8) | (hex_value(src[k + 4]) << 4) |
                          hex_value(src[k + 5]));
        k += 6;
      } else {
        switch (x) {
          case 'b': unit = '\b'; break;
          case 'f': unit = '\f'; break;
          case 'n': unit = '\n'; break;
          case 'r': unit = '\r'; break;
          case 't': unit = '\t'; break;
          default: unit = x; break;
        }
        k += 2;
      }
      if (count + 1 > cap) return (size_t)-1;
      out[count++] = unit;
    } else {
      uint32_t cp;
      size_t len;
      if (c < 0xE0) {
        cp = c & 0x1F;
        len = 2;
      } else if (c < 0xF0) {
        cp = c & 0x0F;
        len = 3;
      } else {
        cp = c & 0x07;
        len = 4;
      }
      for (size_t j = 1; j < len; j++) cp = (cp << 6) | (src[k + j] & 0x3F);
      k += len;
      if (cp >= 0x10000) {
        if (count + 2 > cap) return (size_t)-1;
        cp -= 0x10000;
        out[count++] = (uint16_t)(0xD800 + (cp >> 10));
        out[count++] = (uint16_t)(0xDC00 + (cp & 0x3FF));
      } else {
        if (count + 1 > cap) return (size_t)-1;
        out[count++] = (uint16_t)cp;
      }
    }
  }
  return count;
}

size_t egress_decode_utf16(const uint8_t *src, size_t length, uint16_t *out) {
  return decode_capped(src, length, out, length);
}

/* Compare a string span's decoded value with an ASCII word. */
static int span_equals(const state *s, const egress_span *span, const char *word) {
  size_t n = strlen(word);
  if (!span->escaped) return span->length == n && memcmp(s->p + span->start, word, n) == 0;
  uint16_t buf[32];
  size_t count = decode_capped(s->p + span->start, span->length, buf, 32);
  if (count != n) return 0;
  for (size_t j = 0; j < n; j++) {
    if (buf[j] != (uint8_t)word[j]) return 0;
  }
  return 1;
}

static const char *dangerous_key(const state *s, const egress_span *span) {
  static const char *const names[] = {"__proto__", "constructor", "prototype"};
  if (!span->escaped && span->length != 9 && span->length != 11) return NULL;
  for (int j = 0; j < 3; j++) {
    if (span_equals(s, span, names[j])) return names[j];
  }
  return NULL;
}

enum {
  K_OTHER,
  K_V,
  K_PORT,
  K_KIND,
  K_ID,
  K_PAYLOAD,
  K_EVENT,
  K_TYPE,
  K_GENERATION,
  K_ORDINAL,
  K_AGGREGATE_ID,
  K_SEQ,
  K_PATTERN,
  K_INCLUSIVE,
  K_COUNT,
  K_SESSION_ID,
  K_BLOB_POSITION,
  K_FRAME_BYTES
};

static int key_id(const state *s, const egress_span *span) {
  static const char *const names[] = {NULL,          "v",        "port",         "kind",         "id",
                                      "payload",     "event",    "type",         "generation",   "ordinal",
                                      "aggregateID", "seq",      "pattern",      "inclusive",    "count",
                                      "sessionID",   "__eventBlobPosition",      "frameBytes"};
  if (span->length > 24) return K_OTHER;
  for (int j = 1; j <= K_FRAME_BYTES; j++) {
    if (span_equals(s, span, names[j])) return j;
  }
  return K_OTHER;
}

/* UTF-8 length of a validated key body after unescaping; a lone surrogate escape counts as 3 bytes. */
static size_t decoded_key_bytes(const state *s, const egress_span *span) {
  if (!span->escaped) return span->length;
  const uint8_t *src = s->p + span->start;
  size_t n = span->length, k = 0, bytes = 0;
  while (k < n) {
    if (src[k] != '\\') {
      bytes++;
      k++;
    } else if (src[k + 1] != 'u') {
      bytes++;
      k += 2;
    } else {
      unsigned unit = (unsigned)((hex_value(src[k + 2]) << 12) | (hex_value(src[k + 3]) << 8) | (hex_value(src[k + 4]) << 4) |
                                 hex_value(src[k + 5]));
      k += 6;
      if (unit >= 0xD800 && unit <= 0xDBFF && k + 5 < n && src[k] == '\\' && src[k + 1] == 'u') {
        unsigned low = (unsigned)((hex_value(src[k + 2]) << 12) | (hex_value(src[k + 3]) << 8) | (hex_value(src[k + 4]) << 4) |
                                  hex_value(src[k + 5]));
        if (low >= 0xDC00 && low <= 0xDFFF) {
          bytes += 4;
          k += 6;
          continue;
        }
      }
      bytes += unit < 0x80 ? 1 : unit < 0x800 ? 2 : 3;
    }
  }
  return bytes;
}

/* At a member key: scan it, note limit violations, consume the colon. */
static int member_key(state *s, size_t *i, egress_span *key) {
  skip_ws(s, i);
  if (*i >= s->end || s->p[*i] != '"') return fail(s, EGRESS_CODE_INVALID_JSON, *i, "expected a property name");
  size_t at = *i;
  if (!scan_string(s, i, key)) return 0;
  if (key->length > EGRESS_SCAN_MAX_KEY_BYTES && decoded_key_bytes(s, key) > EGRESS_SCAN_MAX_KEY_BYTES) {
    note_limit(s, EGRESS_CODE_KEY_TOO_LONG, at, "object key too long", NULL);
  }
  const char *bad = dangerous_key(s, key);
  if (bad != NULL) note_limit(s, EGRESS_CODE_DANGEROUS_KEY, at, "dangerous object key", bad);
  skip_ws(s, i);
  if (*i >= s->end || s->p[*i] != ':') return fail(s, EGRESS_CODE_INVALID_JSON, *i, "expected ':'");
  (*i)++;
  return 1;
}

/* Iterate an object whose '{' is consumed. 1: a member (value follows), 0: closed, -1: error. */
static int next_member(state *s, size_t *i, int *first, egress_span *key) {
  skip_ws(s, i);
  if (*i >= s->end) return fail(s, EGRESS_CODE_INVALID_JSON, *i, "unexpected end of JSON") - 1;
  uint8_t c = s->p[*i];
  if (c == '}' && *first) {
    (*i)++;
    return 0;
  }
  if (!*first) {
    if (c == '}') {
      (*i)++;
      return 0;
    }
    if (c != ',') return fail(s, EGRESS_CODE_INVALID_JSON, *i, "expected ',' or '}'") - 1;
    (*i)++;
  }
  *first = 0;
  return member_key(s, i, key) ? 1 : -1;
}

/* Container kinds, one bit per level (1 = array). The first 512 levels live inline; deeper input is validated on
   a heap bitmap so that its syntax can still be judged before the nesting limit is reported. */
typedef struct {
  uint64_t inline_bits[EGRESS_SCAN_MAX_DEPTH / 64];
  uint64_t *heap;
  size_t capacity;
} kind_stack;

static int stack_push(kind_stack *st, size_t sp, int bit) {
  if (sp >= st->capacity) {
    size_t grown = st->capacity * 2;
    uint64_t *bits = realloc(st->heap, grown / 8);
    if (bits == NULL) return 0;
    if (st->heap == NULL) memcpy(bits, st->inline_bits, sizeof st->inline_bits);
    memset(bits + st->capacity / 64, 0, (grown - st->capacity) / 8);
    st->heap = bits;
    st->capacity = grown;
  }
  uint64_t *words = st->heap != NULL ? st->heap : st->inline_bits;
  if (bit) words[sp / 64] |= (uint64_t)1 << (sp % 64);
  else words[sp / 64] &= ~((uint64_t)1 << (sp % 64));
  return 1;
}

static int stack_top(const kind_stack *st, size_t sp) {
  const uint64_t *words = st->heap != NULL ? st->heap : st->inline_bits;
  return (int)((words[sp / 64] >> (sp % 64)) & 1);
}

static int skip_value_with(state *s, size_t *ip, int depth, kind_stack *st) {
  size_t sp = 0;
  size_t i = *ip;
  egress_span span;
  uint8_t c;

value:
  skip_ws(s, &i);
  if (i >= s->end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "unexpected end of JSON");
  c = s->p[i];
  if (c == '{' || c == '[') {
    if ((size_t)depth + sp + 1 > EGRESS_SCAN_MAX_DEPTH) note_limit(s, EGRESS_CODE_TOO_DEEP, i, "nesting too deep", NULL);
    if (!stack_push(st, sp, c == '[')) return fail(s, EGRESS_CODE_TOO_DEEP, i, "nesting too deep");
    sp++;
    i++;
    skip_ws(s, &i);
    if (i < s->end && s->p[i] == (c == '{' ? '}' : ']')) {
      i++;
      sp--;
      goto after;
    }
    if (c == '{') {
      if (!member_key(s, &i, &span)) return 0;
    }
    goto value;
  }
  if (c == '"') {
    if (!scan_string(s, &i, &span)) return 0;
  } else if (c == 't') {
    if (!scan_literal(s, &i, "true")) return 0;
  } else if (c == 'f') {
    if (!scan_literal(s, &i, "false")) return 0;
  } else if (c == 'n') {
    if (!scan_literal(s, &i, "null")) return 0;
  } else if (c == '-' || is_digit(c)) {
    if (!scan_number(s, &i, &span)) return 0;
  } else {
    return fail(s, EGRESS_CODE_INVALID_JSON, i, "unexpected character");
  }

after:
  if (sp == 0) {
    *ip = i;
    return 1;
  }
  skip_ws(s, &i);
  if (i >= s->end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "unexpected end of JSON");
  c = s->p[i];
  if (!stack_top(st, sp - 1)) {
    if (c == ',') {
      i++;
      if (!member_key(s, &i, &span)) return 0;
      goto value;
    }
    if (c == '}') {
      i++;
      sp--;
      goto after;
    }
    return fail(s, EGRESS_CODE_INVALID_JSON, i, "expected ',' or '}'");
  }
  if (c == ',') {
    i++;
    goto value;
  }
  if (c == ']') {
    i++;
    sp--;
    goto after;
  }
  return fail(s, EGRESS_CODE_INVALID_JSON, i, "expected ',' or ']'");
}

/* Validate one value. `depth` is the number of containers already enclosing it. */
static int skip_value(state *s, size_t *ip, int depth) {
  kind_stack st;
  st.heap = NULL;
  st.capacity = EGRESS_SCAN_MAX_DEPTH;
  int ok = skip_value_with(s, ip, depth, &st);
  free(st.heap);
  return ok;
}

enum { T_NONE, T_STRING, T_NUMBER, T_BOOL, T_OBJECT, T_OTHER };

typedef struct {
  uint8_t t;
  uint8_t b;
  egress_span span;
} slot;

typedef struct {
  uint8_t t;
  slot id, type;
} event_t;

typedef struct {
  uint8_t t;
  event_t event;
  slot generation, ordinal, aggregate_id, seq, pattern, inclusive, count;
} payload_t;

typedef struct {
  slot v, port, kind, id;
  payload_t payload;
} envelope_t;

static int read_slot(state *s, size_t *i, int depth, slot *out) {
  memset(out, 0, sizeof *out);
  skip_ws(s, i);
  if (*i >= s->end) return fail(s, EGRESS_CODE_INVALID_JSON, *i, "unexpected end of JSON");
  uint8_t c = s->p[*i];
  if (c == '"') {
    out->t = T_STRING;
    return scan_string(s, i, &out->span);
  }
  if (c == '-' || is_digit(c)) {
    out->t = T_NUMBER;
    return scan_number(s, i, &out->span);
  }
  out->t = c == 't' || c == 'f' ? T_BOOL : T_OTHER;
  out->b = c == 't';
  return skip_value(s, i, depth);
}

static double number_value(const state *s, const egress_span *span) {
  const uint8_t *p = s->p + span->start;
  size_t n = span->length;
  int plain = 1;
  for (size_t j = 0; j < n; j++) {
    if (p[j] == '.' || p[j] == 'e' || p[j] == 'E') plain = 0;
  }
  if (plain && n <= 16) {
    size_t j = p[0] == '-' ? 1 : 0;
    double v = 0;
    for (; j < n; j++) v = v * 10 + (p[j] - '0');
    return p[0] == '-' ? -v : v;
  }
  if (n > 399) return plain ? HUGE_VAL : NAN;
  char buf[400];
  memcpy(buf, p, n);
  buf[n] = 0;
  return strtod(buf, NULL);
}

/* `safe`: within +-(2^53 - 1), as Schema.Int; otherwise any finite integer. */
static int slot_integer(const state *s, const slot *sl, int non_negative, int safe, double *out) {
  if (sl->t != T_NUMBER) return 0;
  double d = number_value(s, &sl->span);
  if (!isfinite(d) || floor(d) != d) return 0;
  if (non_negative && d < 0) return 0;
  if (safe && fabs(d) > 9007199254740991.0) return 0;
  *out = d == 0 ? 0 : d;
  return 1;
}

static int parse_event(state *s, size_t *i, event_t *event) {
  memset(event, 0, sizeof *event);
  event->t = T_OBJECT;
  (*i)++;
  int first = 1, m;
  egress_span key;
  while ((m = next_member(s, i, &first, &key)) == 1) {
    int ok;
    switch (key_id(s, &key)) {
      case K_ID: ok = read_slot(s, i, 3, &event->id); break;
      case K_TYPE: ok = read_slot(s, i, 3, &event->type); break;
      default: ok = skip_value(s, i, 3); break;
    }
    if (!ok) return 0;
  }
  return m == 0;
}

static int parse_payload(state *s, size_t *i, payload_t *payload) {
  memset(payload, 0, sizeof *payload);
  payload->t = T_OBJECT;
  (*i)++;
  int first = 1, m;
  egress_span key;
  while ((m = next_member(s, i, &first, &key)) == 1) {
    int ok;
    switch (key_id(s, &key)) {
      case K_EVENT:
        skip_ws(s, i);
        if (*i < s->end && s->p[*i] == '{') {
          ok = parse_event(s, i, &payload->event);
        } else {
          memset(&payload->event, 0, sizeof payload->event);
          payload->event.t = T_OTHER;
          ok = skip_value(s, i, 2);
        }
        break;
      case K_GENERATION: ok = read_slot(s, i, 2, &payload->generation); break;
      case K_ORDINAL: ok = read_slot(s, i, 2, &payload->ordinal); break;
      case K_AGGREGATE_ID: ok = read_slot(s, i, 2, &payload->aggregate_id); break;
      case K_SEQ: ok = read_slot(s, i, 2, &payload->seq); break;
      case K_PATTERN: ok = read_slot(s, i, 2, &payload->pattern); break;
      case K_INCLUSIVE: ok = read_slot(s, i, 2, &payload->inclusive); break;
      case K_COUNT: ok = read_slot(s, i, 2, &payload->count); break;
      default: ok = skip_value(s, i, 2); break;
    }
    if (!ok) return 0;
  }
  return m == 0;
}

static int parse_envelope(state *s, size_t start, size_t end, envelope_t *env) {
  s->end = end;
  size_t i = start;
  skip_ws(s, &i);
  if (i >= end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "empty envelope");
  if (s->p[i] != '{') {
    if (!skip_value(s, &i, 0)) return 0;
    skip_ws(s, &i);
    if (i != end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "unexpected data after the envelope");
    if (!finish_segment(s)) return 0;
    return fail(s, EGRESS_CODE_BAD_ENVELOPE, start, "frame must be an object");
  }
  i++;
  int first = 1, m;
  egress_span key;
  while ((m = next_member(s, &i, &first, &key)) == 1) {
    int ok;
    switch (key_id(s, &key)) {
      case K_V: ok = read_slot(s, &i, 1, &env->v); break;
      case K_PORT: ok = read_slot(s, &i, 1, &env->port); break;
      case K_KIND: ok = read_slot(s, &i, 1, &env->kind); break;
      case K_ID: ok = read_slot(s, &i, 1, &env->id); break;
      case K_PAYLOAD:
        skip_ws(s, &i);
        if (i < end && s->p[i] == '{') {
          ok = parse_payload(s, &i, &env->payload);
        } else {
          memset(&env->payload, 0, sizeof env->payload);
          env->payload.t = T_OTHER;
          ok = skip_value(s, &i, 1);
        }
        break;
      default: ok = skip_value(s, &i, 1); break;
    }
    if (!ok) return 0;
  }
  if (m < 0) return 0;
  skip_ws(s, &i);
  if (i != end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "unexpected data after the envelope");
  return finish_segment(s);
}

static void copy_number(const state *s, const slot *sl, egress_number *out) {
  double v;
  slot_integer(s, sl, 0, 0, &v);
  out->present = 1;
  out->value = v;
}

static int validate_envelope(state *s, const envelope_t *env, size_t at, egress_scan_result *r) {
  const payload_t *pl = &env->payload;
  double d;
  if (!(env->v.t == T_NUMBER && number_value(s, &env->v.span) == EGRESS_SCAN_FRAME_VERSION)) {
    return fail(s, EGRESS_CODE_UNSUPPORTED_VERSION, at, "unsupported frame version");
  }
  if (env->port.t != T_STRING || !span_equals(s, &env->port.span, "events")) {
    return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "port must be \"events\"");
  }
  if (env->id.t != T_NONE) {
    if (!slot_integer(s, &env->id, 1, 1, &d)) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "id must be a non-negative integer");
    r->id.present = 1;
    r->id.value = d;
  }
  if (env->kind.t != T_STRING) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "kind must be a string");
  if (span_equals(s, &env->kind.span, "event")) {
    r->kind = EGRESS_KIND_EVENT;
    if (pl->t != T_OBJECT) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "event payload must be an object");
    if (pl->event.t != T_OBJECT) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "payload.event must be an object");
    if (pl->event.id.t != T_STRING || pl->event.type.t != T_STRING) {
      return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "payload.event.id and type must be strings");
    }
    r->event_id = pl->event.id.span;
    r->event_type = pl->event.type.span;
    if (pl->generation.t != T_NONE) {
      if (!slot_integer(s, &pl->generation, 0, 1, &d)) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "generation must be an integer");
      copy_number(s, &pl->generation, &r->generation);
    }
    if (pl->ordinal.t != T_NONE) {
      if (!slot_integer(s, &pl->ordinal, 0, 1, &d)) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "ordinal must be an integer");
      copy_number(s, &pl->ordinal, &r->ordinal);
    }
    if (pl->aggregate_id.t != T_NONE) {
      if (pl->aggregate_id.t != T_STRING || pl->aggregate_id.span.length == 0) {
        return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "aggregateID must be a non-empty string");
      }
      r->aggregate_id = pl->aggregate_id.span;
    }
    if (pl->seq.t != T_NONE) {
      if (!slot_integer(s, &pl->seq, 1, 0, &d)) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "seq must be a non-negative integer");
      copy_number(s, &pl->seq, &r->seq);
    }
    if (r->aggregate_id.present != r->seq.present) {
      return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "durable event must carry both aggregateID and seq, or neither");
    }
    return 1;
  }
  if (span_equals(s, &env->kind.span, "end")) {
    r->kind = EGRESS_KIND_END;
    if (pl->t != T_NONE) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "end frame takes no payload");
    return 1;
  }
  if (span_equals(s, &env->kind.span, "resync")) {
    r->kind = EGRESS_KIND_RESYNC;
    if (pl->t == T_NONE) return 1;
    if (pl->t != T_OBJECT) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "resync payload must be an object");
    if (pl->pattern.t != T_NONE) {
      if (pl->pattern.t != T_STRING) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "pattern must be a string");
      r->pattern = pl->pattern.span;
    }
    if (pl->aggregate_id.t != T_NONE) {
      if (pl->aggregate_id.t != T_STRING || pl->aggregate_id.span.length == 0) {
        return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "aggregateID must be a non-empty string");
      }
      r->aggregate_id = pl->aggregate_id.span;
    }
    if (pl->seq.t != T_NONE) {
      if (!slot_integer(s, &pl->seq, 1, 0, &d)) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "seq must be a non-negative integer");
      copy_number(s, &pl->seq, &r->seq);
    }
    if (pl->inclusive.t != T_NONE) {
      if (pl->inclusive.t != T_BOOL) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "inclusive must be a boolean");
      r->has_inclusive = 1;
      r->inclusive = pl->inclusive.b;
    }
    return 1;
  }
  if (span_equals(s, &env->kind.span, "ack")) {
    r->kind = EGRESS_KIND_ACK;
    if (pl->t == T_NONE) return 1;
    if (pl->t != T_OBJECT) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "ack payload must be an object");
    if (pl->count.t != T_NONE) {
      if (!slot_integer(s, &pl->count, 1, 1, &d)) return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "count must be a non-negative integer");
      copy_number(s, &pl->count, &r->count);
    }
    return 1;
  }
  return fail(s, EGRESS_CODE_BAD_ENVELOPE, at, "unknown frame kind");
}

static int parse_properties(state *s, size_t start, size_t end, egress_scan_result *r) {
  s->end = end;
  size_t i = start;
  skip_ws(s, &i);
  if (i >= end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "empty properties");
  if (s->p[i] != '{') {
    if (!skip_value(s, &i, 0)) return 0;
  } else {
    i++;
    int first = 1, m;
    egress_span key;
    slot session = {0}, aggregate = {0}, frame_bytes = {0};
    struct {
      uint8_t t;
      slot aggregate_id, seq;
    } position = {0};
    while ((m = next_member(s, &i, &first, &key)) == 1) {
      int ok;
      switch (key_id(s, &key)) {
        case K_SESSION_ID: ok = read_slot(s, &i, 1, &session); break;
        case K_AGGREGATE_ID: ok = read_slot(s, &i, 1, &aggregate); break;
        case K_FRAME_BYTES: ok = read_slot(s, &i, 1, &frame_bytes); break;
        case K_BLOB_POSITION:
          memset(&position, 0, sizeof position);
          skip_ws(s, &i);
          if (i < end && s->p[i] == '{') {
            position.t = T_OBJECT;
            i++;
            int pfirst = 1, pm;
            egress_span pkey;
            ok = 1;
            while (ok && (pm = next_member(s, &i, &pfirst, &pkey)) == 1) {
              switch (key_id(s, &pkey)) {
                case K_AGGREGATE_ID: ok = read_slot(s, &i, 2, &position.aggregate_id); break;
                case K_SEQ: ok = read_slot(s, &i, 2, &position.seq); break;
                default: ok = skip_value(s, &i, 2); break;
              }
            }
            if (ok && pm < 0) ok = 0;
          } else {
            position.t = T_OTHER;
            ok = skip_value(s, &i, 1);
          }
          break;
        default: ok = skip_value(s, &i, 1); break;
      }
      if (!ok) return 0;
    }
    if (m < 0) return 0;
    if (session.t == T_STRING) r->properties_session_id = session.span;
    if (aggregate.t == T_STRING) r->properties_aggregate_id = aggregate.span;
    r->is_blob_reference = position.t == T_OBJECT && position.aggregate_id.t == T_STRING && position.seq.t == T_NUMBER &&
                           frame_bytes.t == T_NUMBER;
  }
  skip_ws(s, &i);
  if (i != end) return fail(s, EGRESS_CODE_INVALID_JSON, i, "unexpected data after the properties");
  return finish_segment(s);
}

/* A syntax error is reported as invalid-utf8 when the segment holds ill-formed UTF-8 anywhere (the JS decoder decodes
   the whole segment first). Error path only. */
static void diagnose_utf8(state *s, size_t start, size_t end) {
  if (s->r->code != EGRESS_CODE_INVALID_JSON) return;
  size_t at = utf8_first_invalid(s->p, start, end);
  if (at == end) return;
  fail(s, EGRESS_CODE_INVALID_UTF8, at, "invalid UTF-8 sequence");
}

void egress_scan(const uint8_t *data, size_t length, egress_scan_result *r) {
  memset(r, 0, sizeof *r);
  state s = {data, length, r, EGRESS_CODE_NONE, 0, NULL, NULL};
  if (length > EGRESS_SCAN_MAX_BYTES) {
    fail(&s, EGRESS_CODE_TOO_LARGE, 0, "frame exceeds the 4 MiB limit");
    return;
  }
  if (length < 4) {
    fail(&s, EGRESS_CODE_BAD_HEADER, 0, "truncated header");
    return;
  }
  size_t envelope_length = (size_t)data[0] | ((size_t)data[1] << 8) | ((size_t)data[2] << 16) | ((size_t)data[3] << 24);
  if (envelope_length > length - 4) {
    fail(&s, EGRESS_CODE_BAD_HEADER, 0, "envelope length exceeds frame");
    return;
  }
  size_t envelope_end = 4 + envelope_length;
  envelope_t env;
  memset(&env, 0, sizeof env);
  if (!parse_envelope(&s, 4, envelope_end, &env)) {
    diagnose_utf8(&s, 4, envelope_end);
    return;
  }
  if (!validate_envelope(&s, &env, 4, r)) return;
  if (r->kind == EGRESS_KIND_EVENT) {
    r->properties_start = envelope_end;
    r->properties_end = length;
    if (!parse_properties(&s, envelope_end, length, r)) {
      diagnose_utf8(&s, envelope_end, length);
      return;
    }
  } else if (envelope_end != length) {
    fail(&s, EGRESS_CODE_BAD_ENVELOPE, envelope_end, "unexpected bytes after a frame without properties");
    return;
  }
  r->ok = 1;
}
