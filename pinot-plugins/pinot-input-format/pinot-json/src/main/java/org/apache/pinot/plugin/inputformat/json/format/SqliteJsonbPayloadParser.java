/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.json.format;

import com.google.common.collect.Maps;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/// Parses the <a href="https://sqlite.org/jsonb.html">SQLite JSONB</a> binary format (SQLite 3.45+).
///
/// Every element is a 1–9 byte header followed by a payload. The header's first byte packs the element type in
/// the low nibble and a payload-size descriptor in the high nibble; descriptors 12–15 mean the size is a big-
/// endian `uint8` / `uint16` / `uint32` / `uint64` in the following bytes, and 0–11 are the size itself.
///
/// Numbers are stored as their ASCII text, so integers narrow to `Integer` / `Long` / `BigInteger` and floats
/// to `Double`, matching the text-JSON value contract. `TEXTJ` strings/labels are JSON-unescaped (invalid
/// escapes are rejected); `TEXT5` additionally accepts JSON5 escapes.
///
/// The decoder expects one row per payload, so the top-level element must be an `OBJECT`.
class SqliteJsonbPayloadParser implements JsonPayloadParser {

  // Element types (low nibble of the header's first byte).
  private static final int TYPE_NULL = 0;
  private static final int TYPE_TRUE = 1;
  private static final int TYPE_FALSE = 2;
  private static final int TYPE_INT = 3;      // canonical decimal integer, ASCII
  private static final int TYPE_INT5 = 4;     // JSON5 integer (hex / leading sign), ASCII
  private static final int TYPE_FLOAT = 5;    // canonical float, ASCII
  private static final int TYPE_FLOAT5 = 6;   // JSON5 float, ASCII
  private static final int TYPE_TEXT = 7;     // raw text, no escapes
  private static final int TYPE_TEXTJ = 8;    // text with JSON escapes
  private static final int TYPE_TEXT5 = 9;    // text with JSON5 escapes
  private static final int TYPE_TEXTRAW = 10; // raw text needing quoting, no escapes
  private static final int TYPE_ARRAY = 11;
  private static final int TYPE_OBJECT = 12;

  private static final BigInteger INT_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
  private static final BigInteger INT_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
  private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

  // RFC 8259 number grammar for the canonical TYPE_INT / TYPE_FLOAT forms: an optional minus, an integer part
  // with no leading zeros, and (for floats) an optional fraction and exponent. No leading '+', NaN, Infinity or
  // hex -- the tokens Double.parseDouble / Long.parseLong would otherwise accept.
  private static final Pattern CANONICAL_INT = Pattern.compile("-?(?:0|[1-9][0-9]*)");
  private static final Pattern CANONICAL_NUMBER =
      Pattern.compile("-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?");

  // Bound nesting depth and numeric-token length to Jackson's StreamReadConstraints defaults, so this binary
  // path rejects the same pathological shapes text JSON already rejects via JsonUtils.bytesToMap. The depth cap
  // is the important one: each nesting level is only ~1-3 wire bytes, so without it a small message can recurse
  // deep enough to throw StackOverflowError -- an Error, not an Exception, that escapes the decode-error
  // handling and fails the whole consuming segment instead of dropping one bad record.
  private static final int MAX_NESTING_DEPTH = 1000;
  private static final int MAX_NUMBER_LENGTH = 1000;

  @Override
  public boolean matches(byte[] payload, int offset, int length) {
    // The top-level element must be an OBJECT (low nibble == 12) to produce a row. That never collides with
    // text JSON, whose first byte is '{' (0x7B) or '[' (0x5B) -- both low nibble 0x0B (ARRAY), never 0x0C.
    if (length < 1 || (payload[offset] & 0x0F) != TYPE_OBJECT) {
      return false;
    }
    // The nibble alone is a weak signal: one byte in sixteen of arbitrary binary would claim the payload. Also
    // require the object's declared size to exactly fill the payload -- the same validity rule parse() enforces
    // -- so AUTO cannot hand a corrupt message to this parser on the strength of a single nibble.
    int sizeDescriptor = (payload[offset] & 0xFF) >>> 4;
    int headerLength = headerLength(sizeDescriptor);
    if (length < headerLength) {
      return false;
    }
    return declaredSize(payload, offset, sizeDescriptor) == (long) length - headerLength;
  }

  /// Bytes occupied by an element header: the type/size-descriptor byte plus the width of any explicit size
  /// field. Descriptors 0-11 encode the size in the byte itself; 12-15 prepend a 1-, 2-, 4- or 8-byte
  /// big-endian size.
  ///
  /// Shared by [#matches] and [#readElement] so detection and parsing can never disagree about the layout.
  private static int headerLength(int sizeDescriptor) {
    switch (sizeDescriptor) {
      case 12:
        return 2;
      case 13:
        return 3;
      case 14:
        return 5;
      case 15:
        return 9;
      default:
        return 1;
    }
  }

  /// Payload size declared by the element header starting at {@code offset}. The caller must first ensure
  /// [#headerLength] bytes are available. A descriptor-15 size with its sign bit set yields a negative value,
  /// which every caller rejects (it can neither be a valid length nor equal a payload length).
  private static long declaredSize(byte[] payload, int offset, int sizeDescriptor) {
    switch (sizeDescriptor) {
      case 12:
        return readUnsignedBE(payload, offset + 1, 1);
      case 13:
        return readUnsignedBE(payload, offset + 1, 2);
      case 14:
        return readUnsignedBE(payload, offset + 1, 4);
      case 15:
        return readUnsignedBE(payload, offset + 1, 8);
      default:
        return sizeDescriptor;
    }
  }

  /// Reads {@code count} big-endian bytes as an unsigned value. An 8-byte field with the sign bit set yields a
  /// negative long, which never equals a payload length and is rejected by the bounds checks.
  private static long readUnsignedBE(byte[] payload, int offset, int count) {
    long value = 0;
    for (int i = 0; i < count; i++) {
      value = (value << 8) | (payload[offset + i] & 0xFFL);
    }
    return value;
  }

  @Override
  public Map<String, Object> parse(byte[] payload, int offset, int length) {
    int limit = offset + length;
    Cursor cursor = new Cursor(payload, offset, limit);
    Object value = readElement(cursor, limit, 0);
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException("Top-level SQLite JSONB element must be an object");
    }
    // SQLite's validity rule: the outer element must exactly fill the BLOB. Without this, a payload whose
    // top-level element declares a short size (e.g. a bare 0x0C followed by data) would decode to a partial --
    // possibly empty -- row and silently discard the trailing bytes instead of rejecting a corrupt message.
    if (cursor._pos != limit) {
      throw new IllegalArgumentException(
          "Top-level SQLite JSONB element consumed " + (cursor._pos - offset) + " of " + length + " bytes");
    }
    //noinspection unchecked
    return (Map<String, Object>) value;
  }

  /// Reads one element. {@code parentEnd} is the exclusive end of the enclosing container (the whole payload at
  /// the top level); an element may never extend past it, otherwise a nested element could overrun its parent
  /// while still fitting the payload and silently swallow its following siblings.
  private static Object readElement(Cursor cursor, int parentEnd, int depth) {
    int header = cursor.peekUInt8();
    int type = header & 0x0F;
    int sizeDescriptor = header >>> 4;
    int headerStart = cursor._pos;
    // Decode the header through the same helpers matches() uses, so detection and parsing cannot drift apart.
    cursor.skip(headerLength(sizeDescriptor));
    long payloadSize = declaredSize(cursor._buf, headerStart, sizeDescriptor);
    int start = cursor._pos;
    int end = cursor.boundedEnd(start, payloadSize, parentEnd);
    Object result;
    switch (type) {
      case TYPE_NULL:
        result = null;
        break;
      case TYPE_TRUE:
        result = Boolean.TRUE;
        break;
      case TYPE_FALSE:
        result = Boolean.FALSE;
        break;
      case TYPE_INT:
        result = parseCanonicalInt(cursor.ascii(start, end));
        break;
      case TYPE_INT5:
        result = parseInt5(cursor.ascii(start, end));
        break;
      case TYPE_FLOAT:
        result = parseCanonicalFloat(cursor.ascii(start, end));
        break;
      case TYPE_FLOAT5:
        result = parseJson5Float(cursor.ascii(start, end));
        break;
      case TYPE_TEXT:
      case TYPE_TEXTRAW:
        result = cursor.utf8(start, end);
        break;
      case TYPE_TEXTJ:
        result = unescape(cursor.utf8(start, end), false);
        break;
      case TYPE_TEXT5:
        result = unescape(cursor.utf8(start, end), true);
        break;
      case TYPE_ARRAY:
        return readArray(cursor, end, depth);
      case TYPE_OBJECT:
        return readObject(cursor, end, depth);
      default:
        throw new IllegalArgumentException("Reserved/invalid SQLite JSONB element type: " + type);
    }
    cursor._pos = end;
    return result;
  }

  private static List<Object> readArray(Cursor cursor, int end, int depth) {
    int childDepth = nextDepth(depth);
    List<Object> list = new ArrayList<>();
    while (cursor._pos < end) {
      list.add(readElement(cursor, end, childDepth));
    }
    return list;
  }

  private static Map<String, Object> readObject(Cursor cursor, int end, int depth) {
    int childDepth = nextDepth(depth);
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(4);
    while (cursor._pos < end) {
      Object label = readElement(cursor, end, childDepth);
      if (!(label instanceof String)) {
        throw new IllegalArgumentException("SQLite JSONB object label must be text");
      }
      if (cursor._pos >= end) {
        throw new IllegalArgumentException("SQLite JSONB object is missing a value for label: " + label);
      }
      map.put((String) label, readElement(cursor, end, childDepth));
    }
    return map;
  }

  /// Returns {@code depth + 1} for the children of a container, throwing once nesting would exceed
  /// [#MAX_NESTING_DEPTH]. Keeps deep nesting on the `IllegalArgumentException` path (handled as a bad record)
  /// rather than letting the recursion overflow the stack with a `StackOverflowError`.
  private static int nextDepth(int depth) {
    if (depth >= MAX_NESTING_DEPTH) {
      throw new IllegalArgumentException("SQLite JSONB nesting exceeds the maximum depth of " + MAX_NESTING_DEPTH);
    }
    return depth + 1;
  }

  /// Parses a canonical (RFC 8259) integer, the SQLite `TYPE_INT` form. Validated as strictly as text JSON:
  /// the permissive tokens `Long.parseLong` / `BigInteger` accept -- a leading `+` or leading zeros -- are
  /// rejected, so a malformed `TYPE_INT` fails the record rather than decoding to a value Jackson would refuse.
  /// The JSON5 forms (hex, leading sign) live in [#parseInt5].
  private static Object parseCanonicalInt(String text) {
    if (!CANONICAL_INT.matcher(text).matches()) {
      throw new IllegalArgumentException("Invalid canonical SQLite JSONB integer: " + text);
    }
    return parseInt(text, 10);
  }

  private static Object parseInt(String text, int radix) {
    // Fast path: values that fit a long (the overwhelming majority) avoid BigInteger allocation.
    try {
      return narrowLong(Long.parseLong(text, radix));
    } catch (NumberFormatException e) {
      return narrow(new BigInteger(text, radix));
    }
  }

  /// Narrows an integer to the smallest of `Integer` / `Long` / `BigInteger` that holds it, mirroring the
  /// Jackson text-JSON contract (`BigInteger` is later widened to `BigDecimal` by the record extractor).
  private static Object narrow(BigInteger value) {
    if (value.compareTo(INT_MIN) >= 0 && value.compareTo(INT_MAX) <= 0) {
      return value.intValue();
    }
    if (value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0) {
      return value.longValue();
    }
    return value;
  }

  private static Object parseInt5(String text) {
    String digits = text;
    boolean negative = false;
    if (!digits.isEmpty() && (digits.charAt(0) == '+' || digits.charAt(0) == '-')) {
      negative = digits.charAt(0) == '-';
      digits = digits.substring(1);
    }
    Object magnitude;
    if (digits.length() > 2 && digits.charAt(0) == '0' && (digits.charAt(1) == 'x' || digits.charAt(1) == 'X')) {
      magnitude = parseInt(digits.substring(2), 16);
    } else {
      magnitude = parseInt(digits, 10);
    }
    if (!negative) {
      return magnitude;
    }
    return magnitude instanceof BigInteger ? narrow(((BigInteger) magnitude).negate())
        : narrowLong(-((Number) magnitude).longValue());
  }

  private static Object narrowLong(long value) {
    // NOTE: keep these as separate returns. A `cond ? (int) value : value` ternary would numerically promote
    // the int branch to long, always boxing to Long.
    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
      return (int) value;
    }
    return value;
  }

  /// Parses a canonical (RFC 8259) float, the SQLite `TYPE_FLOAT` form. `Double.parseDouble` is far more
  /// permissive than JSON -- it accepts `NaN`, `Infinity`, a leading `+`, Java hex floats and type suffixes --
  /// so the text is first validated against the JSON number grammar. That rejects the non-finite and other
  /// non-canonical tokens exactly as Jackson would, rather than ingesting them. The JSON5 forms live in
  /// [#parseJson5Float].
  private static Double parseCanonicalFloat(String text) {
    if (!CANONICAL_NUMBER.matcher(text).matches()) {
      throw new IllegalArgumentException("Invalid canonical SQLite JSONB float: " + text);
    }
    return Double.parseDouble(text);
  }

  private static Double parseJson5Float(String text) {
    // JSON5 permits a leading '+' and the tokens Infinity / NaN, all of which Double.parseDouble accepts once
    // the '+' is stripped.
    String normalized = !text.isEmpty() && text.charAt(0) == '+' ? text.substring(1) : text;
    return Double.parseDouble(normalized);
  }

  /// Single-pass JSON string unescape. Returns the input unchanged (no allocation) when it contains no
  /// backslash. Rejects invalid escapes for `TEXTJ`; when {@code json5} is set, also accepts the JSON5 escape
  /// extensions (`\'`, `\v`, `\0`, `\xHH`, line continuations, and `\<char>` passthrough).
  private static String unescape(String content, boolean json5) {
    int firstEscape = content.indexOf('\\');
    if (firstEscape < 0) {
      return content;
    }
    int n = content.length();
    StringBuilder sb = new StringBuilder(n);
    sb.append(content, 0, firstEscape);
    int i = firstEscape;
    while (i < n) {
      char c = content.charAt(i++);
      if (c != '\\') {
        sb.append(c);
        continue;
      }
      if (i >= n) {
        throw new IllegalArgumentException("Dangling escape in SQLite JSONB text");
      }
      char e = content.charAt(i++);
      switch (e) {
        case '"':
          sb.append('"');
          break;
        case '\\':
          sb.append('\\');
          break;
        case '/':
          sb.append('/');
          break;
        case 'b':
          sb.append('\b');
          break;
        case 'f':
          sb.append('\f');
          break;
        case 'n':
          sb.append('\n');
          break;
        case 'r':
          sb.append('\r');
          break;
        case 't':
          sb.append('\t');
          break;
        case 'u':
          sb.append((char) parseHex(content, i, 4));
          i += 4;
          break;
        default:
          i = appendJson5Escape(sb, content, e, i, json5);
          break;
      }
    }
    return sb.toString();
  }

  private static int appendJson5Escape(StringBuilder sb, String content, char e, int i, boolean json5) {
    if (!json5) {
      throw new IllegalArgumentException("Invalid JSON escape '\\" + e + "' in SQLite JSONB text");
    }
    switch (e) {
      case '\'':
        sb.append('\'');
        return i;
      case 'v':
        sb.append('\u000B');
        return i;
      case '0':
        sb.append('\0');
        return i;
      case 'x':
        sb.append((char) parseHex(content, i, 2));
        return i + 2;
      case '\n':
        return i; // line continuation
      case '\r':
        return i < content.length() && content.charAt(i) == '\n' ? i + 1 : i; // CRLF continuation
      default:
        sb.append(e); // \<char> -> char
        return i;
    }
  }

  private static int parseHex(String content, int start, int len) {
    if (start + len > content.length()) {
      throw new IllegalArgumentException("Truncated \\u / \\x escape in SQLite JSONB text");
    }
    int value = 0;
    for (int i = start; i < start + len; i++) {
      int digit = Character.digit(content.charAt(i), 16);
      if (digit < 0) {
        throw new IllegalArgumentException("Invalid hex escape in SQLite JSONB text");
      }
      value = (value << 4) | digit;
    }
    return value;
  }

  /// Sequential reader over a bounded region of a byte array. Multi-byte size fields are big-endian per the
  /// SQLite JSONB spec.
  private static final class Cursor {
    private final byte[] _buf;
    private final int _limit;
    private int _pos;

    private Cursor(byte[] buf, int start, int limit) {
      _buf = buf;
      _pos = start;
      _limit = limit;
    }

    /// The next byte, without advancing.
    private int peekUInt8() {
      if (_pos >= _limit) {
        throw new IllegalArgumentException("Truncated SQLite JSONB payload");
      }
      return _buf[_pos] & 0xFF;
    }

    /// Advances past {@code count} bytes, verifying they are present. Compares without adding to {@code _pos}
    /// so the check cannot overflow.
    private void skip(int count) {
      if (count > _limit - _pos) {
        throw new IllegalArgumentException("Truncated SQLite JSONB payload");
      }
      _pos += count;
    }

    /// Validates that a payload of {@code size} bytes starting at {@code start} fits inside the enclosing
    /// container (never merely inside the whole payload) and returns its exclusive end. Compares without adding
    /// to {@code start} so an adversarial `uint64` size cannot overflow past the guard. A {@code start} already
    /// past {@code parentEnd} — possible when a trailing element's header straddles the boundary — makes
    /// {@code parentEnd - start} negative and is therefore rejected too.
    private int boundedEnd(int start, long size, int parentEnd) {
      if (size < 0 || size > parentEnd - start) {
        throw new IllegalArgumentException("SQLite JSONB element size exceeds its enclosing container");
      }
      return start + (int) size;
    }

    /// Decodes a numeric token as ASCII. Numeric element types are this method's only callers, so it also
    /// enforces the numeric length cap here: `BigInteger(String)` parsing is ~O(n^2), and without a bound a
    /// single ~1 MB run of digits would be a per-message CPU sink with no analogue in text JSON, which caps
    /// number length the same way via Jackson's StreamReadConstraints.
    private String ascii(int start, int end) {
      if (end - start > MAX_NUMBER_LENGTH) {
        throw new IllegalArgumentException(
            "SQLite JSONB numeric token exceeds the maximum length of " + MAX_NUMBER_LENGTH);
      }
      return new String(_buf, start, end - start, StandardCharsets.US_ASCII);
    }

    /// Decodes text strictly as UTF-8. Unlike `new String(..., UTF_8)`, which substitutes U+FFFD for malformed
    /// bytes, a `CodingErrorAction.REPORT` decoder throws, so a corrupt JSONB record fails rather than ingesting
    /// mutated field names or values -- matching how text JSON / Jackson reject invalid UTF-8.
    private String utf8(int start, int end) {
      CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
      try {
        return decoder.decode(ByteBuffer.wrap(_buf, start, end - start)).toString();
      } catch (CharacterCodingException e) {
        throw new IllegalArgumentException("Invalid UTF-8 in SQLite JSONB text", e);
      }
    }
  }
}
