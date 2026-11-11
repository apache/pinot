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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utilities for Pinot's `MAP` column representation.
///
/// Write paths come in two flavors:
/// - `sortByKey = true` (default) — entries are sorted by key and nested maps are also key-sorted via
///   [SerializationFeature#ORDER_MAP_ENTRIES_BY_KEYS]. Output is canonical: a pure function of the logical map.
/// - `sortByKey = false` — entries written in the input map's iteration order with no nested key sort. Faster, but
///   bytes are not a pure function of the logical map.
///
/// Read paths are identical regardless of how the input was written.
public class MapUtils {
  private MapUtils() {
  }

  /// Immutable MAP key with its UTF-8 representation cached for selective lookup hot paths.
  public static final class PreparedMapKey {
    private final String _key;
    private final byte[] _utf8Bytes;

    public PreparedMapKey(String key) {
      _key = key;
      _utf8Bytes = Utf8Utils.encode(key);
    }

    public String getKey() {
      return _key;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(MapUtils.class);

  // Pinot's standard ObjectMapper config (JSR-310 / ISO-8601 for LocalDate / LocalTime — see JsonUtils),
  // plus key-ordered output for the sorted variant so byte output is a pure function of the logical map.
  // Writes only — read paths delegate to JsonUtils, whose DEFAULT_READER is already JSR-310-aware.
  private static final ObjectWriter SORTED_WRITER = JsonUtils.newObjectMapperWithJavaTime()
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).writer();
  private static final ObjectWriter UNSORTED_WRITER = JsonUtils.newObjectMapperWithJavaTime().writer();

  /// Serializes a map into a length-prefixed binary frame: `[size][keyLen][keyBytes][valueLen][valueBytes]...`.
  /// Entries are sorted by key before writing for canonical output.
  public static byte[] serializeMap(Map<String, Object> map) {
    return serializeMap(map, true);
  }

  /// Variant of [#serializeMap(Map)] that lets the caller skip the per-entry key sort when canonical output is not
  /// required. `sortByKey = false` writes entries in the input map's iteration order and skips the per-value
  /// nested-map key sort — faster, but the bytes are no longer a pure function of the logical map.
  public static byte[] serializeMap(Map<String, Object> map, boolean sortByKey) {
    int size = map.size();
    if (size == 0) {
      return new byte[Integer.BYTES];
    }
    ObjectWriter writer = sortByKey ? SORTED_WRITER : UNSORTED_WRITER;

    // Sorted path on a non-SortedMap: copy entries to a sortable array. Otherwise iterate the entrySet directly to
    // avoid the array allocation (`SortedMap.entrySet()` is already sorted; the unsorted path doesn't care).
    Collection<Entry<String, Object>> entries;
    if (sortByKey && !(map instanceof SortedMap)) {
      //noinspection unchecked
      Entry<String, Object>[] sorted = map.entrySet().toArray(new Entry[0]);
      Arrays.sort(sorted, Entry.comparingByKey());
      entries = Arrays.asList(sorted);
    } else {
      entries = map.entrySet();
    }

    // First pass: serialize values and accumulate the total buffer size (4-byte map size + per-entry 4-byte key/value
    // lengths plus key/value bytes).
    long bufferSize = (1 + 2 * (long) size) * Integer.BYTES;
    byte[][] keyBytesArray = new byte[size][];
    byte[][] valueBytesArray = new byte[size][];
    int index = 0;
    for (Entry<String, Object> entry : entries) {
      byte[] keyBytes = Utf8Utils.encode(entry.getKey());
      keyBytesArray[index] = keyBytes;
      bufferSize += keyBytes.length;
      byte[] valueBytes;
      try {
        valueBytes = writer.writeValueAsBytes(entry.getValue());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      valueBytesArray[index] = valueBytes;
      bufferSize += valueBytes.length;
      index++;
    }
    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");

    // Second pass: emit into a single pre-sized buffer.
    byte[] bytes = new byte[(int) bufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(size);
    for (int i = 0; i < size; i++) {
      byte[] keyBytes = keyBytesArray[i];
      byteBuffer.putInt(keyBytes.length);
      byteBuffer.put(keyBytes);
      byte[] valueBytes = valueBytesArray[i];
      byteBuffer.putInt(valueBytes.length);
      byteBuffer.put(valueBytes);
    }
    return bytes;
  }

  /// Returns the byte size that [#serializeMap] would produce for the given map. Sort order does not affect the size.
  /// Streams each value through Jackson into a counting sink so no per-value `byte[]` is allocated — Jackson's
  /// per-thread `BufferRecycler` provides the encoding buffer.
  public static int serializedSize(Map<String, Object> map) {
    int size = map.size();
    if (size == 0) {
      return Integer.BYTES;
    }
    long bufferSize = (1 + 2 * (long) size) * Integer.BYTES;
    CountingOutputStream sink = new CountingOutputStream();
    for (Entry<String, Object> entry : map.entrySet()) {
      bufferSize += Utf8Utils.encodedLength(entry.getKey());
      try {
        UNSORTED_WRITER.writeValue(sink, entry.getValue());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    bufferSize += sink._count;
    Preconditions.checkState(bufferSize <= Integer.MAX_VALUE, "Buffer size exceeds 2GB");
    return (int) bufferSize;
  }

  /// Discards bytes and counts them. Used by [#serializedSize] to measure JSON output without allocating a buffer.
  private static final class CountingOutputStream extends OutputStream {
    private long _count;

    @Override
    public void write(int b) {
      _count++;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      _count += len;
    }
  }

  public static Map<String, Object> deserializeMap(byte[] bytes) {
    return deserializeMap(ByteBuffer.wrap(bytes));
  }

  public static Map<String, Object> deserializeMap(ByteBuffer byteBuffer) {
    int size = byteBuffer.getInt();
    if (size == 0) {
      return Map.of();
    }
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      String key = Utf8Utils.decode(readLengthPrefixed(byteBuffer));
      byte[] valueBytes = readLengthPrefixed(byteBuffer);
      try {
        Object value = JsonUtils.bytesToObject(valueBytes, Object.class);
        map.put(key, value);
      } catch (IOException e) {
        LOGGER.error("Caught exception while deserializing value for key: {}", key, e);
      }
    }
    return map;
  }

  /// Deserializes only the value for the requested key from a length-prefixed MAP frame.
  /// Non-matching keys and values are skipped without allocating byte arrays or invoking Jackson.
  ///
  /// @param bytes Serialized MAP frame
  /// @param key Key whose value should be deserialized
  /// @return Deserialized value, or `null` if the key is missing, has a null value, or its JSON value cannot be
  /// deserialized
  /// @throws BufferUnderflowException if the MAP frame is malformed or truncated
  @Nullable
  public static Object deserializeMapEntryValue(byte[] bytes, String key) {
    return deserializeMapEntryValue(ByteBuffer.wrap(bytes), new PreparedMapKey(key));
  }

  /// Variant of [#deserializeMapEntryValue(byte[], String)] that reuses a pre-encoded MAP key.
  @Nullable
  public static Object deserializeMapEntryValue(byte[] bytes, PreparedMapKey key) {
    return deserializeMapEntryValue(ByteBuffer.wrap(bytes), key);
  }

  /// Variant of [#deserializeMapEntryValue(byte[], String)] that reads from the supplied buffer without copying the
  /// complete MAP frame.
  ///
  /// Consumes the buffer from its current position and forces [ByteOrder#BIG_ENDIAN] on it — the write path frames
  /// lengths through a big-endian [ByteBuffer], while an off-heap view inherits the platform's native order.
  ///
  /// @throws BufferUnderflowException if the MAP frame is malformed or truncated
  @Nullable
  public static Object deserializeMapEntryValue(ByteBuffer byteBuffer, String key) {
    return deserializeMapEntryValue(byteBuffer, new PreparedMapKey(key));
  }

  /// Variant of [#deserializeMapEntryValue(ByteBuffer, String)] that reuses a pre-encoded MAP key.
  ///
  /// @throws BufferUnderflowException if the MAP frame is malformed or truncated
  @Nullable
  public static Object deserializeMapEntryValue(ByteBuffer byteBuffer, PreparedMapKey key) {
    byte[] valueBytes = findValueBytes(byteBuffer, key);
    if (valueBytes == null) {
      return null;
    }
    try {
      return JsonUtils.bytesToObject(valueBytes, Object.class);
    } catch (IOException e) {
      LOGGER.error("Caught exception while deserializing value for key: {}", key.getKey(), e);
      return null;
    }
  }

  @Nullable
  public static String deserializeMapEntryValueAsString(ByteBuffer byteBuffer, String key) {
    return deserializeMapEntryValueAsString(byteBuffer, new PreparedMapKey(key));
  }

  /// Reads the value for a prepared key as a string, skipping Jackson for the value shapes whose stored bytes are
  /// already exactly what `toString()` on the parsed value would produce.
  ///
  /// Consumes the buffer from its current position and forces [ByteOrder#BIG_ENDIAN] on it, exactly as
  /// [#deserializeMapEntryValue(ByteBuffer, PreparedMapKey)] does, so a caller must not assume the buffer is
  /// reusable afterwards. A truncated or corrupt frame raises [BufferUnderflowException].
  @Nullable
  public static String deserializeMapEntryValueAsString(ByteBuffer byteBuffer, PreparedMapKey key) {
    byte[] valueBytes = findValueBytes(byteBuffer, key);
    if (valueBytes == null) {
      return null;
    }
    String decoded = decodeWithoutJackson(valueBytes);
    if (decoded != null) {
      return decoded;
    }
    try {
      Object value = JsonUtils.bytesToObject(valueBytes, Object.class);
      return value == null ? null : value.toString();
    } catch (IOException e) {
      LOGGER.error("Caught exception while deserializing value for key: {}", key.getKey(), e);
      return null;
    }
  }

  /// Renders a stored JSON value without Jackson when the bytes are already identical to `toString()` on whatever
  /// Jackson would have parsed them into, otherwise returns `null` so the caller falls back.
  ///
  /// Covers plain strings, canonical integers and the two boolean literals. Non-integral numbers deliberately fall
  /// back: Jackson binds them to `Double`, whose `toString()` re-normalizes, so `1.50` has to render as `1.5`.
  @Nullable
  private static String decodeWithoutJackson(byte[] valueBytes) {
    int length = valueBytes.length;
    if (length == 0) {
      return null;
    }
    switch (valueBytes[0]) {
      case '"':
        return unquotePlainJsonString(valueBytes, length);
      case 't':
        return length == 4 && valueBytes[1] == 'r' && valueBytes[2] == 'u' && valueBytes[3] == 'e' ? "true" : null;
      case 'f':
        return length == 5 && valueBytes[1] == 'a' && valueBytes[2] == 'l' && valueBytes[3] == 's'
            && valueBytes[4] == 'e' ? "false" : null;
      default:
        return isCanonicalInteger(valueBytes, length)
            ? new String(valueBytes, 0, length, StandardCharsets.US_ASCII) : null;
    }
  }

  /// True when the bytes are an integer in the exact form `Integer`, `Long` and `BigInteger` render - which is what
  /// Jackson binds an integral JSON number to, so the stored bytes and `toString()` coincide. Leading zeros, `+`,
  /// `-0`, decimal points and exponents are all rejected because they would re-render differently.
  private static boolean isCanonicalInteger(byte[] valueBytes, int length) {
    int start = valueBytes[0] == '-' ? 1 : 0;
    if (length == start) {
      return false;
    }
    if (valueBytes[start] == '0') {
      // Bare "0" is canonical; "-0" renders as "0" and anything longer has a leading zero.
      return length == 1;
    }
    for (int i = start; i < length; i++) {
      if (valueBytes[i] < '0' || valueBytes[i] > '9') {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private static String unquotePlainJsonString(byte[] valueBytes, int length) {
    if (length < 2 || valueBytes[length - 1] != '"') {
      return null;
    }
    for (int i = 1; i < length - 1; i++) {
      if (valueBytes[i] == '\\') {
        return null;
      }
    }
    return new String(valueBytes, 1, length - 2, StandardCharsets.UTF_8);
  }

  @Nullable
  private static byte[] findValueBytes(ByteBuffer byteBuffer, PreparedMapKey key) {
    byteBuffer.order(ByteOrder.BIG_ENDIAN);
    int size = byteBuffer.getInt();
    if (size < 0) {
      throw new BufferUnderflowException();
    }
    if (size == 0) {
      return null;
    }
    byte[] keyBytes = key._utf8Bytes;
    int keyBytesLength = keyBytes.length;
    for (int i = 0; i < size; i++) {
      int keyLength = byteBuffer.getInt();
      // Bounds-check up front so the absolute gets below are provably in range, and so a truncated frame still
      // surfaces as BufferUnderflowException rather than IndexOutOfBoundsException.
      checkLength(byteBuffer, keyLength);
      // Compare through absolute gets so a length mismatch or a differing byte skips the rest of the key outright,
      // rather than walking it one relative get at a time just to advance the position.
      boolean matches = keyLength == keyBytesLength;
      if (matches) {
        int keyOffset = byteBuffer.position();
        for (int j = 0; j < keyLength; j++) {
          if (byteBuffer.get(keyOffset + j) != keyBytes[j]) {
            matches = false;
            break;
          }
        }
      }
      byteBuffer.position(byteBuffer.position() + keyLength);

      int valueLength = byteBuffer.getInt();
      checkLength(byteBuffer, valueLength);
      if (!matches) {
        byteBuffer.position(byteBuffer.position() + valueLength);
        continue;
      }
      // Keys within a frame are unique - the write path iterates a Map - so the first match is the only match and
      // the remaining entries never need to be scanned.
      byte[] valueBytes = new byte[valueLength];
      byteBuffer.get(valueBytes);
      return valueBytes;
    }
    return null;
  }

  private static byte[] readLengthPrefixed(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    byte[] bytes = new byte[length];
    byteBuffer.get(bytes);
    return bytes;
  }

  private static void checkLength(ByteBuffer byteBuffer, int length) {
    if (length < 0 || length > byteBuffer.remaining()) {
      throw new BufferUnderflowException();
    }
  }

  /// Renders a serialized MAP frame as a JSON object without materializing the map.
  ///
  /// The frame already stores each value as the JSON bytes that [#serializeMap] produced, so the values are copied
  /// through verbatim and only the keys are quoted. That skips the parse-into-`HashMap`-then-serialize-again round
  /// trip [#toString(Map)] performs, and it skips Jackson entirely.
  ///
  /// Entries are emitted in frame order. Both forward-index write paths (`ForwardIndexCreator#putValue` at segment
  /// build and `MutableSegmentImpl` while consuming) frame maps through the key-sorting [#serializeMap(Map)], so for
  /// those frames this is byte-identical to `toString(deserializeMap(frame))`. A frame written through
  /// [#serializeMap(Map, boolean)] with `sortByKey = false` renders in its own insertion order instead.
  public static String frameToJsonString(byte[] bytes) {
    return frameToJsonString(ByteBuffer.wrap(bytes));
  }

  /// Variant of [#frameToJsonString(byte\[\])] reading from the buffer's current position.
  public static String frameToJsonString(ByteBuffer byteBuffer) {
    byteBuffer.order(ByteOrder.BIG_ENDIAN);
    int size = byteBuffer.getInt();
    if (size == 0) {
      return "{}";
    }
    // Quoting a key adds 2 bytes and the separators add 2, while the two length prefixes it replaces free up 8, so
    // an unescaped rendering never exceeds the remaining frame bytes. Escaping is the only path that can grow.
    JsonBuilder builder = new JsonBuilder(byteBuffer.remaining() + 2);
    builder.append((byte) '{');
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        builder.append((byte) ',');
      }
      int keyLength = byteBuffer.getInt();
      checkLength(byteBuffer, keyLength);
      builder.appendQuotedKey(byteBuffer, keyLength);
      byteBuffer.position(byteBuffer.position() + keyLength);
      builder.append((byte) ':');
      int valueLength = byteBuffer.getInt();
      checkLength(byteBuffer, valueLength);
      builder.appendRaw(byteBuffer, valueLength);
      byteBuffer.position(byteBuffer.position() + valueLength);
    }
    builder.append((byte) '}');
    return builder.toUtf8String();
  }

  /// Growable byte sink for [#frameToJsonString]. Assembling UTF-8 bytes and decoding once at the end avoids
  /// decoding every key individually, which is what makes the common all-ASCII frame allocation-light.
  private static final class JsonBuilder {
    private byte[] _bytes;
    private int _length;

    private JsonBuilder(int capacity) {
      _bytes = new byte[capacity];
    }

    private void ensure(int extra) {
      if (_length + extra > _bytes.length) {
        _bytes = Arrays.copyOf(_bytes, Math.max(_length + extra, _bytes.length * 2));
      }
    }

    private void append(byte b) {
      ensure(1);
      _bytes[_length++] = b;
    }

    /// Copies `length` bytes from the buffer's current position without advancing it.
    private void appendRaw(ByteBuffer byteBuffer, int length) {
      ensure(length);
      int offset = byteBuffer.position();
      for (int i = 0; i < length; i++) {
        _bytes[_length++] = byteBuffer.get(offset + i);
      }
    }

    /// Writes the key as a JSON string. Keys needing no escaping - the overwhelming majority - are copied as raw
    /// UTF-8; anything else falls back to decoding the key and letting Jackson escape it.
    private void appendQuotedKey(ByteBuffer byteBuffer, int length) {
      int offset = byteBuffer.position();
      boolean clean = true;
      for (int i = 0; i < length; i++) {
        byte b = byteBuffer.get(offset + i);
        // Signed bytes below 0x20 are control characters; continuation bytes of multi-byte UTF-8 are negative and
        // need no escaping, so only the ASCII range is inspected.
        if ((b >= 0 && b < 0x20) || b == '"' || b == '\\') {
          clean = false;
          break;
        }
      }
      append((byte) '"');
      if (clean) {
        appendRaw(byteBuffer, length);
      } else {
        byte[] keyBytes = new byte[length];
        for (int i = 0; i < length; i++) {
          keyBytes[i] = byteBuffer.get(offset + i);
        }
        byte[] escaped = JsonStringEncoder.getInstance().quoteAsUTF8(Utf8Utils.decode(keyBytes));
        ensure(escaped.length);
        System.arraycopy(escaped, 0, _bytes, _length, escaped.length);
        _length += escaped.length;
      }
      append((byte) '"');
    }

    private String toUtf8String() {
      return new String(_bytes, 0, _length, StandardCharsets.UTF_8);
    }
  }

  public static String toString(Map<String, Object> map) {
    return toString(map, true);
  }

  /// `sortByKey = false` skips key sorting (top-level and nested) for faster serialization when canonical output is
  /// not required.
  public static String toString(Map<String, Object> map, boolean sortByKey) {
    try {
      return (sortByKey ? SORTED_WRITER : UNSORTED_WRITER).writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Object> fromString(String json) {
    try {
      return JsonUtils.stringToMap(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
