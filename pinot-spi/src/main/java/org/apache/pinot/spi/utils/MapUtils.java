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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(MapUtils.class);
  private static final ObjectMapper SORTED_MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private static final ObjectMapper UNSORTED_MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
  };

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
    ObjectMapper mapper = sortByKey ? SORTED_MAPPER : UNSORTED_MAPPER;

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
        valueBytes = mapper.writeValueAsBytes(entry.getValue());
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
        UNSORTED_MAPPER.writeValue(sink, entry.getValue());
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
        Object value = UNSORTED_MAPPER.readValue(valueBytes, Object.class);
        map.put(key, value);
      } catch (IOException e) {
        LOGGER.error("Caught exception while deserializing value for key: {}", key, e);
      }
    }
    return map;
  }

  private static byte[] readLengthPrefixed(ByteBuffer byteBuffer) {
    int length = byteBuffer.getInt();
    byte[] bytes = new byte[length];
    byteBuffer.get(bytes);
    return bytes;
  }

  public static String toString(Map<String, Object> map) {
    return toString(map, true);
  }

  /// `sortByKey = false` skips key sorting (top-level and nested) for faster serialization when canonical output is
  /// not required.
  public static String toString(Map<String, Object> map, boolean sortByKey) {
    try {
      return (sortByKey ? SORTED_MAPPER : UNSORTED_MAPPER).writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Object> fromString(String json) {
    try {
      return UNSORTED_MAPPER.readValue(json, MAP_TYPE_REFERENCE);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
