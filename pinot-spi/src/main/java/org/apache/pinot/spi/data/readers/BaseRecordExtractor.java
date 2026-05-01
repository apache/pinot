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
package org.apache.pinot.spi.data.readers;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;


/// Default [RecordExtractor] implementation. Subclasses override only the bits the format needs.
///
/// @param <T> the format of the input record
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseRecordExtractor<T> implements RecordExtractor<T> {

  /// Include-list resolved from [#init]'s `fields` argument: empty when [#_extractAll] is `true`, otherwise the
  /// (immutable) set of column names to populate. Subclasses read this in their `extract` loop.
  protected Set<String> _fields = Set.of();

  /// `true` when `init(null/empty, ...)` was called — extract every field the input record exposes. Initialized to
  /// `true` so the pre-`init` state is self-consistent with [#_fields] being empty (both interpretations of "no
  /// include list yet" agree on extract-all).
  protected boolean _extractAll = true;

  /// {@inheritDoc}
  ///
  /// Resolves `fields` into [#_fields] / [#_extractAll] (`null` or empty → extract-all; otherwise an immutable
  /// copy), then delegates format-specific configuration to [#initConfig] (default no-op). Format-specific
  /// extractors override [#initConfig] for config rather than reimplementing this method.
  @Override
  public void init(@Nullable Set<String> fields, @Nullable RecordExtractorConfig config) {
    if (CollectionUtils.isEmpty(fields)) {
      _extractAll = true;
      _fields = Set.of();
    } else {
      _extractAll = false;
      _fields = Set.copyOf(fields);
    }
    initConfig(config);
  }

  /// Format-specific config hook called from [#init] after [#_fields] / [#_extractAll] are resolved. Default is a
  /// no-op; override when the extractor needs to read a [RecordExtractorConfig] subtype or wire up per-instance
  /// state (descriptor caches, multi-value delimiters, etc.).
  protected void initConfig(@Nullable RecordExtractorConfig config) {
  }

  /// Whether `fieldName` should be populated in the output [GenericRow]. Returns `true` when [#_extractAll] is set
  /// (no include list — every field is extracted) or when the explicit include list [#_fields] contains the name.
  /// Use this in `extract` to gate per-column `putValue` calls instead of inlining
  /// `_extractAll || _fields.contains(fieldName)`.
  protected final boolean shouldExtract(String fieldName) {
    return _extractAll || _fields.contains(fieldName);
  }

  /// {@inheritDoc}
  ///
  /// Dispatches `value` to the matching converter:
  /// - multi-value → [#convertMultiValue]
  /// - map → [#convertMap]
  /// - nested record → [#convertRecord]
  /// - everything else → [#convertSingleValue]
  ///
  /// The base impl never returns `null`; format-specific overrides may translate format-native null sentinels by
  /// returning `null`.
  @Nullable
  @Override
  public Object convert(Object value) {
    Object convertedValue;
    if (isMultiValue(value)) {
      convertedValue = convertMultiValue(value);
    } else if (isMap(value)) {
      convertedValue = convertMap(value);
    } else if (isRecord(value)) {
      convertedValue = convertRecord(value);
    } else {
      convertedValue = convertSingleValue(value);
    }
    return convertedValue;
  }

  /// Whether `value` is multi-value. Default: `Collection` or non-`byte[]` array.
  protected boolean isMultiValue(Object value) {
    return value instanceof Collection || (value.getClass().isArray() && !(value instanceof byte[]));
  }

  /// Whether `value` is a map. Default: `instanceof Map`.
  protected boolean isMap(Object value) {
    return value instanceof Map;
  }

  /// Whether `value` is a nested record. Default `false`; override for formats with nested record types.
  protected boolean isRecord(Object value) {
    return false;
  }

  /// Converts a multi-value ([Collection] / `Object[]` / primitive array) into `Object[]`, recursing on each
  /// element via [#convert]. The base impl never returns `null`; the return is marked nullable so format-specific
  /// overrides retain the option to translate format-native null sentinels (e.g. an empty list interpreted as
  /// `null`).
  @Nullable
  protected Object[] convertMultiValue(Object value) {
    if (value instanceof Collection) {
      return convertCollection((Collection) value);
    }
    if (value instanceof Object[]) {
      return convertArray((Object[]) value);
    }
    return convertPrimitiveArray(value);
  }

  protected Object[] convertCollection(Collection collection) {
    int numValues = collection.size();
    Object[] convertedValues = new Object[numValues];
    int index = 0;
    for (Object value : collection) {
      Object convertedValue = value != null ? convert(value) : null;
      convertedValues[index++] = convertedValue;
    }
    return convertedValues;
  }

  protected Object[] convertArray(Object[] array) {
    int numValues = array.length;
    Object[] convertedValues = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      Object value = array[i];
      Object convertedValue = value != null ? convert(value) : null;
      convertedValues[i] = convertedValue;
    }
    return convertedValues;
  }

  protected Object[] convertPrimitiveArray(Object array) {
    if (array instanceof int[]) {
      return ArrayUtils.toObject((int[]) array);
    }
    if (array instanceof long[]) {
      return ArrayUtils.toObject((long[]) array);
    }
    if (array instanceof float[]) {
      return ArrayUtils.toObject((float[]) array);
    }
    if (array instanceof double[]) {
      return ArrayUtils.toObject((double[]) array);
    }
    throw new IllegalArgumentException("Unsupported primitive array type: " + array.getClass().getName());
  }

  /// Converts a map, recursing on each value via [#convert]. Keys go through [#convertSingleValue]; entries with a
  /// null key — either input or post-conversion (a format-specific override may translate the key to `null` for a
  /// format-native null sentinel) — are dropped. The base impl never returns `null`; the return is marked nullable
  /// so format-specific overrides retain the option to translate format-native null sentinels.
  @Nullable
  protected Map<Object, Object> convertMap(Object value) {
    Map<Object, Object> map = (Map) value;
    Map<Object, Object> convertedMap = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      Object mapKey = entry.getKey();
      if (mapKey == null) {
        continue;
      }
      Object convertedKey = convertSingleValue(mapKey);
      if (convertedKey == null) {
        continue;
      }
      Object mapValue = entry.getValue();
      convertedMap.put(convertedKey, mapValue != null ? convert(mapValue) : null);
    }
    return convertedMap;
  }

  /// Converts a nested record into a `Map<String, Object>`. Default throws — override in formats that have nested
  /// records (Avro `GenericRecord`, Thrift `TBase`, ProtoBuf nested `Message`). Marked nullable so format-specific
  /// overrides retain the option to translate format-native null sentinels.
  @Nullable
  protected Map<Object, Object> convertRecord(Object value) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support nested records; override convertRecord() to enable.");
  }

  /// Converts a single value per the [RecordExtractor] contract:
  /// - `Byte` / `Short` widen to `Integer` so all small ints unify behind a single Pinot type
  /// - [BigInteger] widens to [BigDecimal] (Pinot has no `BigInteger` data type; downstream transforms handle
  ///   `BigDecimal` natively)
  /// - other `Number` (`Integer` / `Long` / `Float` / `Double` / `BigDecimal`) passes through
  /// - `Boolean` passes through
  /// - `byte[]` passes through
  /// - [Timestamp] passes through
  /// - `ByteBuffer` materializes to `byte[]` (sliced so the source buffer's position is not advanced)
  ///
  /// [Temporal] family (`java.time`):
  /// - [LocalDate] / [LocalTime] pass through (TZ-independent date / time-of-day)
  /// - [Instant] / [OffsetDateTime] / [ZonedDateTime] bridge to [Timestamp] via [Timestamp#from] — all three are
  ///   unambiguously TZ-anchored, so the conversion is lossless (sub-second nanos preserved)
  /// - [LocalDateTime] bridges to [Timestamp] interpreting the wall-clock as UTC
  /// - other [Temporal] types fall through to `value.toString()`
  ///
  /// Everything else falls back to `value.toString()`. The base impl never returns `null`; the return is marked
  /// nullable so format-specific overrides retain the option to translate format-native null sentinels.
  @Nullable
  protected Object convertSingleValue(Object value) {
    if (value instanceof Number) {
      if (value instanceof Byte || value instanceof Short) {
        return ((Number) value).intValue();
      }
      if (value instanceof BigInteger) {
        return new BigDecimal((BigInteger) value);
      }
      return value;
    }
    if (value instanceof Boolean || value instanceof byte[] || value instanceof Timestamp) {
      return value;
    }
    if (value instanceof ByteBuffer) {
      // ByteBuffer might be reused by the reader. Slice to avoid advancing the original buffer's position.
      ByteBuffer slice = ((ByteBuffer) value).slice();
      byte[] bytesValue = new byte[slice.limit()];
      slice.get(bytesValue);
      return bytesValue;
    }
    if (value instanceof Temporal) {
      if (value instanceof LocalDate || value instanceof LocalTime) {
        return value;
      }
      if (value instanceof Instant) {
        return Timestamp.from((Instant) value);
      }
      if (value instanceof OffsetDateTime) {
        return Timestamp.from(((OffsetDateTime) value).toInstant());
      }
      if (value instanceof ZonedDateTime) {
        return Timestamp.from(((ZonedDateTime) value).toInstant());
      }
      if (value instanceof LocalDateTime) {
        return Timestamp.from(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
      }
      // Other Temporal types (Year, YearMonth, OffsetTime, etc.) fall through to toString.
    }
    return value.toString();
  }
}
