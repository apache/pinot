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
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;


/// Default [RecordExtractor] implementation. Subclasses override only the bits the format needs.
///
/// [#convert] dispatches by shape:
/// - multi-value (`Collection` / non-`byte[]` array) → [#convertMultiValue] → `Object[]` (each element recursed)
/// - map → [#convertMap] → `Map<String, Object>` (keys converted via [#convertSingleValue] then stringified via
///   [#stringifyMapKey], each value recursed)
/// - nested record → [#convertRecord] (throws by default; override for formats with nested records)
/// - everything else → [#convertSingleValue]
///
/// [#convertSingleValue] applies universal normalizations for primitive types that any format extractor might
/// produce:
/// - `Byte` / `Short` widen to `Integer` so all small ints unify behind a single Pinot type
/// - `BigInteger` widens to `BigDecimal` (Pinot has no `BigInteger` data type; downstream transforms handle
///   `BigDecimal` natively)
/// - other `Number` (`Integer` / `Long` / `Float` / `Double` / `BigDecimal`) passes through
/// - `Boolean` passes through
/// - `byte[]` passes through
/// - `ByteBuffer` materializes to `byte[]` (sliced so the source buffer's position is not advanced)
/// - everything else falls back to `value.toString()`
///
/// **Logical types (DECIMAL / TIMESTAMP / DATE / TIME / UUID) are NOT handled here** — see [RecordExtractor]
/// for the contract. Format-specific extractors do the native-to-contract conversion themselves (e.g. the
/// Avro extractor walks the schema in its own `extract` and never reaches this dispatcher).
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

  /// Walks `value` through the dispatcher described on [BaseRecordExtractor] and returns the
  /// contract-shaped result (single value, `Object[]`, or `Map<Object, Object>`). Subclasses call this
  /// from their `extract` loop when the input is already a contract-friendly Java type tree
  /// (`Collection` / `Map` / nested record). Schema-driven subclasses (Avro, Parquet native) bypass this
  /// path entirely and produce contract values inline.
  ///
  /// The base impl never returns `null`; format-specific overrides may translate format-native null
  /// sentinels by returning `null`. Java `null` inputs are short-circuited by callers and never reach
  /// this method.
  @Nullable
  protected Object convert(Object value) {
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
    Object[] convertedValues = new Object[collection.size()];
    int index = 0;
    for (Object value : collection) {
      convertedValues[index++] = value != null ? convert(value) : null;
    }
    return convertedValues;
  }

  protected Object[] convertArray(Object[] array) {
    int numValues = array.length;
    Object[] convertedValues = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      Object value = array[i];
      convertedValues[i] = value != null ? convert(value) : null;
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

  /// Converts a map, recursing on each value via [#convert]. Keys go through [#convertSingleValue] (so format-native
  /// raw types like `ByteBuffer` / `Byte` / `BigInteger` reach the contract form expected by [#stringifyMapKey])
  /// and are then stringified per the `Map<String, Object>` contract. Entries with a `null` key — either input or
  /// post-conversion (a format-specific override may translate the key to `null` for a format-native null sentinel)
  /// — are dropped. The base impl never returns `null`; the return is marked nullable so format-specific overrides
  /// retain the option to translate format-native null sentinels.
  @Nullable
  protected Map<String, Object> convertMap(Object value) {
    Map<Object, Object> map = (Map) value;
    Map<String, Object> convertedMap = Maps.newHashMapWithExpectedSize(map.size());
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
      convertedMap.put(stringifyMapKey(convertedKey), mapValue != null ? convert(mapValue) : null);
    }
    return convertedMap;
  }

  /// Stringifies a map key per the `RecordExtractor` `Map<String, Object>` contract. Single source of truth
  /// for map-key stringification across every format extractor:
  /// - `byte[]` → base64 (matches Jackson's `byte[]` value serialization, so a serialized map reads
  ///   uniformly across keys and values).
  /// - `Timestamp` → ISO-8601 UTC via `Timestamp#toInstant().toString()` — JVM-TZ-stable and preserves
  ///   sub-millisecond nanos so distinct Parquet `TIMESTAMP_MICROS` / `TIMESTAMP_NANOS` / `INT96` keys
  ///   don't collapse into colliding entries. (Diverges from the value-side `WRITE_DATES_AS_TIMESTAMPS=true`
  ///   numeric-millis convention; matches Jackson's `WRITE_DATE_KEYS_AS_TIMESTAMPS=false` default for
  ///   date-typed map keys.)
  /// - Everything else (`String`, `Integer`, `Long`, `Float`, `Double`, `Boolean`, `BigDecimal`, `UUID`,
  ///   `LocalDate`, `LocalTime`) has a stable, TZ-independent `toString()`.
  public static String stringifyMapKey(Object key) {
    if (key instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) key);
    }
    if (key instanceof Timestamp) {
      return ((Timestamp) key).toInstant().toString();
    }
    return key.toString();
  }

  /// Converts a nested record into a `Map<String, Object>`. Default throws — override in formats that have nested
  /// records (Avro `GenericRecord`, Thrift `TBase`, ProtoBuf nested `Message`). Marked nullable so format-specific
  /// overrides retain the option to translate format-native null sentinels.
  @Nullable
  protected Map<String, Object> convertRecord(Object value) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support nested records; override convertRecord() to enable.");
  }

  /// Applies the universal primitive normalizations documented on [BaseRecordExtractor]. Format-specific extractors
  /// override this to add native-type handling (e.g. Avro `Instant` → `Timestamp`, ProtoBuf `EnumValueDescriptor`
  /// → `String`). The base impl never returns `null`; the return is marked nullable so format-specific overrides
  /// retain the option to translate format-native null sentinels.
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
    if (value instanceof Boolean || value instanceof byte[]) {
      return value;
    }
    if (value instanceof ByteBuffer) {
      // ByteBuffer might be reused by the reader. Slice to avoid advancing the original buffer's position.
      ByteBuffer slice = ((ByteBuffer) value).slice();
      byte[] bytesValue = new byte[slice.limit()];
      slice.get(bytesValue);
      return bytesValue;
    }
    return value.toString();
  }
}
