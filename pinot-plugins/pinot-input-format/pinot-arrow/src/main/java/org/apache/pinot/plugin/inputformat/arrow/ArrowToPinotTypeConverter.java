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
package org.apache.pinot.plugin.inputformat.arrow;

import com.google.common.collect.Maps;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 * Stateless schema-driven Arrow → Pinot value converter shared by the row-major and
 * column-major Arrow ingestion paths.
 *
 * <p>The conversion mirrors the contract established by {@link ArrowRecordExtractor} prior to
 * this extraction (see apache/pinot#18434 for the original row-major refactor): one branch per
 * {@link ArrowType.ArrowTypeID}, with complex types recursing through their child {@link Field}s
 * and scalars normalising per Pinot's expected JDK types.
 *
 * <p>Reused by:
 * <ul>
 *   <li>{@link ArrowRecordExtractor} — row-major ingestion via {@code ArrowRecordReader}
 *   <li>Column-major {@code ColumnReader} implementations that wrap Arrow vectors and need to
 *       emit Pinot-canonical JDK types from {@code getValue() / next()}
 * </ul>
 *
 * <p>All conversion methods are static; the only per-extraction state is the
 * {@code extractRawTimeValues} flag, passed through as a method parameter.
 */
public final class ArrowToPinotTypeConverter {

  private ArrowToPinotTypeConverter() {
  }

  /**
   * Convert an Arrow vector value to its Pinot-canonical JDK representation.
   *
   * @param field the Arrow {@link Field} describing the value's type
   * @param value the raw value emitted by {@code FieldVector.getObject(docId)}
   * @param extractRawTimeValues when {@code true}, {@code Date} / {@code Time} / {@code Timestamp}
   *                             surface as raw {@code int} / {@code long} in the schema's
   *                             {@link org.apache.arrow.vector.types.TimeUnit} instead of the
   *                             corresponding {@code java.time} / {@link Timestamp} contract type
   * @return the Pinot-canonical value, or {@code null} for {@link ArrowType.Null}
   */
  @Nullable
  public static Object toPinotValue(Field field, Object value, boolean extractRawTimeValues) {
    ArrowType type = field.getType();
    switch (type.getTypeID()) {
      // Pass-through — Arrow boxes these directly into the contract output type.
      case Bool:            // Boolean
      case FloatingPoint:   // Float / Double
      case Decimal:         // BigDecimal
      case Binary:          // byte[]
      case LargeBinary:     // byte[]
      case FixedSizeBinary: // byte[]
        return value;
      // toString — `Utf8` / `LargeUtf8` produce `String`; `Interval` / `Duration` produce ISO-8601
      // (`java.time.Period` / `java.time.Duration` / `PeriodDuration` all have meaningful toString).
      case Utf8:
      case LargeUtf8:
      case Interval:
      case Duration:
        return value.toString();
      // Integer — `Byte` widens to `Integer` per contract (sign-extended for signed `TinyIntVector`,
      // zero-extended via `& 0xFF` for unsigned `UInt1Vector`); `Short` (signed `SmallIntVector`)
      // sign-extends; `Character` (unsigned 16, from `UInt2Vector`) widens to its `int` code point;
      // `Integer` / `Long` pass through.
      case Int:
        if (value instanceof Byte) {
          int v = (Byte) value;
          return ((ArrowType.Int) type).getIsSigned() ? v : v & 0xFF;
        }
        if (value instanceof Short) {
          return ((Short) value).intValue();
        }
        if (value instanceof Character) {
          return (int) (Character) value;
        }
        return value;
      // Null — NullVector.getObject always returns null; callers should short-circuit on null,
      // so this branch is unreachable in practice. Defensive return.
      case Null:
        return null;
      // Logical temporal — schema's `TimeUnit` drives the conversion.
      case Timestamp:
        return convertTimestamp((ArrowType.Timestamp) type, value, extractRawTimeValues);
      case Date:
        return convertDate((ArrowType.Date) type, value, extractRawTimeValues);
      case Time:
        return convertTime((ArrowType.Time) type, value, extractRawTimeValues);
      // Multi-value — `List` (and primitive-array lists) → `Object[]`.
      case List:
      case LargeList:
      case FixedSizeList:
        return convertList(field.getChildren().get(0), (List<?>) value, extractRawTimeValues);
      // Map / nested complex types.
      case Map:
        // The Map field's children are [entriesStruct]; the entries struct's children are
        // [keyField, valueField] (named per MapVector.KEY_NAME / VALUE_NAME).
        Field entriesField = field.getChildren().get(0);
        return convertMap(entriesField.getChildren().get(0), entriesField.getChildren().get(1),
            (List<?>) value, extractRawTimeValues);
      case Struct:
        return convertStruct(field.getChildren(), (Map<?, ?>) value, extractRawTimeValues);
      case Union:
        // The chosen branch isn't visible from the resolved value alone — dispatch by the value's
        // runtime Java type. Nested complex sub-branches fall back to `value.toString()`.
        return convertByRuntimeType(value);
      default:
        // `NONE` is a placeholder; any other ID is a future Arrow addition.
        throw new IllegalStateException("Unsupported Arrow type: " + type + " for field: " + field.getName());
    }
  }

  /// Constructs a [Timestamp] from an Arrow `Timestamp` value. No-TZ vectors surface as
  /// `LocalDateTime` (interpreted as UTC); with-TZ vectors surface as `Long` epoch counted in the
  /// schema's `TimeUnit`. Sub-millisecond precision is preserved via [TimestampUtils].
  /// With `extractRawTimeValues` the raw `long` epoch in the schema's `TimeUnit` is returned.
  private static Object convertTimestamp(ArrowType.Timestamp type, Object value, boolean extractRawTimeValues) {
    if (extractRawTimeValues) {
      if (value instanceof LocalDateTime) {
        // No-TZ vector — convert the LocalDateTime back to an epoch `long` in the declared unit.
        Instant instant = ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
        return toEpochInUnit(instant, type.getUnit());
      }
      // With-TZ vector — already raw `long` in the declared unit.
      return value;
    }
    if (value instanceof LocalDateTime) {
      return Timestamp.from(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
    }
    long raw = ((Number) value).longValue();
    switch (type.getUnit()) {
      case SECOND:
        return new Timestamp(raw * 1000L);
      case MILLISECOND:
        return new Timestamp(raw);
      case MICROSECOND:
        return TimestampUtils.fromMicrosSinceEpoch(raw);
      case NANOSECOND:
        return TimestampUtils.fromNanosSinceEpoch(raw);
      default:
        throw new IllegalStateException("Unsupported Timestamp unit: " + type.getUnit());
    }
  }

  private static long toEpochInUnit(Instant instant, org.apache.arrow.vector.types.TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return instant.getEpochSecond();
      case MILLISECOND:
        return instant.toEpochMilli();
      case MICROSECOND:
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
      case NANOSECOND:
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
      default:
        throw new IllegalStateException("Unsupported Timestamp unit: " + unit);
    }
  }

  /// Reduces an Arrow `Date` value to its contract Java type ([LocalDate]), or to `int`
  /// days-since-epoch when `extractRawTimeValues` is set. `DateDayVector` surfaces as `Integer`
  /// raw days; `DateMilliVector` surfaces as `LocalDateTime` at UTC midnight.
  private static Object convertDate(ArrowType.Date type, Object value, boolean extractRawTimeValues) {
    int days;
    switch (type.getUnit()) {
      case DAY:
        days = (Integer) value;
        break;
      case MILLISECOND:
        days = (int) ((LocalDateTime) value).toLocalDate().toEpochDay();
        break;
      default:
        throw new IllegalStateException("Unsupported Date unit: " + type.getUnit());
    }
    return extractRawTimeValues ? days : LocalDate.ofEpochDay(days);
  }

  /// Constructs a [LocalTime] from an Arrow `Time` value, dispatched by the schema's `TimeUnit`:
  /// `TimeMilliVector` surfaces as `LocalDateTime`; `TimeSecVector` as `Integer`;
  /// `TimeMicroVector` / `TimeNanoVector` as `Long`. All collapse onto nanoseconds-since-midnight.
  /// With `extractRawTimeValues` the raw count in the schema's `TimeUnit` is returned instead.
  private static Object convertTime(ArrowType.Time type, Object value, boolean extractRawTimeValues) {
    if (extractRawTimeValues) {
      if (value instanceof LocalDateTime) {
        // `TimeMilliVector` surfaces as `LocalDateTime`; raw is `int` ms since midnight.
        return (int) (((LocalDateTime) value).toLocalTime().toNanoOfDay() / 1_000_000L);
      }
      // `TimeSecVector` (Integer) / `TimeMicroVector` / `TimeNanoVector` (Long) — already raw.
      return value;
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toLocalTime();
    }
    long raw = ((Number) value).longValue();
    switch (type.getUnit()) {
      case SECOND:
        return LocalTime.ofSecondOfDay(raw);
      case MILLISECOND:
        return LocalTime.ofNanoOfDay(raw * 1_000_000L);
      case MICROSECOND:
        return LocalTime.ofNanoOfDay(raw * 1_000L);
      case NANOSECOND:
        return LocalTime.ofNanoOfDay(raw);
      default:
        throw new IllegalStateException("Unsupported Time unit: " + type.getUnit());
    }
  }

  private static Object[] convertList(Field elementField, List<?> list, boolean extractRawTimeValues) {
    int size = list.size();
    Object[] result = new Object[size];
    int i = 0;
    for (Object element : list) {
      result[i++] = element != null ? toPinotValue(elementField, element, extractRawTimeValues) : null;
    }
    return result;
  }

  /// Flattens an Arrow `Map` column's entry list (`List<Map<KEY_NAME, VALUE_NAME>>`) into a
  /// `Map<String, Object>`, recursing into each value via [#toPinotValue] and stringifying each
  /// key via [BaseRecordExtractor#stringifyMapKey] per the contract. Entries with a `null` key
  /// (input or post-conversion) are dropped.
  private static Map<String, Object> convertMap(Field keyField, Field valueField, List<?> entries,
      boolean extractRawTimeValues) {
    Map<String, Object> result = Maps.newLinkedHashMapWithExpectedSize(entries.size());
    for (Object entry : entries) {
      if (entry == null) {
        continue;
      }
      Map<?, ?> entryMap = (Map<?, ?>) entry;
      Object rawKey = entryMap.get(MapVector.KEY_NAME);
      if (rawKey == null) {
        continue;
      }
      Object convertedKey = toPinotValue(keyField, rawKey, extractRawTimeValues);
      if (convertedKey == null) {
        continue;
      }
      Object rawValue = entryMap.get(MapVector.VALUE_NAME);
      result.put(BaseRecordExtractor.stringifyMapKey(convertedKey),
          rawValue != null ? toPinotValue(valueField, rawValue, extractRawTimeValues) : null);
    }
    return result;
  }

  private static Map<String, Object> convertStruct(List<Field> childFields, Map<?, ?> value,
      boolean extractRawTimeValues) {
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(childFields.size());
    for (Field childField : childFields) {
      String name = childField.getName();
      Object rawValue = value.get(name);
      result.put(name, rawValue != null ? toPinotValue(childField, rawValue, extractRawTimeValues) : null);
    }
    return result;
  }

  /// Runtime-type dispatch used by the `Union` case (where the chosen branch isn't accessible
  /// from the resolved value). Mirrors the scalar handling of [#toPinotValue] for the common
  /// Arrow boxed types; nested complex types fall back to `value.toString()` because their child
  /// [Field]s aren't reachable from here.
  private static Object convertByRuntimeType(Object value) {
    if (value instanceof Number) {
      if (value instanceof Byte || value instanceof Short) {
        return ((Number) value).intValue();
      }
      return value;
    }
    if (value instanceof Boolean || value instanceof byte[]) {
      return value;
    }
    if (value instanceof Character) {
      // `UInt2Vector` surfaces as `Character`; widen to `int` per the Int(16) contract.
      return (int) (Character) value;
    }
    if (value instanceof LocalDateTime) {
      // Ambiguous between Timestamp / Date / Time — best-effort: treat as Timestamp UTC.
      return Timestamp.from(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
    }
    // `Text` (Utf8 / LargeUtf8), `Period` / `Duration` / `PeriodDuration` (Interval / Duration), and
    // anything unrecognized fall through to `toString()`.
    return value.toString();
  }
}
