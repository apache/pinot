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
package org.apache.pinot.plugin.inputformat.parquet;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.apache.pinot.spi.utils.UuidUtils;


/// Extracts Pinot [GenericRow] from Parquet [Group] objects, using Parquet [LogicalTypeAnnotation]s to drive
/// LIST and MAP handling per the Parquet LogicalTypes spec backward-compatibility rules. Output adheres to
/// the `RecordExtractor` contract.
///
/// **Primitive types** — preserved as their boxed Java type:
/// - `BOOLEAN` → `Boolean`
/// - `INT32` → `Integer`
/// - `INT64` → `Long`
/// - `FLOAT` → `Float`
/// - `DOUBLE` → `Double`
/// - `BINARY` with `STRING` / `ENUM` annotation → `String`
/// - `BINARY` / `FIXED_LEN_BYTE_ARRAY` without annotation → `byte[]`
///
/// **Logical types** — Parquet logical-type annotations decoded into uniform Java types regardless of the
/// underlying physical encoding:
/// - `DECIMAL` (on `INT32` / `INT64` / `BINARY` / `FIXED_LEN_BYTE_ARRAY`) → `BigDecimal`. Always converted —
///   raw bytes aren't interpretable without external precision / scale
/// - `INT64` + `TIMESTAMP_MILLIS` / `MICROS` / `NANOS` → `Timestamp` (sub-millisecond nanos preserved via
///   `Timestamp#setNanos`), or `Long` raw value in the column's declared unit when raw
/// - `INT96` → `Timestamp` (sub-millisecond nanos preserved), or `Long` epoch nanos when raw (nanos is
///   INT96's natural unit since its physical encoding — nanos-of-day + Julian day — carries nanosecond
///   precision)
/// - `INT32` + `DATE` → `LocalDate`, or `Integer` raw days-since-epoch when raw
/// - `INT32` + `TIME_MILLIS` → `LocalTime`, or `Integer` raw ms-since-midnight when raw
/// - `INT64` + `TIME_MICROS` / `TIME_NANOS` → `LocalTime` (full nanosecond precision), or `Long` raw
///   value-since-midnight in the column's declared unit when raw
/// - `FIXED_LEN_BYTE_ARRAY(16)` + `UUID` → `java.util.UUID`. Always converted; the downstream type
///   transformer adapts to the Pinot column's storage type — `STRING` → canonical form, `BYTES` →
///   16-byte big-endian form
///
/// [ParquetNativeRecordExtractorConfig#isExtractRawTimeValues] opts out of TIMESTAMP / DATE / TIME conversion.
/// DECIMAL and UUID always convert.
///
/// **Multi-value:** `LIST`-annotated group (standard 3-level wrapper or legacy non-wrapper forms) → `Object[]`.
///
/// **Map / nested complex:** `MAP`-annotated group → `Map<Object, Object>`; plain non-annotated group →
/// `Map<String, Object>` (struct).
///
/// **Null:** field with zero repetition count → `null`.
public class ParquetNativeRecordExtractor extends BaseRecordExtractor<Group> {

  private boolean _extractRawTimeValues;

  @Override
  protected void initConfig(@Nullable RecordExtractorConfig config) {
    if (config instanceof ParquetNativeRecordExtractorConfig) {
      _extractRawTimeValues = ((ParquetNativeRecordExtractorConfig) config).isExtractRawTimeValues();
    }
  }

  @Override
  public GenericRow extract(Group from, GenericRow to) {
    // Parquet values bypass `convert()` — `extractValue` produces contract-typed values directly (Number,
    // Boolean, byte[], String, BigDecimal, LocalDate, LocalTime, Timestamp, plus Object[] / Map for nested
    // complex types via recursive extractValue). The base's universal normalizations (Byte/Short widening,
    // ByteBuffer slicing, BigInteger → BigDecimal) don't apply to anything Parquet produces.
    GroupType fromType = from.getType();
    if (_extractAll) {
      List<Type> fields = fromType.getFields();
      for (Type field : fields) {
        String fieldName = field.getName();
        to.putValue(fieldName, extractValue(from, fromType.getFieldIndex(fieldName)));
      }
    } else {
      for (String fieldName : _fields) {
        if (fromType.containsField(fieldName)) {
          to.putValue(fieldName, extractValue(from, fromType.getFieldIndex(fieldName)));
        }
      }
    }
    return to;
  }

  @Nullable
  private Object extractValue(Group from, int fieldIndex) {
    int numValues = from.getFieldRepetitionCount(fieldIndex);
    Type fieldType = from.getType().getType(fieldIndex);
    // REPEATED fields are always multi-valued — even when 0 or 1 occurrences are present, the contract is
    // `Object[]` (matching how LIST-annotated groups surface). For OPTIONAL / REQUIRED fields, 0 → null and
    // 1 → the scalar.
    if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
      Object[] results = new Object[numValues];
      for (int i = 0; i < numValues; i++) {
        results[i] = extractValue(from, fieldIndex, fieldType, i);
      }
      return results;
    }
    if (numValues == 0) {
      return null;
    }
    return extractValue(from, fieldIndex, fieldType, 0);
  }

  @Nullable
  private Object extractValue(Group from, int fieldIndex, Type fieldType, int index) {
    LogicalTypeAnnotation logicalTypeAnnotation = fieldType.getLogicalTypeAnnotation();
    if (fieldType.isPrimitive()) {
      PrimitiveType.PrimitiveTypeName primitiveTypeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
      switch (primitiveTypeName) {
        case BOOLEAN:
          return from.getBoolean(fieldIndex, index);
        case INT32:
          int intValue = from.getInteger(fieldIndex, index);
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
            return BigDecimal.valueOf(intValue, decimalLogicalTypeAnnotation.getScale());
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            // `DATE` (int32 days-since-epoch) → [LocalDate] (TZ-independent), or raw days when configured.
            return _extractRawTimeValues ? intValue : LocalDate.ofEpochDay(intValue);
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            // `TIME_MILLIS` (int32 millis-since-midnight) → [LocalTime], or raw ms when configured.
            return _extractRawTimeValues ? intValue : LocalTime.ofNanoOfDay(intValue * 1_000_000L);
          }
          return intValue;
        case INT64:
          long longValue = from.getLong(fieldIndex, index);
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
            return BigDecimal.valueOf(longValue, decimalLogicalTypeAnnotation.getScale());
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            // `TIME_MICROS` / `TIME_NANOS` → [LocalTime] preserving full nanosecond precision, or raw value
            // (in the column's declared unit) when configured.
            if (_extractRawTimeValues) {
              return longValue;
            }
            LogicalTypeAnnotation.TimeUnit timeUnit =
                ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation).getUnit();
            return LocalTime.ofNanoOfDay(timeUnit == LogicalTypeAnnotation.TimeUnit.MICROS ? longValue * 1_000L
                : longValue);
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            // `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` / `TIMESTAMP_NANOS` → [Timestamp] preserving sub-millisecond
            // nanos via `setNanos`. Raw mode returns the value as stored in the column's declared unit.
            // TODO: `isAdjustedToUTC()` is ignored — a `timestamp-* adjustToUTC=false` field is local-clock
            //   time, not a UTC instant, but we treat all values as UTC instants per the Pinot TIMESTAMP
            //   contract. For local-datetime data the produced `Timestamp` will be off by the JVM TZ offset.
            if (_extractRawTimeValues) {
              return longValue;
            }
            return ParquetUtils.convertLongToTimestamp(longValue,
                ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit());
          }
          return longValue;
        case INT96: {
          // INT96 has no Parquet logical-type spec, but its physical encoding (nanos-of-day + Julian day) is
          // nanosecond-precision, so nanos is the natural raw unit.
          long nanos = ParquetUtils.convertInt96ToEpochNanos(from.getInt96(fieldIndex, index).getBytes());
          return _extractRawTimeValues ? nanos : TimestampUtils.fromNanosSinceEpoch(nanos);
        }
        case FLOAT:
          return from.getFloat(fieldIndex, index);
        case DOUBLE:
          return from.getDouble(fieldIndex, index);
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
              || logicalTypeAnnotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            return from.getValueToString(fieldIndex, index);
          }
          byte[] binaryBytes = from.getBinary(fieldIndex, index).getBytes();
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return new BigDecimal(new BigInteger(binaryBytes),
                ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation).getScale());
          }
          if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
            // `FIXED_LEN_BYTE_ARRAY(16) + UUID` → [UUID] (always converted; the downstream type
            // transformer adapts to the Pinot column's storage type). UUID wire bytes are big-endian
            // per RFC 4122.
            return UuidUtils.fromBytes(binaryBytes);
          }
          return binaryBytes;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported field type: %s, primitive type: %s, logical type: %s", fieldType,
                  primitiveTypeName, logicalTypeAnnotation));
      }
    }
    Group group = from.getGroup(fieldIndex, index);
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      return extractList(group);
    }
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
      return extractKeyValueMap(group);
    }
    return extractStruct(group);
  }

  private Object[] extractList(Group group) {
    // Group is annotated with LIST. Per the Parquet LogicalTypes spec it always has exactly one child, the
    // repeated wrapper. The wrapper's schema (not the row data) decides which encoding we are reading, so we
    // resolve that once here and dispatch the whole list down a single branch.
    String parentListName = group.getType().getName();
    Type repeatedField = group.getType().getType(0);
    int numValues = group.getFieldRepetitionCount(0);
    Object[] values = new Object[numValues];
    if (isStandardListWrapper(repeatedField, parentListName)) {
      // Standard 3-level LIST encoding (modern Parquet writers, parquet-avro, Spark, Arrow, …):
      //   <list-rep> group <name> (LIST) {
      //     repeated group list {            // wrapper carries only repetition
      //       <elem-rep> <elem-type> element;
      //     }
      //   }
      // Per the Parquet LogicalTypes backward-compat rules this also covers any single-field repeated group whose
      // wrapper field is NOT named `array` or `<list>_tuple` (rule 4) — the inner field IS the element, regardless
      // of its name. Strip the wrapper so each row surfaces the bare element value, matching Apache Arrow /
      // parquet-avro (with `parquet.avro.add-list-element-records=false`).
      for (int i = 0; i < numValues; i++) {
        values[i] = extractValue(group.getGroup(0, i), 0);
      }
    } else {
      // Legacy non-wrapper repeated forms:
      //   * Repeated primitive: `repeated <elem-type> <name>;` (rule 1) — `repeatedField.isPrimitive()`.
      //   * Repeated multi-field group: `repeated group <name> { <fields…> };` (rule 2) — the group IS the element.
      //   * Repeated single-field group named `array` or `<list>_tuple` (rule 3) — the group IS the element,
      //     preserved as a struct/Map.
      // In all of these the repeated field is the element, so we extract it directly without unwrapping.
      for (int i = 0; i < numValues; i++) {
        values[i] = extractValue(group, 0, repeatedField, i);
      }
    }
    return values;
  }

  private Map<String, Object> extractStruct(Group group) {
    // Plain Parquet group (no LIST/MAP annotation) — surfaces as a Map keyed by child field name. Reached for
    // nested struct fields and for groups read with the legacy/un-annotated branch.
    int numValues = group.getType().getFieldCount();
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(numValues);
    for (int i = 0; i < numValues; i++) {
      map.put(group.getType().getType(i).getName(), extractValue(group, i));
    }
    return map;
  }

  private Map<String, Object> extractKeyValueMap(Group group) {
    // Group is annotated with MAP. Per the Parquet LogicalTypes spec it always has exactly one child — a repeated
    // group with two fields named "key" and "value":
    //   <map-repetition> group <name> (MAP) {
    //     repeated group key_value {
    //       required <key-type>   key;
    //       <value-repetition> <value-type> value;
    //     }
    //   }
    // The repeated wrapper name (`key_value` / `map`) and field order vary across writers, so we resolve the
    // key/value field indices from the schema once and reuse them for every entry.
    //
    // NOTE: Parquet does NOT guarantee that MAP entries are returned in any particular order on read — neither
    // sorted nor in insertion order. Writers, page boundaries, and dictionary encodings can all reorder entries.
    // If you need a stable order, write the data as a LIST of STRUCT<key, value> instead of using the native MAP
    // logical type. We therefore use a plain HashMap here and make no ordering promise to downstream consumers.
    int numValues = group.getFieldRepetitionCount(0);
    if (numValues == 0) {
      return Map.of();
    }
    GroupType keyValueType = group.getType().getType(0).asGroupType();
    int keyIndex = keyValueType.getFieldIndex("key");
    int valueIndex = keyValueType.getFieldIndex("value");
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(numValues);
    for (int i = 0; i < numValues; i++) {
      Group keyValueGroup = group.getGroup(0, i);
      Object key = extractValue(keyValueGroup, keyIndex);
      Object value = extractValue(keyValueGroup, valueIndex);
      map.put(stringifyMapKey(key), value);
    }
    return map;
  }

  private boolean isStandardListWrapper(Type repeatedField, String parentListName) {
    // Implements the Parquet LogicalTypes spec backward-compatibility rules for LIST element resolution:
    //   1. Repeated primitive: the primitive IS the element (no wrapper).
    //   2. Repeated multi-field group: the group IS the element (no wrapper).
    //   3. Repeated single-field group named `array` or `<list>_tuple`: the group IS the element (no wrapper).
    //   4. Otherwise (single-field group, any other name): the inner field IS the element (wrapper present).
    // This mirrors how Apache Arrow / parquet-cpp / parquet-avro (with add-list-element-records=false) interpret
    // LIST encodings, so the same Parquet bytes produce the same logical rows across readers.
    if (repeatedField.isPrimitive()) {
      return false;
    }
    GroupType repeatedGroup = repeatedField.asGroupType();
    if (repeatedGroup.getFieldCount() != 1) {
      return false;
    }
    String repeatedFieldName = repeatedField.getName();
    return !"array".equals(repeatedFieldName) && !(parentListName + "_tuple").equals(repeatedFieldName);
  }
}
