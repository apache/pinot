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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/// Extracts Pinot [GenericRow] from an Avro [GenericRecord]. A single schema-driven walk produces values
/// that satisfy the `RecordExtractor` contract directly — no second pass through `BaseRecordExtractor#convert`.
///
/// **Avro source type → Java input → Java output type:**
/// - avro `boolean` → `Boolean` → `Boolean`
/// - avro `int` → `Integer` → `Integer`
/// - avro `long` → `Long` → `Long`
/// - avro `float` → `Float` → `Float`
/// - avro `double` → `Double` → `Double`
/// - avro `string` → `Utf8` → `String` (via `Utf8.toString()`)
/// - avro `bytes` → `ByteBuffer` → `byte[]` (materialized from the buffer's remaining content)
/// - avro `fixed` → `GenericFixed` / `GenericData.Fixed` → `byte[]`
/// - avro `enum` → `GenericData.EnumSymbol` → `String` (enum name)
/// - avro `array<T>` → `List<T>` → `Object[]` (each element recursively converted)
/// - avro `map<string, T>` → `Map<Utf8, T>` → `Map<String, Object>` (each value recursively converted)
/// - avro nested `record` → `GenericRecord` → `Map<String, Object>`
/// - avro `union[null, X]` with the `null` branch selected → `null`
///
/// **Logical types** (the reader emits the raw Avro physical type; this extractor applies the conversion via
///   [#CONVERSION_MAP] to produce the Pinot contract type):
/// - `decimal` / `big-decimal` → raw `ByteBuffer` → [BigDecimal] (always converted; raw bytes aren't interpretable
///   without external precision/scale)
/// - `timestamp-millis` → raw `Long` → [Timestamp], or `Long` raw epoch millis when `extractRawTimeValues` is `true`
/// - `timestamp-micros` → raw `Long` → [Timestamp] (sub-millisecond micros preserved), or `Long` raw epoch micros when
///   `extractRawTimeValues` is `true`
/// - `timestamp-nanos` → raw `Long` → [Timestamp] (nanosecond precision preserved), or `Long` raw epoch nanos when
///   `extractRawTimeValues` is `true`
/// - `date` → raw `Integer` → [LocalDate], or `Integer` raw days-since-epoch when `extractRawTimeValues` is `true`
/// - `time-millis` → raw `Integer` → [LocalTime], or `Integer` raw ms-since-midnight when `extractRawTimeValues` is
///   `true`
/// - `time-micros` → raw `Long` → [LocalTime], or `Long` raw µs-since-midnight when `extractRawTimeValues` is `true`
/// - `uuid` → raw `Utf8` / `GenericFixed` → [UUID] (always converted; downstream type transformer adapts to the Pinot
///   column type — `STRING` column gets canonical UUID string, `BYTES` column gets 16-byte big-endian form)
public class AvroRecordExtractor extends BaseRecordExtractor<GenericRecord> {
  private static final Conversion<BigDecimal> DECIMAL_CONVERSION = new Conversions.DecimalConversion();
  private static final Conversion<BigDecimal> BIG_DECIMAL_CONVERSION = new Conversions.BigDecimalConversion();
  private static final Conversion<Instant> TIMESTAMP_MILLIS_CONVERSION =
      new TimeConversions.TimestampMillisConversion();
  private static final Conversion<Instant> TIMESTAMP_MICROS_CONVERSION =
      new TimeConversions.TimestampMicrosConversion();
  private static final Conversion<Instant> TIMESTAMP_NANOS_CONVERSION = new TimeConversions.TimestampNanosConversion();
  private static final Conversion<LocalDate> DATE_CONVERSION = new TimeConversions.DateConversion();
  private static final Conversion<LocalTime> TIME_MILLIS_CONVERSION = new TimeConversions.TimeMillisConversion();
  private static final Conversion<LocalTime> TIME_MICROS_CONVERSION = new TimeConversions.TimeMicrosConversion();
  private static final Conversion<UUID> UUID_CONVERSION = new Conversions.UUIDConversion();

  private static final Map<String, Conversion<?>> CONVERSION_MAP = Map.of(
      DECIMAL_CONVERSION.getLogicalTypeName(), DECIMAL_CONVERSION,
      BIG_DECIMAL_CONVERSION.getLogicalTypeName(), BIG_DECIMAL_CONVERSION,
      TIMESTAMP_MILLIS_CONVERSION.getLogicalTypeName(), TIMESTAMP_MILLIS_CONVERSION,
      TIMESTAMP_MICROS_CONVERSION.getLogicalTypeName(), TIMESTAMP_MICROS_CONVERSION,
      TIMESTAMP_NANOS_CONVERSION.getLogicalTypeName(), TIMESTAMP_NANOS_CONVERSION,
      DATE_CONVERSION.getLogicalTypeName(), DATE_CONVERSION,
      TIME_MILLIS_CONVERSION.getLogicalTypeName(), TIME_MILLIS_CONVERSION,
      TIME_MICROS_CONVERSION.getLogicalTypeName(), TIME_MICROS_CONVERSION,
      UUID_CONVERSION.getLogicalTypeName(), UUID_CONVERSION
  );

  private static final Set<String> TEMPORAL_LOGICAL_TYPES = Set.of(
      TIMESTAMP_MILLIS_CONVERSION.getLogicalTypeName(),
      TIMESTAMP_MICROS_CONVERSION.getLogicalTypeName(),
      TIMESTAMP_NANOS_CONVERSION.getLogicalTypeName(),
      DATE_CONVERSION.getLogicalTypeName(),
      TIME_MILLIS_CONVERSION.getLogicalTypeName(),
      TIME_MICROS_CONVERSION.getLogicalTypeName()
  );

  protected boolean _extractRawTimeValues;

  @Override
  protected void initConfig(@Nullable RecordExtractorConfig config) {
    if (config instanceof AvroRecordExtractorConfig) {
      _extractRawTimeValues = ((AvroRecordExtractorConfig) config).isExtractRawTimeValues();
    }
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    Schema schema = from.getSchema();
    if (_extractAll) {
      for (Schema.Field field : schema.getFields()) {
        String fieldName = field.name();
        to.putValue(fieldName, convert(field.schema(), from.get(fieldName)));
      }
    } else {
      for (String fieldName : _fields) {
        Schema.Field field = schema.getField(fieldName);
        if (field != null) {
          to.putValue(fieldName, convert(field.schema(), from.get(field.pos())));
        }
      }
    }
    return to;
  }

  /// Schema-driven dispatch — produces a contract-compliant value in a single walk.
  /// `null` short-circuits at the top so subclasses overriding [#convertSingleValue] never see null.
  @Nullable
  protected Object convert(Schema schema, @Nullable Object value) {
    if (value == null) {
      return null;
    }
    schema = resolveUnionSchema(schema, value);
    switch (schema.getType()) {
      case ARRAY:
        // GenericDatumReader emits array<T> as GenericData.Array, which is a List.
        return convertArray(schema, (List<?>) value);
      case MAP:
        // GenericDatumReader emits map<T> as a HashMap. Avro keys are always strings; the runtime key type
        // is `Utf8` unless the schema declares `avro.java.string = "String"`. We normalize to `String`.
        return convertMap(schema, (Map<?, ?>) value);
      case RECORD:
        return convertRecord(schema, (GenericRecord) value);
      default:
        return convertSingleValue(schema, value);
    }
  }

  /// Picks the union branch matching the runtime value via [GenericData#resolveUnion] — primitive-type-name
  /// dispatch (`Long → "long"`, `ByteBuffer → "bytes"`, `Utf8 → "string"`, ...) plus named-type dispatch by
  /// schema name (`GenericRecord` / `GenericFixed` / `GenericData.EnumSymbol`). Avro disallows duplicate
  /// primitive types in a single union, so the resolution is unambiguous. Throws
  /// [org.apache.avro.UnresolvedUnionException] when the value matches no branch.
  ///
  /// Assumes the input is in raw Avro physical form — see the empty-`GenericData` policy on
  /// [AvroUtils#getAvroReader]. A pre-converted value (e.g. `BigDecimal` for a `bytes(decimal)` branch)
  /// won't match by primitive-type-name and will throw.
  private static Schema resolveUnionSchema(Schema schema, Object value) {
    if (!schema.isUnion()) {
      return schema;
    }
    int index = GenericData.get().resolveUnion(schema, value);
    return schema.getTypes().get(index);
  }

  private Object[] convertArray(Schema schema, List<?> list) {
    Schema elementSchema = schema.getElementType();
    int numElements = list.size();
    Object[] result = new Object[numElements];
    for (int i = 0; i < numElements; i++) {
      result[i] = convert(elementSchema, list.get(i));
    }
    return result;
  }

  private Map<String, Object> convertMap(Schema schema, Map<?, ?> map) {
    Schema valueSchema = schema.getValueType();
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      result.put(entry.getKey().toString(), convert(valueSchema, entry.getValue()));
    }
    return result;
  }

  private Map<String, Object> convertRecord(Schema schema, GenericRecord record) {
    List<Schema.Field> fields = schema.getFields();
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(fields.size());
    for (Schema.Field field : fields) {
      result.put(field.name(), convert(field.schema(), record.get(field.name())));
    }
    return result;
  }

  /// Converts a leaf (non-container) Avro value to its Pinot contract type. Subclasses override to handle
  /// format-specific extensions (e.g. parquet-avro's INT96 surfaced as a `fixed(12)` with a sentinel doc).
  /// The default handles the Avro spec — primitives + the logical types registered in [#CONVERSION_MAP].
  ///
  /// Per the Avro spec, only STRING / BYTES / FIXED / INT / LONG can carry a logical-type annotation; the
  /// other primitive types skip the lookup entirely. When a logical-type conversion runs, `Instant`
  /// results (`timestamp-millis` / `timestamp-micros` / `timestamp-nanos`) are post-mapped to [Timestamp]
  /// per the Pinot contract. When [#_extractRawTimeValues] is set, temporal logical types pass through
  /// the raw underlying integer.
  protected Object convertSingleValue(Schema schema, Object value) {
    Schema.Type type = schema.getType();
    switch (type) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
        // The Avro spec defines no logical types for these primitives — skip the logical-type lookup.
        return value;
      case ENUM:
        // No logical types; `EnumSymbol.toString()` returns the canonical enum-symbol string.
        return value.toString();
      default:
        // INT / LONG / STRING / BYTES / FIXED — fall through to logical-type handling below.
        break;
    }
    LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
    if (logicalType != null) {
      String name = logicalType.getName();
      if (!(_extractRawTimeValues && TEMPORAL_LOGICAL_TYPES.contains(name))) {
        Conversion<?> conversion = CONVERSION_MAP.get(name);
        if (conversion != null) {
          Object converted = Conversions.convertToLogicalType(value, schema, logicalType, conversion);
          return converted instanceof Instant ? Timestamp.from((Instant) converted) : converted;
        }
      }
    }
    // No logical-type conversion fired — fall through to physical-type normalization.
    switch (type) {
      case STRING:
        // `Utf8.toString()` returns the decoded String; an already-`String` value is a no-op.
        return value.toString();
      case BYTES:
        // ByteBuffer might be reused by the reader. Slice to avoid advancing the original buffer's position.
        ByteBuffer slice = ((ByteBuffer) value).slice();
        byte[] bytes = new byte[slice.limit()];
        slice.get(bytes);
        return bytes;
      case FIXED:
        return ((GenericFixed) value).bytes();
      default:
        // INT / LONG with no logical type, or temporal raw-mode pass-through.
        return value;
    }
  }
}
