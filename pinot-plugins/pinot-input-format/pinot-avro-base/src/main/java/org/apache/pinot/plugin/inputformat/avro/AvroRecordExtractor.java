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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/// Extracts Pinot [GenericRow] from an Avro [GenericRecord]. Native Java input types are what `GenericDatumReader`
/// produces.
///
/// **Avro source type → Java input → Java output type:**
/// - avro `boolean` → `Boolean` → `Boolean`
/// - avro `int` → `Integer` → `Integer`
/// - avro `long` → `Long` → `Long`
/// - avro `float` → `Float` → `Float`
/// - avro `double` → `Double` → `Double`
/// - avro `string` → `Utf8` → `String` (via `Utf8.toString()`)
/// - avro `bytes` → `ByteBuffer` → `byte[]` (materialized from the buffer's remaining content)
/// - avro `fixed` → [GenericFixed] / `GenericData.Fixed` → `byte[]`
/// - avro `enum` → `GenericData.EnumSymbol` → `String` (enum name)
/// - avro `array<T>` → `List<T>` → `Object[]` (each element recursively converted)
/// - avro `map<string, T>` → `Map<Utf8, T>` → `Map<String, Object>` (each value recursively converted)
/// - avro nested `record` → [GenericRecord] → `Map<String, Object>`
/// - avro `union[null, X]` with the `null` branch selected → `null`
///
/// **Logical types** (applied when `enableLogicalTypes = true`, the config default; `GenericDatumReader` emits
/// the post-conversion Java types listed below):
/// - `decimal` → `BigDecimal` → `BigDecimal`
/// - `uuid` → `UUID` → `String` (UUID string form via `UUID.toString()`)
/// - `date` → `LocalDate` → `LocalDate` (passes through; TZ-independent)
/// - `time-millis` / `time-micros` → `LocalTime` → `LocalTime` (passes through; full ns precision preserved)
/// - `timestamp-millis` / `timestamp-micros` → `Instant` → `java.sql.Timestamp` (sub-millisecond nanos preserved
///   via `Timestamp.getNanos()`)
public class AvroRecordExtractor extends BaseRecordExtractor<GenericRecord> {
  private boolean _applyLogicalTypes = true;

  @Override
  protected void initConfig(@Nullable RecordExtractorConfig config) {
    if (config instanceof AvroRecordExtractorConfig) {
      _applyLogicalTypes = ((AvroRecordExtractorConfig) config).isEnableLogicalTypes();
    }
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    if (_extractAll) {
      List<Schema.Field> fields = from.getSchema().getFields();
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Object value = from.get(fieldName);
        if (_applyLogicalTypes) {
          value = AvroSchemaUtil.applyLogicalType(field, value);
        }
        if (value != null) {
          value = transformValue(value, field);
        }
        to.putValue(fieldName, value);
      }
    } else {
      for (String fieldName : _fields) {
        Schema.Field field = from.getSchema().getField(fieldName);
        if (field != null) {
          Object value = from.get(field.pos());
          if (_applyLogicalTypes) {
            value = AvroSchemaUtil.applyLogicalType(field, value);
          }
          if (value != null) {
            value = transformValue(value, field);
          }
          to.putValue(fieldName, value);
        }
      }
    }
    return to;
  }

  protected Object transformValue(Object value, Schema.Field field) {
    return convert(value);
  }

  @Override
  protected boolean isRecord(Object value) {
    return value instanceof GenericRecord;
  }

  @Override
  protected Map<Object, Object> convertRecord(Object value) {
    GenericRecord record = (GenericRecord) value;
    List<Schema.Field> fields = record.getSchema().getFields();
    Map<Object, Object> convertedMap = Maps.newHashMapWithExpectedSize(fields.size());
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object fieldValue = record.get(fieldName);
      Object convertedValue = fieldValue != null ? transformValue(fieldValue, field) : null;
      convertedMap.put(fieldName, convertedValue);
    }
    return convertedMap;
  }

  /// Adds Avro-specific handling: [GenericFixed] (`fixed`) → `byte[]`. Everything else delegates to the base
  /// (see class Javadoc for the full Avro-source → Java-output matrix).
  @Override
  protected Object convertSingleValue(Object value) {
    if (value instanceof GenericFixed) {
      return ((GenericFixed) value).bytes();
    }
    return super.convertSingleValue(value);
  }
}
