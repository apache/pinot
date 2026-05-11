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
package org.apache.pinot.plugin.inputformat.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;


/// Extracts Pinot [GenericRow] from a Thrift-generated [TBase] via `getFieldValue(fieldForId(...))`.
///
/// **Thrift source type → Java input → Java output type:**
/// - `bool` → `Boolean` → `Boolean`
/// - `i8` → `Byte` → `Integer` (widened)
/// - `i16` → `Short` → `Integer` (widened)
/// - `i32` → `Integer` → `Integer`
/// - `i64` → `Long` → `Long`
/// - `double` → `Double` → `Double`
/// - `string` → `String` → `String`
/// - `binary` → `ByteBuffer` → `byte[]`
/// - `enum` → `TEnum` → enum name `String` (via `toString()`)
/// - `struct` → [TBase] → `Map<String, Object>`
/// - `list<X>` / `set<X>` → `List<X>` / `Set<X>` → `Object[]`
/// - `map<K, V>` → `Map<K, V>` → `Map<String, Object>` (keys stringified via [BaseRecordExtractor#stringifyMapKey])
/// - unset optional field → `null`
@SuppressWarnings({"rawtypes", "unchecked"})
public class ThriftRecordExtractor extends BaseRecordExtractor<TBase> {

  private Map<String, Integer> _fieldIds;

  @Override
  protected void initConfig(RecordExtractorConfig config) {
    Preconditions.checkArgument(config instanceof ThriftRecordExtractorConfig,
        "ThriftRecordExtractor requires a ThriftRecordExtractorConfig");
    _fieldIds = ((ThriftRecordExtractorConfig) config).getFieldIds();
  }

  @Override
  public GenericRow extract(TBase from, GenericRow to) {
    if (_extractAll) {
      for (Map.Entry<String, Integer> nameToId : _fieldIds.entrySet()) {
        Object value = from.getFieldValue(from.fieldForId(nameToId.getValue()));
        to.putValue(nameToId.getKey(), value != null ? convert(value) : null);
      }
    } else {
      for (String fieldName : _fields) {
        Integer fieldId = _fieldIds.get(fieldName);
        if (fieldId != null) {
          Object value = from.getFieldValue(from.fieldForId(fieldId));
          to.putValue(fieldName, value != null ? convert(value) : null);
        }
      }
    }
    return to;
  }

  /// Dispatches a non-null Thrift value off its runtime Java type: `Object[]` for `Collection` (list / set),
  /// `Map<String, Object>` for `Map` and for nested `TBase` records, single-value normalization for scalars.
  private static Object convert(Object value) {
    // List
    if (value instanceof Collection) {
      return convertCollection((Collection<Object>) value);
    }
    // Map
    if (value instanceof Map) {
      return convertMap((Map<Object, Object>) value);
    }
    // Record
    if (value instanceof TBase) {
      return convertRecord((TBase) value);
    }
    // Single value
    return convertSingleValue(value);
  }

  private static Object[] convertCollection(Collection<Object> collection) {
    Object[] result = new Object[collection.size()];
    int i = 0;
    for (Object value : collection) {
      result[i++] = value != null ? convert(value) : null;
    }
    return result;
  }

  /// Converts a `map<K, V>`. Keys flow through `convertSingleValue` (Thrift's allowed key types — bool /
  /// i8 / i16 / i32 / i64 / double / string / binary — cover the same matrix), then are stringified via
  /// [BaseRecordExtractor#stringifyMapKey] per the `Map<String, Object>` contract.
  private static Map<String, Object> convertMap(Map<Object, Object> map) {
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      Object key = entry.getKey();
      if (key == null) {
        continue;
      }
      Object convertedKey = convertSingleValue(key);
      if (convertedKey == null) {
        continue;
      }
      Object value = entry.getValue();
      result.put(stringifyMapKey(convertedKey), value != null ? convert(value) : null);
    }
    return result;
  }

  private static Map<String, Object> convertRecord(TBase record) {
    Set<TFieldIdEnum> fields = FieldMetaData.getStructMetaDataMap(record.getClass()).keySet();
    Map<String, Object> result = Maps.newHashMapWithExpectedSize(fields.size());
    for (TFieldIdEnum field : fields) {
      Object value = record.getFieldValue(field);
      result.put(field.getFieldName(), value != null ? convert(value) : null);
    }
    return result;
  }

  /// Single-value normalization for Thrift's scalar Java types: `Byte` / `Short` widen to `Integer`,
  /// `ByteBuffer` materializes to `byte[]` (slice-safely so the source buffer's position is not advanced),
  /// other `Number` / `Boolean` / `byte[]` pass through, anything else (e.g. `TEnum`) → `toString()`.
  @VisibleForTesting
  static Object convertSingleValue(Object value) {
    if (value instanceof Number) {
      if (value instanceof Byte || value instanceof Short) {
        return ((Number) value).intValue();
      }
      return value;
    }
    if (value instanceof Boolean || value instanceof byte[]) {
      return value;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer slice = ((ByteBuffer) value).slice();
      byte[] bytes = new byte[slice.limit()];
      slice.get(bytes);
      return bytes;
    }
    return value.toString();
  }
}
