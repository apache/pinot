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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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
/// - thrift `bool` → `Boolean` → `Boolean`
/// - thrift `i8` → `Byte` → `Integer` (widened by base)
/// - thrift `i16` → `Short` → `Integer` (widened by base)
/// - thrift `i32` → `Integer` → `Integer`
/// - thrift `i64` → `Long` → `Long`
/// - thrift `double` → `Double` → `Double`
/// - thrift `string` → `String` → `String`
/// - thrift `binary` → `ByteBuffer` → `byte[]`
/// - thrift `enum` → `TEnum` → enum name `String` (via `toString()`)
/// - thrift nested `struct` → [TBase] → `Map<String, Object>`
/// - thrift `list<X>` / `set<X>` → `List<X>` / `Set<X>` → `Object[]`
/// - thrift `map<K, V>` → `Map<K, V>` → `Map<Object, Object>`
/// - thrift unset optional field → `null`
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
        if (value != null) {
          value = convert(value);
        }
        to.putValue(nameToId.getKey(), value);
      }
    } else {
      for (String fieldName : _fields) {
        Integer fieldId = _fieldIds.get(fieldName);
        if (fieldId != null) {
          Object value = from.getFieldValue(from.fieldForId(fieldId));
          if (value != null) {
            value = convert(value);
          }
          to.putValue(fieldName, value);
        }
      }
    }
    return to;
  }

  /**
   * Returns whether the object is a Thrift object.
   */
  @Override
  protected boolean isRecord(Object value) {
    return value instanceof TBase;
  }

  /**
   * Handles the conversion of each field of a Thrift object.
   *
   * @param value should be verified to be a Thrift TBase type prior to calling this method as it will be casted
   *              without checking
   */
  @Override
  protected Map<Object, Object> convertRecord(Object value) {
    TBase record = (TBase) value;
    Set<TFieldIdEnum> fields = FieldMetaData.getStructMetaDataMap(record.getClass()).keySet();
    Map<Object, Object> convertedRecord = Maps.newHashMapWithExpectedSize(fields.size());
    for (TFieldIdEnum field : fields) {
      Object fieldValue = record.getFieldValue(field);
      Object convertedValue = fieldValue != null ? convert(fieldValue) : null;
      convertedRecord.put(field.getFieldName(), convertedValue);
    }
    return convertedRecord;
  }
}
