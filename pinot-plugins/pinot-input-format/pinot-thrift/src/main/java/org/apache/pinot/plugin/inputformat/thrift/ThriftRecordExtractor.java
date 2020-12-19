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

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;


/**
 * Extractor for records of Thrift input
 */
public class ThriftRecordExtractor extends BaseRecordExtractor<TBase> {

  private Map<String, Integer> _fieldIds;
  private Set<String> _fields;
  private boolean _extractAll = false;

  @Override
  public void init(@Nullable Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
    _fieldIds = ((ThriftRecordExtractorConfig) recordExtractorConfig).getFieldIds();
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Collections.emptySet();
    } else {
      _fields = ImmutableSet.copyOf(fields);
    }
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
        Object value = null;
        Integer fieldId = _fieldIds.get(fieldName);
        if (fieldId != null) {
          //noinspection unchecked
          value = from.getFieldValue(from.fieldForId(fieldId));
        }
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
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
  protected Object convertRecord(Object value) {
    TBase record = (TBase) value;
    Map<Object, Object> convertedRecord = new HashMap<>();
    for (TFieldIdEnum tFieldIdEnum: FieldMetaData.getStructMetaDataMap(record.getClass()).keySet()) {
      Object fieldValue = record.getFieldValue(tFieldIdEnum);
      if (fieldValue != null) {
        fieldValue = convert(fieldValue);
      }
      convertedRecord.put(tFieldIdEnum.getFieldName(), fieldValue);
    }
    return convertedRecord;
  }
}
