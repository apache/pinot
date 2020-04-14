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
package org.apache.pinot.plugin.inputformat.json;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/**
 * Extractor for JSON records
 */
public class JSONRecordExtractor implements RecordExtractor<Map<String, Object>> {

  private List<String> _fields;

  @Override
  public void init(List<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    _fields = fields;
  }

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    for (String fieldName : _fields) {
      Object value = from.get(fieldName);
      // NOTE about JSON behavior - cannot distinguish between INT/LONG and FLOAT/DOUBLE.
      // DataTypeTransformer fixes it.
      Object convertedValue;
      if (value instanceof Collection) {
        convertedValue = convertMultiValue((Collection) value);
      } else {
        convertedValue = convertSingleValue(value);
      }
      to.putValue(fieldName, convertedValue);
    }
    return to;
  }

  /**
   * Converts the value to a single-valued value
   */
  @Nullable
  private Object convertSingleValue(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return value;
    }
    return value.toString();
  }

  /**
   * Converts the value to a multi-valued value
   */
  @Nullable
  private Object convertMultiValue(@Nullable Collection values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    int numValues = values.size();
    Object[] array = new Object[numValues];
    int index = 0;
    for (Object value : values) {
      Object convertedValue = convertSingleValue(value);
      if (convertedValue != null && !convertedValue.toString().equals("")) {
        array[index++] = convertedValue;
      }
    }
    if (index == numValues) {
      return array;
    } else if (index == 0) {
      return null;
    } else {
      return Arrays.copyOf(array, index);
    }
  }
}
