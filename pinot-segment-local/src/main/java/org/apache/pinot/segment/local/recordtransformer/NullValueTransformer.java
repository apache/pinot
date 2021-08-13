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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


public class NullValueTransformer implements RecordTransformer {
  private final Map<String, Object> _defaultNullValues = new HashMap<>();
  private final DateTimeFieldSpec _dateTimeFieldSpec;
  private final DateTimeFormatSpec _defaultTimeValueFormat;

  public NullValueTransformer(TableConfig tableConfig, Schema schema) {
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();

    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && !fieldSpec.getName().equals(timeColumnName)) {
        String fieldName = fieldSpec.getName();
        Object defaultNullValue = fieldSpec.getDefaultNullValue();
        if (fieldSpec.isSingleValueField()) {
          _defaultNullValues.put(fieldName, defaultNullValue);
        } else {
          _defaultNullValues.put(fieldName, new Object[]{defaultNullValue});
        }
      }
    }

    if (tableConfig.getValidationConfig().isAllowNullTimeValue() && timeColumnName != null && schema.getSpecForTimeColumn(timeColumnName) != null) {
      _dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
      _defaultTimeValueFormat = new DateTimeFormatSpec(_dateTimeFieldSpec.getFormat());
    } else {
      _dateTimeFieldSpec = null;
      _defaultTimeValueFormat = null;
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, Object> entry : _defaultNullValues.entrySet()) {
      String fieldName = entry.getKey();
      Object value = record.getValue(fieldName);
      if (value == null) {
        record.putDefaultNullValue(fieldName, entry.getValue());
      }
    }

    // handle null value in time column
    if (_defaultTimeValueFormat != null && record.getValue(_dateTimeFieldSpec.getName()) == null) {
      String timeValueStr = _defaultTimeValueFormat.fromMillisToFormat(System.currentTimeMillis());
      Object timeValue = _dateTimeFieldSpec.getDataType().convert(timeValueStr);
      record.putDefaultNullValue(_dateTimeFieldSpec.getName(), timeValue);
    }

    return record;
  }
}
