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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NullValueTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(NullValueTransformer.class);

  private final Map<String, Object> _defaultNullValues = new HashMap<>();

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

    // NOTE: Time column is used to manage the segments, so its values have to be within the valid range. If the default
    //       time value in the field spec is within the valid range, we use it as the default time value; if not, we use
    //       current time as the default time value.
    if (StringUtils.isNotEmpty(timeColumnName)) {
      DateTimeFieldSpec timeColumnSpec = schema.getSpecForTimeColumn(timeColumnName);
      Preconditions.checkState(timeColumnSpec != null, "Failed to find time field: %s from schema: %s", timeColumnName,
          schema.getSchemaName());

      String defaultTimeString = timeColumnSpec.getDefaultNullValueString();
      DateTimeFormatSpec dateTimeFormatSpec = timeColumnSpec.getFormatSpec();
      try {
        long defaultTimeMs = dateTimeFormatSpec.fromFormatToMillis(defaultTimeString);
        if (TimeUtils.timeValueInValidRange(defaultTimeMs)) {
          _defaultNullValues.put(timeColumnName, timeColumnSpec.getDefaultNullValue());
          return;
        }
      } catch (Exception e) {
        // Ignore
      }
      String currentTimeString = dateTimeFormatSpec.fromMillisToFormat(System.currentTimeMillis());
      Object currentTime = timeColumnSpec.getDataType().convert(currentTimeString);
      _defaultNullValues.put(timeColumnName, currentTime);
      LOGGER.info(
          "Default time: {} does not comply with format: {}, using current time: {} as the default time for table: {}",
          defaultTimeString, timeColumnSpec.getFormat(), currentTime, tableConfig.getTableName());
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
    return record;
  }
}
