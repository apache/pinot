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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for null value transformation operations shared between
 * NullValueTransformer (record-level) and NullValueColumnTransformer (column-level).
 */
public class NullValueTransformerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(NullValueTransformerUtils.class);

  private NullValueTransformerUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Computes the default null value for a given field spec.
   * For time columns, validates that the default time is within valid range,
   * otherwise uses current time.
   *
   * @param fieldSpec The field specification
   * @param tableConfig The table configuration
   * @param schema The schema
   * @return The default null value to use for this field
   */
  @Nullable
  public static Object getDefaultNullValue(FieldSpec fieldSpec, TableConfig tableConfig, Schema schema) {
    String fieldName = fieldSpec.getName();
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();

    // Handle time column specially
    if (StringUtils.isNotEmpty(timeColumnName) && fieldName.equals(timeColumnName)) {
      return getDefaultTimeValue(timeColumnName, tableConfig, schema);
    }

    // For non-time columns, use the field spec's default null value
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    if (fieldSpec.isSingleValueField()) {
      return defaultNullValue;
    } else {
      return new Object[]{defaultNullValue};
    }
  }

  /**
   * Computes the default time value for the time column.
   * Time column is used to manage the segments, so its values have to be within the valid range. If the default
   * time value in the field spec is within the valid range, we use it as the default time value; if not, we use
   * current time as the default time value.
   *
   * @param timeColumnName The name of the time column
   * @param tableConfig The table configuration
   * @param schema The schema
   * @return The default time value to use
   */
  private static Object getDefaultTimeValue(String timeColumnName, TableConfig tableConfig, Schema schema) {
    DateTimeFieldSpec timeColumnSpec = schema.getSpecForTimeColumn(timeColumnName);
    Preconditions.checkState(timeColumnSpec != null, "Failed to find time field: %s from schema: %s", timeColumnName,
        schema.getSchemaName());

    String defaultTimeString = timeColumnSpec.getDefaultNullValueString();
    DateTimeFormatSpec dateTimeFormatSpec = timeColumnSpec.getFormatSpec();

    // Try to use the default time from the field spec if it's valid
    try {
      long defaultTimeMs = dateTimeFormatSpec.fromFormatToMillis(defaultTimeString);
      if (TimeUtils.timeValueInValidRange(defaultTimeMs)) {
        return timeColumnSpec.getDefaultNullValue();
      }
    } catch (Exception e) {
      // Ignore and fall through to use current time
    }

    // Use current time if default time is not valid
    String currentTimeString = dateTimeFormatSpec.fromMillisToFormat(System.currentTimeMillis());
    Object currentTime = timeColumnSpec.getDataType().convert(currentTimeString);
    LOGGER.info(
        "Default time: {} does not comply with format: {}, using current time: {} as the default time for table: {}",
        defaultTimeString, timeColumnSpec.getFormat(), currentTime, tableConfig.getTableName());
    return currentTime;
  }

  /**
   * Transforms a value by replacing null with the default null value.
   *
   * @param value The value to transform
   * @param defaultNullValue The default null value to use if value is null
   * @return The original value if not null, otherwise the default null value
   */
  @Nullable
  public static Object transformValue(@Nullable Object value, Object defaultNullValue) {
    return value != null ? value : defaultNullValue;
  }
}
