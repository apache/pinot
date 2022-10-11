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
package org.apache.pinot.spi.config.table;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * TimestampIndexGranularity is the enum of different time granularities from MILLIS to YEAR.
 */
public enum TimestampIndexGranularity {
  MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR;

  private static final Set<String> VALID_VALUES =
      Arrays.stream(TimestampIndexGranularity.values()).map(v -> v.toString()).collect(Collectors.toSet());

  public static String getColumnNameWithGranularity(String column, TimestampIndexGranularity granularity) {
    return "$" + column + "$" + granularity;
  }

  public static boolean isValidTimeColumnWithGranularityName(String columnName) {
    if (columnName.charAt(0) != '$') {
      return false;
    }
    int secondDollarPos = columnName.indexOf('$', 1);
    if (secondDollarPos < 0) {
      return false;
    }
    return VALID_VALUES.contains(columnName.substring(secondDollarPos + 1));
  }

  public static boolean isValidTimeGranularity(String granularity) {
    return VALID_VALUES.contains(granularity);
  }

  public static Set<String> extractTimestampIndexGranularityColumnNames(TableConfig tableConfig) {
    // Extract Timestamp granularity columns
    Set<String> timeColumnWithGranularity = new HashSet<>();
    if (tableConfig == null || tableConfig.getFieldConfigList() == null) {
      return timeColumnWithGranularity;
    }
    for (FieldConfig fieldConfig : tableConfig.getFieldConfigList()) {
      TimestampConfig timestampConfig = fieldConfig.getTimestampConfig();
      if (timestampConfig == null || timestampConfig.getGranularities() == null) {
        continue;
      }
      String timeColumn = fieldConfig.getName();
      for (TimestampIndexGranularity granularity : timestampConfig.getGranularities()) {
        timeColumnWithGranularity.add(getColumnNameWithGranularity(timeColumn, granularity));
      }
    }
    return timeColumnWithGranularity;
  }

  /**
   * Generate the time column with granularity FieldSpec from existing time column FieldSpec and granularity.
   * The new FieldSpec keeps same FieldType, only update the field name.
   *
   * @param fieldSpec
   * @param granularity
   * @return time column with granularity FieldSpec, null if FieldSpec is not Dimension/Metric/DateTime type.
   */
  @Nullable
  public static FieldSpec getFieldSpecForTimestampColumnWithGranularity(FieldSpec fieldSpec,
      TimestampIndexGranularity granularity) {
    if (fieldSpec instanceof DateTimeFieldSpec) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      return new DateTimeFieldSpec(
          TimestampIndexGranularity.getColumnNameWithGranularity(fieldSpec.getName(), granularity),
          FieldSpec.DataType.TIMESTAMP, dateTimeFieldSpec.getFormat(), dateTimeFieldSpec.getGranularity());
    }
    // DIMENSION, METRIC, TIMESTAMP types are not supported
    return null;
  }

  /**
   * Generate the dateTrunc expression to convert the base time column to the time column with granularity value
   * E.g. $ts$DAY -> dateTrunc('DAY',"ts")
   *
   * @param timeColumn
   * @param granularity
   * @return Time conversion expression
   */
  public static String getTransformExpression(String timeColumn, TimestampIndexGranularity granularity) {
    return "dateTrunc('" + granularity + "',\"" + timeColumn + "\")";
  }
}
