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
package org.apache.pinot.spi.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;


public class TimestampIndexUtils {
  private TimestampIndexUtils() {
  }

  private static final Set<String> VALID_GRANULARITIES =
      Arrays.stream(TimestampIndexGranularity.values()).map(Enum::name).collect(Collectors.toSet());

  private static final EnumMap<TimestampIndexGranularity, String> FIELD_SPEC_GRANULARITY_MAP =
      new EnumMap<>(TimestampIndexGranularity.class);

  static {
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.MILLISECOND, "1:MILLISECONDS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.SECOND, "1:SECONDS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.MINUTE, "1:MINUTES");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.HOUR, "1:HOURS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.DAY, "1:DAYS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.WEEK, "7:DAYS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.MONTH, "30:DAYS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.QUARTER, "90:DAYS");
    FIELD_SPEC_GRANULARITY_MAP.put(TimestampIndexGranularity.YEAR, "365:DAYS");
  }

  /**
   * Returns the column name with granularity, e.g. $ts$DAY.
   */
  public static String getColumnWithGranularity(String timestampColumn, TimestampIndexGranularity granularity) {
    return getColumnWithGranularity(timestampColumn, granularity.name());
  }

  /**
   * Returns the column name with granularity, e.g. $ts$DAY.
   */
  public static String getColumnWithGranularity(String timestampColumn, String granularity) {
    return "$" + timestampColumn + "$" + granularity;
  }

  /**
   * Returns whether the given string is a valid granularity.
   */
  public static boolean isValidGranularity(String granularity) {
    return VALID_GRANULARITIES.contains(granularity);
  }

  /**
   * Returns whether the given column nane is a column name with granularity.
   */
  public static boolean isValidColumnWithGranularity(String column) {
    if (column.charAt(0) != '$') {
      return false;
    }
    int secondDollarPos = column.indexOf('$', 1);
    if (secondDollarPos < 0) {
      return false;
    }
    return VALID_GRANULARITIES.contains(column.substring(secondDollarPos + 1));
  }

  /**
   * Extracts all columns with granularity based on the TIMESTAMP index config.
   */
  public static Set<String> extractColumnsWithGranularity(TableConfig tableConfig) {
    if (tableConfig.getFieldConfigList() == null) {
      return Collections.emptySet();
    }
    Set<String> columnsWithGranularity = new HashSet<>();
    for (FieldConfig fieldConfig : tableConfig.getFieldConfigList()) {
      TimestampConfig timestampConfig = fieldConfig.getTimestampConfig();
      if (timestampConfig == null || CollectionUtils.isEmpty(timestampConfig.getGranularities())) {
        continue;
      }
      String timestampColumn = fieldConfig.getName();
      for (TimestampIndexGranularity granularity : timestampConfig.getGranularities()) {
        columnsWithGranularity.add(getColumnWithGranularity(timestampColumn, granularity));
      }
    }
    return columnsWithGranularity.isEmpty() ? Collections.emptySet() : columnsWithGranularity;
  }

  /**
   * Applies the TIMESTAMP index configured in the table config:
   * - Adds the derived timestamp columns with granularity to the schema
   * - Adds transform for the derived timestamp columns with granularity
   * - Adds range index to the derived timestamp columns with granularity
   */
  public static void applyTimestampIndex(TableConfig tableConfig, Schema schema) {
    if (tableConfig.getFieldConfigList() == null) {
      return;
    }

    Map<String, List<TimestampIndexGranularity>> timestampIndexConfigs = new HashMap<>();
    for (FieldConfig fieldConfig : tableConfig.getFieldConfigList()) {
      TimestampConfig timestampConfig = fieldConfig.getTimestampConfig();
      if (timestampConfig == null || CollectionUtils.isEmpty(timestampConfig.getGranularities())) {
        continue;
      }
      timestampIndexConfigs.put(fieldConfig.getName(), timestampConfig.getGranularities());
    }
    if (timestampIndexConfigs.isEmpty()) {
      return;
    }

    // Synchronize on table config object to prevent concurrent modification
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (tableConfig) {
      // Check if the updates are already applied
      boolean schemaApplied;
      boolean tableConfigApplied;
      Map.Entry<String, List<TimestampIndexGranularity>> sampleEntry =
          timestampIndexConfigs.entrySet().iterator().next();
      String sampleTimestampColumnWithGranularity =
          getColumnWithGranularity(sampleEntry.getKey(), sampleEntry.getValue().get(0));
      schemaApplied = schema.hasColumn(sampleTimestampColumnWithGranularity);
      IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
      List<String> rangeIndexColumns = indexingConfig.getRangeIndexColumns();
      tableConfigApplied =
          rangeIndexColumns != null && rangeIndexColumns.contains(sampleTimestampColumnWithGranularity);
      if (schemaApplied && tableConfigApplied) {
        return;
      }

      // Apply TIMESTAMP index
      List<TransformConfig> transformConfigs = null;
      if (!tableConfigApplied) {
        if (rangeIndexColumns == null) {
          rangeIndexColumns = new ArrayList<>();
          indexingConfig.setRangeIndexColumns(rangeIndexColumns);
        }
        IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
        if (ingestionConfig == null) {
          ingestionConfig = new IngestionConfig();
          tableConfig.setIngestionConfig(ingestionConfig);
        }
        transformConfigs = ingestionConfig.getTransformConfigs();
        if (transformConfigs == null) {
          transformConfigs = new ArrayList<>();
          ingestionConfig.setTransformConfigs(transformConfigs);
        }
      }
      for (Map.Entry<String, List<TimestampIndexGranularity>> entry : timestampIndexConfigs.entrySet()) {
        String timestampColumn = entry.getKey();
        for (TimestampIndexGranularity granularity : entry.getValue()) {
          String columnWithGranularity = getColumnWithGranularity(timestampColumn, granularity);
          if (!schemaApplied) {
            schema.addField(getFieldSpecWithGranularity(columnWithGranularity, granularity));
          }
          if (!tableConfigApplied) {
            transformConfigs.add(
                new TransformConfig(columnWithGranularity, getTransformExpression(timestampColumn, granularity)));
            rangeIndexColumns.add(columnWithGranularity);
          }
        }
      }
    }
  }

  private static DateTimeFieldSpec getFieldSpecWithGranularity(String columnWithGranularity,
      TimestampIndexGranularity granularity) {
    return new DateTimeFieldSpec(columnWithGranularity, DataType.TIMESTAMP,
        DateTimeFieldSpec.TimeFormat.TIMESTAMP.name(), FIELD_SPEC_GRANULARITY_MAP.get(granularity));
  }

  private static String getTransformExpression(String timestampColumn, TimestampIndexGranularity granularity) {
    return "dateTrunc('" + granularity + "',\"" + timestampColumn + "\")";
  }
}
