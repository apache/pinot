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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.recordtransformer.DataTypeTransformer;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stats container that efficiently collects statistics from an immutable segment using column-wise iteration.
 * Uses existing PreIndexStatsCollector implementations for statistics collection, similar to
 * SegmentPreIndexStatsCollectorImpl but iterates by column instead of by row. For existing columns,
 * reads column values using PinotSegmentColumnReader. For new columns, uses default values to generate statistics.
 * Column statistics are calculated based on the target schema from SegmentIndexCreationDriverImpl.
 *
 * <p>Supports data type conversions during schema evolution using Pinot's built-in PinotDataType.convert() system.
 * This handles all compatible conversions including numeric type changes (INT ↔ LONG ↔ FLOAT ↔ DOUBLE ↔ BIG_DECIMAL)
 * and other supported transformations like LONG → STRING according to Pinot's compatibility rules. Supports both
 * single-value and multi-value column conversions following the same pattern as DataTypeTransformer.
 *
 * <p>Note: This class is designed to work with columnar segments and assumes that the segment is immutable, does not
 * need resorting of data or re-transformation of rows.
 */
public class SegmentColumnarPreIndexStatsContainer implements SegmentPreIndexStatsContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarPreIndexStatsContainer.class);

  private final IndexSegment _sourceSegment;
  private final Schema _targetSchema;
  private final StatsCollectorConfig _statsCollectorConfig;
  private final Map<String, AbstractColumnStatisticsCollector> _columnStatsCollectorMap;
  private final int _totalDocCount;
  private final Schema _sourceSchema;

    public SegmentColumnarPreIndexStatsContainer(IndexSegment sourceSegment, Schema targetSchema,
                                               StatsCollectorConfig statsCollectorConfig) {
    _sourceSegment = sourceSegment;
    _targetSchema = targetSchema;
    _statsCollectorConfig = statsCollectorConfig;
    _totalDocCount = sourceSegment.getSegmentMetadata().getTotalDocs();
    _sourceSchema = sourceSegment.getSegmentMetadata().getSchema();
    _columnStatsCollectorMap = new HashMap<>();

    initializeStatsCollectors();
    collectColumnStats();
  }

  /**
   * Initialize stats collectors for all columns in the target schema, similar to SegmentPreIndexStatsCollectorImpl.
   */
  private void initializeStatsCollectors() {
    for (FieldSpec fieldSpec : _targetSchema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      switch (fieldSpec.getDataType().getStoredType()) {
        case INT:
          _columnStatsCollectorMap.put(columnName,
              new IntColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case LONG:
          _columnStatsCollectorMap.put(columnName,
              new LongColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case FLOAT:
          _columnStatsCollectorMap.put(columnName,
              new FloatColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case DOUBLE:
          _columnStatsCollectorMap.put(columnName,
              new DoubleColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case BIG_DECIMAL:
          _columnStatsCollectorMap.put(columnName,
              new BigDecimalColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case STRING:
          _columnStatsCollectorMap.put(columnName,
              new StringColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case BYTES:
          _columnStatsCollectorMap.put(columnName,
              new BytesColumnPredIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        case MAP:
          _columnStatsCollectorMap.put(columnName,
              new MapColumnPreIndexStatsCollector(columnName, _statsCollectorConfig));
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + fieldSpec.getDataType());
      }
    }
  }

  /**
   * Collect stats by iterating column-wise instead of row-wise.
   * For existing columns, read values using PinotSegmentColumnReader.
   * For new columns, use default values.
   */
  private void collectColumnStats() {
    Set<String> sourceColumns = _sourceSegment.getPhysicalColumnNames();

    LOGGER.info("Collecting stats for {} columns using column-wise iteration",
        _targetSchema.getPhysicalColumnNames().size());

    for (FieldSpec fieldSpec : _targetSchema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      AbstractColumnStatisticsCollector statsCollector = _columnStatsCollectorMap.get(columnName);

      if (sourceColumns.contains(columnName)) {
        // Column exists in source segment - read column values and collect stats
        LOGGER.debug("Collecting stats by reading column values for existing column: {}", columnName);
        collectStatsFromExistingColumn(columnName, statsCollector);
      } else {
        // New column - collect stats using default values
        LOGGER.debug("Collecting stats using default values for new column: {}", columnName);
        collectStatsFromDefaultValues(columnName, fieldSpec, statsCollector);
      }

      // Seal the stats collector
      statsCollector.seal();
    }
  }

  /**
   * Collect stats from existing column by reading all column values using PinotSegmentColumnReader.
   * This approach is similar to SegmentPreIndexStatsCollectorImpl but iterates by column instead of by row.
   * Handles data type conversion when the target schema has a different data type than the source segment.
   */
  private void collectStatsFromExistingColumn(String columnName, AbstractColumnStatisticsCollector statsCollector) {
    // todo: An optimisation here can be to utilise the dictionary in the segment instead of reading values
    LOGGER.debug("Collecting stats by reading column values for column: {}", columnName);

    FieldSpec targetFieldSpec = _targetSchema.getFieldSpecFor(columnName);
    FieldSpec.DataType targetDataType = targetFieldSpec.getDataType();

    // Check if data type conversion is needed by comparing source and target data types
    FieldSpec sourceFieldSpec = _sourceSchema.getFieldSpecFor(columnName);
    FieldSpec.DataType sourceDataType = sourceFieldSpec != null ? sourceFieldSpec.getDataType() : null;
    boolean needsConversion = sourceDataType != null && !sourceDataType.equals(targetDataType);

    if (needsConversion) {
      LOGGER.debug("Data type conversion needed for column: {} from {} to {}",
          columnName, sourceDataType, targetDataType);
    }

    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_sourceSegment, columnName)) {
      for (int docId = 0; docId < _totalDocCount; docId++) {
        Object value = columnReader.getValue(docId);
        // Convert value to target data type only if data types differ
        Object valueToCollect = needsConversion
            ? convertValueToTargetDataType(value, targetDataType, columnName)
            : value;
        statsCollector.collect(valueToCollect);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to collect stats for column: {}", columnName, e);
      throw new RuntimeException("Failed to collect stats for column: " + columnName, e);
    }
  }

  /**
   * Collect stats for new columns using default values.
   * Instead of reading document values, collect stats by using the default value for all documents.
   */
  private void collectStatsFromDefaultValues(String columnName, FieldSpec fieldSpec,
      AbstractColumnStatisticsCollector statsCollector) {
    // todo: An optimisation here can be to not iterate over all documents as the default value is the same for all
    LOGGER.debug("Collecting stats using default values for new column: {}", columnName);

    Object defaultValue = fieldSpec.getDefaultNullValue();

    // Collect the default value for all documents
    for (int docId = 0; docId < _totalDocCount; docId++) {
      statsCollector.collect(defaultValue);
    }
  }

  /**
   * Convert value from source segment data type to target schema data type using DataTypeTransformer's
   * conversion logic. This ensures consistency with the standard data transformation pipeline.
   */
  private Object convertValueToTargetDataType(Object value, FieldSpec.DataType targetDataType, String columnName) {
    if (value == null) {
      return null;
    }

    try {
      // Get target field spec to determine the target PinotDataType
      FieldSpec targetFieldSpec = _targetSchema.getFieldSpecFor(columnName);
      PinotDataType targetType = PinotDataType.getPinotDataTypeForIngestion(targetFieldSpec);

            // Use DataTypeTransformer's conversion logic for consistency
      return DataTypeTransformer.convertValue(value, targetType, columnName);
    } catch (Exception e) {
      LOGGER.warn("Failed to convert value '{}' to target data type {} for column {}: {}. Using original value.",
          value, targetDataType, columnName, e.getMessage());
      return value;
    }
  }

  @Override
  public ColumnStatistics getColumnProfileFor(String column) {
    return _columnStatsCollectorMap.get(column);
  }

  @Override
  public int getTotalDocCount() {
    return _totalDocCount;
  }
}
