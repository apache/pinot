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
package org.apache.pinot.core.segment.processing.reducer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.segment.processing.aggregator.ValueAggregator;
import org.apache.pinot.core.segment.processing.aggregator.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RollupReducer aggregates the metric values for GenericRows with the same dimension + time values.
 */
public class RollupReducer implements Reducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollupReducer.class);
  private static final AggregationFunctionType DEFAULT_AGGREGATOR_TYPE = AggregationFunctionType.SUM;

  private final String _partitionId;
  private final GenericRowFileManager _fileManager;
  private final Map<String, AggregationFunctionType> _aggregationTypes;
  private final Map<String, Map<String, String>> _aggregationFunctionParameters;
  private final File _reducerOutputDir;
  private final int _maxBatchSize;
  private GenericRowFileManager _rollupFileManager;

  public RollupReducer(String partitionId, GenericRowFileManager fileManager,
      Map<String, AggregationFunctionType> aggregationTypes,
      Map<String, Map<String, String>> aggregationFunctionParameters, File reducerOutputDir) {
    this(partitionId, fileManager, aggregationTypes, aggregationFunctionParameters, reducerOutputDir,
        MinionConstants.MergeTask.DEFAULT_REDUCER_MAX_BATCH_SIZE);
  }

  public RollupReducer(String partitionId, GenericRowFileManager fileManager,
      Map<String, AggregationFunctionType> aggregationTypes,
      Map<String, Map<String, String>> aggregationFunctionParameters, File reducerOutputDir, int maxBatchSize) {
    _partitionId = partitionId;
    _fileManager = fileManager;
    _aggregationTypes = aggregationTypes;
    _aggregationFunctionParameters = aggregationFunctionParameters;
    _reducerOutputDir = reducerOutputDir;
    _maxBatchSize = maxBatchSize;
  }

  @Override
  public GenericRowFileManager reduce()
      throws Exception {
    try {
      return doReduce();
    } catch (Exception e) {
      // Cleaning up resources created by the reducer, leaving others to the caller like the input _fileManager.
      if (_rollupFileManager != null) {
        _rollupFileManager.cleanUp();
      }
      throw e;
    }
  }

  private GenericRowFileManager doReduce()
      throws Exception {
    LOGGER.info("Start reducing on partition: {}", _partitionId);
    long reduceStartTimeMs = System.currentTimeMillis();

    GenericRowFileReader fileReader = _fileManager.getFileReader();
    int numRows = fileReader.getNumRows();
    int numSortFields = fileReader.getNumSortFields();
    LOGGER.info("Start sorting on numRows: {}, numSortFields: {}", numRows, numSortFields);
    long sortStartTimeMs = System.currentTimeMillis();
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    LOGGER.info("Finish sorting in {}ms", System.currentTimeMillis() - sortStartTimeMs);

    List<FieldSpec> fieldSpecs = _fileManager.getFieldSpecs();
    boolean includeNullFields = _fileManager.isIncludeNullFields();
    List<AggregatorContext> aggregatorContextList = new ArrayList<>();
    for (FieldSpec fieldSpec : fieldSpecs) {
      if (fieldSpec.getFieldType() == FieldType.METRIC) {
        aggregatorContextList.add(new AggregatorContext(fieldSpec,
            _aggregationTypes.getOrDefault(fieldSpec.getName(), DEFAULT_AGGREGATOR_TYPE),
            _aggregationFunctionParameters.getOrDefault(fieldSpec.getName(), Collections.emptyMap())));
      }
    }

    File partitionOutputDir = new File(_reducerOutputDir, _partitionId);
    FileUtils.forceMkdir(partitionOutputDir);
    LOGGER.info("Start creating rollup file under dir: {}", partitionOutputDir);
    long rollupFileCreationStartTimeMs = System.currentTimeMillis();
    _rollupFileManager = new GenericRowFileManager(partitionOutputDir, fieldSpecs, includeNullFields, 0);
    GenericRowFileWriter rollupFileWriter = _rollupFileManager.getFileWriter();

    // Check if any aggregators support batch aggregation
    long batchAggregatorCount = aggregatorContextList.stream()
        .filter(ctx -> ctx._aggregator.supportsBatchAggregation())
        .count();

    if (batchAggregatorCount > 0) {
      LOGGER.info("Using hybrid aggregation for partition: {} ({} batch, {} pairwise)",
          _partitionId, batchAggregatorCount, aggregatorContextList.size() - batchAggregatorCount);
      reduceHybrid(recordReader, numRows, aggregatorContextList, rollupFileWriter, includeNullFields);
    } else {
      reducePairwise(recordReader, numRows, aggregatorContextList, rollupFileWriter, includeNullFields);
    }

    _rollupFileManager.closeFileWriter();
    LOGGER.info("Finish creating rollup file in {}ms", System.currentTimeMillis() - rollupFileCreationStartTimeMs);

    _fileManager.cleanUp();
    LOGGER.info("Finish reducing in {}ms", System.currentTimeMillis() - reduceStartTimeMs);
    return _rollupFileManager;
  }

  /**
   * Pairwise reduce - the original implementation that aggregates rows one at a time.
   */
  private void reducePairwise(GenericRowFileRecordReader recordReader, int numRows,
      List<AggregatorContext> aggregatorContextList, GenericRowFileWriter rollupFileWriter, boolean includeNullFields)
      throws Exception {
    GenericRow previousRow = new GenericRow();
    recordReader.read(0, previousRow);
    int previousRowId = 0;
    GenericRow buffer = new GenericRow();
    if (includeNullFields) {
      for (int i = 1; i < numRows; i++) {
        buffer.clear();
        recordReader.read(i, buffer);
        if (recordReader.compare(previousRowId, i) == 0) {
          aggregateWithNullFields(previousRow, buffer, aggregatorContextList);
        } else {
          rollupFileWriter.write(previousRow);
          previousRowId = i;
          GenericRow temp = previousRow;
          previousRow = buffer;
          buffer = temp;
        }
      }
    } else {
      for (int i = 1; i < numRows; i++) {
        buffer.clear();
        recordReader.read(i, buffer);
        if (recordReader.compare(previousRowId, i) == 0) {
          aggregateWithoutNullFields(previousRow, buffer, aggregatorContextList);
        } else {
          rollupFileWriter.write(previousRow);
          previousRowId = i;
          GenericRow temp = previousRow;
          previousRow = buffer;
          buffer = temp;
        }
      }
    }
    rollupFileWriter.write(previousRow);
  }

  /**
   * Hybrid reduce - uses batch aggregation only for aggregators that support it,
   * pairwise aggregation for others. This optimizes memory usage by only buffering
   * values for columns that benefit from batch aggregation (e.g., sketches).
   */
  private void reduceHybrid(GenericRowFileRecordReader recordReader, int numRows,
      List<AggregatorContext> aggregatorContextList, GenericRowFileWriter rollupFileWriter, boolean includeNullFields)
      throws Exception {
    // Only allocate batch storage for aggregators that support it
    List<List<Object>> batchValues = new ArrayList<>(aggregatorContextList.size());
    for (int j = 0; j < aggregatorContextList.size(); j++) {
      if (aggregatorContextList.get(j)._aggregator.supportsBatchAggregation()) {
        batchValues.add(new ArrayList<>());
      } else {
        batchValues.add(null);  // null indicates pairwise aggregation
      }
    }

    GenericRow baseRow = new GenericRow();
    recordReader.read(0, baseRow);
    int baseRowId = 0;

    // Initialize batch values for batch-supporting aggregators
    for (int j = 0; j < aggregatorContextList.size(); j++) {
      if (batchValues.get(j) != null) {
        String column = aggregatorContextList.get(j)._column;
        if (!includeNullFields || !baseRow.isNullValue(column)) {
          batchValues.get(j).add(baseRow.getValue(column));
        }
      }
    }

    GenericRow currentRow = new GenericRow();
    for (int i = 1; i < numRows; i++) {
      currentRow.clear();
      recordReader.read(i, currentRow);

      if (recordReader.compare(baseRowId, i) == 0) {
        // Same key - batch or aggregate pairwise depending on aggregator
        for (int j = 0; j < aggregatorContextList.size(); j++) {
          AggregatorContext ctx = aggregatorContextList.get(j);
          String column = ctx._column;

          if (batchValues.get(j) != null) {
            // Batch aggregation - collect value
            if (!includeNullFields || !currentRow.isNullValue(column)) {
              batchValues.get(j).add(currentRow.getValue(column));
            }
          } else {
            // Pairwise aggregation - aggregate immediately (O(1) memory)
            if (includeNullFields) {
              if (!currentRow.isNullValue(column)) {
                if (baseRow.removeNullValueField(column)) {
                  baseRow.putValue(column, currentRow.getValue(column));
                } else {
                  baseRow.putValue(column, ctx._aggregator.aggregate(
                      baseRow.getValue(column), currentRow.getValue(column), ctx._functionParameters));
                }
              }
            } else {
              baseRow.putValue(column, ctx._aggregator.aggregate(
                  baseRow.getValue(column), currentRow.getValue(column), ctx._functionParameters));
            }
          }
        }

        // Memory safety: flush partial batch results if batch gets too large
        int currentBatchSize = 0;
        for (List<Object> batch : batchValues) {
          if (batch != null && batch.size() > currentBatchSize) {
            currentBatchSize = batch.size();
          }
        }
        if (currentBatchSize >= _maxBatchSize) {
          flushBatchToBaseRow(baseRow, batchValues, aggregatorContextList, includeNullFields);
        }
      } else {
        // Key changed - finalize batch aggregations and write
        finalizeBatchAndWrite(baseRow, batchValues, aggregatorContextList, rollupFileWriter, includeNullFields);

        // Start new key
        baseRowId = i;
        baseRow.clear();
        recordReader.read(i, baseRow);

        // Reset batch values for batch-supporting aggregators
        for (int j = 0; j < aggregatorContextList.size(); j++) {
          if (batchValues.get(j) != null) {
            batchValues.get(j).clear();
            String column = aggregatorContextList.get(j)._column;
            if (!includeNullFields || !baseRow.isNullValue(column)) {
              batchValues.get(j).add(baseRow.getValue(column));
            }
          }
        }
      }
    }

    // Write final key
    finalizeBatchAndWrite(baseRow, batchValues, aggregatorContextList, rollupFileWriter, includeNullFields);
  }

  /**
   * Flush batch values into the base row (partial aggregation for memory safety).
   * Only processes columns that use batch aggregation.
   */
  private void flushBatchToBaseRow(GenericRow baseRow, List<List<Object>> batchValues,
      List<AggregatorContext> aggregatorContextList, boolean includeNullFields) {
    for (int j = 0; j < aggregatorContextList.size(); j++) {
      List<Object> values = batchValues.get(j);
      if (values != null && !values.isEmpty()) {
        AggregatorContext ctx = aggregatorContextList.get(j);
        Object aggregated = ctx._aggregator.aggregateBatch(values, ctx._functionParameters);
        baseRow.putValue(ctx._column, aggregated);
        if (includeNullFields) {
          baseRow.removeNullValueField(ctx._column);
        }
        values.clear();
        values.add(aggregated);  // Continue with partial result
      }
    }
  }

  /**
   * Finalize batch aggregations and write the result.
   * Pairwise columns already have their final values in baseRow.
   */
  private void finalizeBatchAndWrite(GenericRow baseRow, List<List<Object>> batchValues,
      List<AggregatorContext> aggregatorContextList, GenericRowFileWriter rollupFileWriter, boolean includeNullFields)
      throws Exception {
    for (int j = 0; j < aggregatorContextList.size(); j++) {
      List<Object> values = batchValues.get(j);
      if (values != null && !values.isEmpty()) {
        AggregatorContext ctx = aggregatorContextList.get(j);
        Object aggregated = ctx._aggregator.aggregateBatch(values, ctx._functionParameters);
        baseRow.putValue(ctx._column, aggregated);
        if (includeNullFields) {
          baseRow.removeNullValueField(ctx._column);
        }
      }
    }
    rollupFileWriter.write(baseRow);
  }

  private static void aggregateWithNullFields(GenericRow aggregatedRow, GenericRow rowToAggregate,
      List<AggregatorContext> aggregatorContextList) {
    for (AggregatorContext aggregatorContext : aggregatorContextList) {
      String column = aggregatorContext._column;

      // Skip aggregating on null fields
      if (rowToAggregate.isNullValue(column)) {
        continue;
      }

      if (aggregatedRow.removeNullValueField(column)) {
        // Null field, directly put new value
        aggregatedRow.putValue(column, rowToAggregate.getValue(column));
      } else {
        // Non-null field, aggregate the value
        aggregatedRow.putValue(column,
            aggregatorContext._aggregator.aggregate(aggregatedRow.getValue(column), rowToAggregate.getValue(column),
                aggregatorContext._functionParameters));
      }
    }
  }

  private static void aggregateWithoutNullFields(GenericRow aggregatedRow, GenericRow rowToAggregate,
      List<AggregatorContext> aggregatorContextList) {
    for (AggregatorContext aggregatorContext : aggregatorContextList) {
      String column = aggregatorContext._column;
      aggregatedRow.putValue(column,
          aggregatorContext._aggregator.aggregate(aggregatedRow.getValue(column), rowToAggregate.getValue(column),
              aggregatorContext._functionParameters));
    }
  }

  private static class AggregatorContext {
    final String _column;
    final ValueAggregator _aggregator;
    final Map<String, String> _functionParameters;

    AggregatorContext(FieldSpec fieldSpec, AggregationFunctionType aggregationType,
        Map<String, String> functionParameters) {
      _column = fieldSpec.getName();
      _aggregator = ValueAggregatorFactory.getValueAggregator(aggregationType, fieldSpec.getDataType());
      _functionParameters = functionParameters;
    }
  }
}
