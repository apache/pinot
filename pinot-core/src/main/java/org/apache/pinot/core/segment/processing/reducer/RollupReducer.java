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


/// RollupReducer aggregates the metric values for GenericRows with the same dimension + time values.
///
/// When order sensitive aggregations (FIRSTWITHTIME/LASTWITHTIME) are configured, rows within a rollup group are
/// sorted by the hidden original time column appended by the mapper. Rows with identical original time values are
/// ordered arbitrarily (the sort is not stable), so first/last picks among exact ties non-deterministically,
/// including across task retries.
public class RollupReducer implements Reducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollupReducer.class);
  private static final AggregationFunctionType DEFAULT_AGGREGATOR_TYPE = AggregationFunctionType.SUM;

  private final String _partitionId;
  private final GenericRowFileManager _fileManager;
  private final Map<String, AggregationFunctionType> _aggregationTypes;
  private final Map<String, Map<String, String>> _aggregationFunctionParameters;
  private final File _reducerOutputDir;
  private GenericRowFileManager _rollupFileManager;

  public RollupReducer(String partitionId, GenericRowFileManager fileManager,
      Map<String, AggregationFunctionType> aggregationTypes,
      Map<String, Map<String, String>> aggregationFunctionParameters, File reducerOutputDir) {
    _partitionId = partitionId;
    _fileManager = fileManager;
    _aggregationTypes = aggregationTypes;
    _aggregationFunctionParameters = aggregationFunctionParameters;
    _reducerOutputDir = reducerOutputDir;
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
    // When order sensitive aggregations (FIRSTWITHTIME/LASTWITHTIME) are configured, the mapper appends a hidden
    // column carrying the original (pre-rounding) time value as the last sort field, so that rows within the same
    // rollup group are sorted by the original time. The hidden column is not part of the group key, and is stripped
    // from the rollup output. The flag is carried explicitly by the file manager (set by the mapper that added the
    // field) instead of being inferred from the column name, so that a schema column which happens to share the name
    // can never be mistaken for the hidden field.
    boolean hasOriginalTimeField = _fileManager.hasOriginalTimeField();
    int numGroupFields = hasOriginalTimeField ? numSortFields - 1 : numSortFields;
    List<FieldSpec> outputFieldSpecs = fieldSpecs;
    if (hasOriginalTimeField) {
      outputFieldSpecs = new ArrayList<>(fieldSpecs);
      outputFieldSpecs.remove(numSortFields - 1);
    }
    List<AggregatorContext> aggregatorContextList = new ArrayList<>();
    for (FieldSpec fieldSpec : outputFieldSpecs) {
      if (fieldSpec.getFieldType() == FieldType.METRIC) {
        AggregationFunctionType aggregationType =
            _aggregationTypes.getOrDefault(fieldSpec.getName(), DEFAULT_AGGREGATOR_TYPE);
        if (ValueAggregatorFactory.requiresTimeOrdering(aggregationType) && !hasOriginalTimeField) {
          throw new IllegalStateException(String.format(
              "Aggregation type: %s on column: %s requires a time column with EPOCH time handling",
              aggregationType, fieldSpec.getName()));
        }
        aggregatorContextList.add(new AggregatorContext(fieldSpec, aggregationType,
            _aggregationFunctionParameters.getOrDefault(fieldSpec.getName(), Collections.emptyMap())));
      }
    }

    File partitionOutputDir = new File(_reducerOutputDir, _partitionId);
    FileUtils.forceMkdir(partitionOutputDir);
    LOGGER.info("Start creating rollup file under dir: {}", partitionOutputDir);
    long rollupFileCreationStartTimeMs = System.currentTimeMillis();
    _rollupFileManager = new GenericRowFileManager(partitionOutputDir, outputFieldSpecs, includeNullFields, 0);
    GenericRowFileWriter rollupFileWriter = _rollupFileManager.getFileWriter();
    GenericRow previousRow = new GenericRow();
    recordReader.read(0, previousRow);
    int previousRowId = 0;
    GenericRow buffer = new GenericRow();
    if (includeNullFields) {
      for (int i = 1; i < numRows; i++) {
        buffer.clear();
        recordReader.read(i, buffer);
        if (recordReader.compare(previousRowId, i, numGroupFields) == 0) {
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
        if (recordReader.compare(previousRowId, i, numGroupFields) == 0) {
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
    _rollupFileManager.closeFileWriter();
    LOGGER.info("Finish creating rollup file in {}ms", System.currentTimeMillis() - rollupFileCreationStartTimeMs);

    _fileManager.cleanUp();
    LOGGER.info("Finish reducing in {}ms", System.currentTimeMillis() - reduceStartTimeMs);
    return _rollupFileManager;
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
