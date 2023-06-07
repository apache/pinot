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


/**
 * RollupReducer aggregates the metric values for GenericRows with the same dimension + time values.
 */
public class RollupReducer implements Reducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollupReducer.class);
  private static final AggregationFunctionType DEFAULT_AGGREGATOR_TYPE = AggregationFunctionType.SUM;

  private final String _partitionId;
  private final GenericRowFileManager _fileManager;
  private final Map<String, AggregationFunctionType> _aggregationTypes;
  private final File _reducerOutputDir;
  private GenericRowFileManager _rollupFileManager;

  public RollupReducer(String partitionId, GenericRowFileManager fileManager,
      Map<String, AggregationFunctionType> aggregationTypes, File reducerOutputDir) {
    _partitionId = partitionId;
    _fileManager = fileManager;
    _aggregationTypes = aggregationTypes;
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
    List<AggregatorContext> aggregatorContextList = new ArrayList<>();
    for (FieldSpec fieldSpec : fieldSpecs) {
      if (fieldSpec.getFieldType() == FieldType.METRIC) {
        aggregatorContextList.add(new AggregatorContext(fieldSpec,
            _aggregationTypes.getOrDefault(fieldSpec.getName(), DEFAULT_AGGREGATOR_TYPE)));
      }
    }

    File partitionOutputDir = new File(_reducerOutputDir, _partitionId);
    FileUtils.forceMkdir(partitionOutputDir);
    LOGGER.info("Start creating rollup file under dir: {}", partitionOutputDir);
    long rollupFileCreationStartTimeMs = System.currentTimeMillis();
    _rollupFileManager = new GenericRowFileManager(partitionOutputDir, fieldSpecs, includeNullFields, 0);
    GenericRowFileWriter rollupFileWriter = _rollupFileManager.getFileWriter();
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
            aggregatorContext._aggregator.aggregate(aggregatedRow.getValue(column), rowToAggregate.getValue(column)));
      }
    }
  }

  private static void aggregateWithoutNullFields(GenericRow aggregatedRow, GenericRow rowToAggregate,
      List<AggregatorContext> aggregatorContextList) {
    for (AggregatorContext aggregatorContext : aggregatorContextList) {
      String column = aggregatorContext._column;
      aggregatedRow.putValue(column,
          aggregatorContext._aggregator.aggregate(aggregatedRow.getValue(column), rowToAggregate.getValue(column)));
    }
  }

  private static class AggregatorContext {
    final String _column;
    final ValueAggregator _aggregator;

    AggregatorContext(FieldSpec fieldSpec, AggregationFunctionType aggregationType) {
      _column = fieldSpec.getName();
      _aggregator = ValueAggregatorFactory.getValueAggregator(aggregationType, fieldSpec.getDataType());
    }
  }
}
