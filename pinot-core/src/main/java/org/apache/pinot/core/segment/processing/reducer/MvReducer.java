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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.aggregator.ValueAggregator;
import org.apache.pinot.core.segment.processing.aggregator.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.framework.MaterializedViewProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.FieldType.DIMENSION;
import static org.apache.pinot.spi.data.FieldSpec.FieldType.METRIC;


/**
 * MvReducer aggregates part of metric values for GenericRows with the select dimension + time values.
 */
public class MvReducer implements Reducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MvReducer.class);

  private final String _partitionId;
  private final GenericRowFileManager _fileManager;
  private final Map<String, Set<AggregationFunctionType>> _aggregationFunctionSetMap;
  private final Map<String, Map<String, String>> _aggregationFunctionParameters;
  private final File _reducerOutputDir;
  private final TableConfig _mvTableConfig;
  private final Schema _mvSchema;

  private final Set<String> _selectedDimensionList;


  public MvReducer(String partitionId, GenericRowFileManager fileManager, File reducerOutputDir,
      SegmentProcessorConfig segmentProcessorConfig) {
    _partitionId = partitionId;
    _fileManager = fileManager;
    _reducerOutputDir = reducerOutputDir;
    _aggregationFunctionParameters = segmentProcessorConfig.getAggregationFunctionParameters();

    MaterializedViewProcessorConfig mvConfig = segmentProcessorConfig.getMaterializedViewProcessorConfig();
    _mvTableConfig = mvConfig.getMvTableConfig();
    _mvSchema = mvConfig.getMvSchema();
    _selectedDimensionList = mvConfig.getSelectedDimensions();
    _aggregationFunctionSetMap = mvConfig.getAggregationFunctionSetMap();
  }

  private Map mapFieldSpecs(List<FieldSpec> fieldSpecs) {
    Map<String, FieldSpec> fieldSpecMap = new HashMap<>();
    for (FieldSpec fieldSpec : fieldSpecs) {
      fieldSpecMap.put(fieldSpec.getName(), fieldSpec);
    }
    return fieldSpecMap;
  }

  @Override
  public GenericRowFileManager reduce()
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
    List<FieldSpec> aggFieldSpecs = new ArrayList<>(_mvSchema.getAllFieldSpecs());
    //TODO Rebuild fieldSpecs use new metrics and dimensions
    Map<String, FieldSpec> fieldSpecMap = mapFieldSpecs(fieldSpecs);
    Map<String, FieldSpec> aggFieldSpecMap = mapFieldSpecs(aggFieldSpecs);
    boolean includeNullFields = _fileManager.isIncludeNullFields();
    List<AggregatorContext> aggregatorContextList = new ArrayList<>();

    for (Map.Entry<String, Set<AggregationFunctionType>> entry : _aggregationFunctionSetMap.entrySet()) {
      String column = entry.getKey();
      FieldSpec fieldSpec = fieldSpecMap.get(column);
      for (AggregationFunctionType aggregatorContext : entry.getValue()) {
        if (fieldSpec != null && fieldSpec.getFieldType() == METRIC) {
          aggregatorContextList.add(new AggregatorContext(fieldSpec, aggregatorContext,
              _aggregationFunctionParameters.getOrDefault(fieldSpec.getName(), Collections.emptyMap())));
        }
      }
    }

    File partitionOutputDir = new File(_reducerOutputDir, _partitionId);
    FileUtils.forceMkdir(partitionOutputDir);
    LOGGER.info("Start creating rollup file under dir: {}", partitionOutputDir);
    long rollupFileCreationStartTimeMs = System.currentTimeMillis();
    GenericRowFileManager rollupFileManager =
        new GenericRowFileManager(partitionOutputDir, aggFieldSpecs, includeNullFields, 0);
    GenericRowFileWriter rollupFileWriter = rollupFileManager.getFileWriter();
    GenericRow previousRow = new GenericRow();
    recordReader.readAndTransform(0, previousRow, _aggregationFunctionSetMap);
    int previousRowId = 0;
    GenericRow buffer = new GenericRow();
    if (includeNullFields) {
      for (int i = 1; i < numRows; i++) {
        buffer.clear();
        recordReader.readAndTransform(i, buffer, _aggregationFunctionSetMap);
        if (recordReader.compare(previousRowId, i) == 0) {
          aggregateWithNullFields(previousRow, buffer, aggregatorContextList);
        } else {
          fillInvalidColumnWithNull(previousRow, aggFieldSpecMap);
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
        recordReader.readAndTransform(i, buffer, _aggregationFunctionSetMap);
        if (recordReader.compare(previousRowId, i) == 0) {
          aggregateWithoutNullFields(previousRow, buffer, aggregatorContextList);
        } else {
          fillInvalidColumnWithNull(previousRow, aggFieldSpecMap);
          rollupFileWriter.write(previousRow);
          previousRowId = i;
          GenericRow temp = previousRow;
          previousRow = buffer;
          buffer = temp;
        }
      }
    }
    fillInvalidColumnWithNull(previousRow, aggFieldSpecMap);
    rollupFileWriter.write(previousRow);
    rollupFileManager.closeFileWriter();
    LOGGER.info("Finish creating rollup file in {}ms", System.currentTimeMillis() - rollupFileCreationStartTimeMs);

    _fileManager.cleanUp();
    LOGGER.info("Finish reducing in {}ms", System.currentTimeMillis() - reduceStartTimeMs);
    return rollupFileManager;
  }

  private void fillInvalidColumnWithNull(GenericRow genericRow, Map<String, FieldSpec> aggFieldSpecMap) {
    List<String> columnList = new ArrayList(genericRow.getFieldToValueMap().keySet());
    if (columnList == null) {
      return;
    }
    for (String column : columnList) {
      if (!aggFieldSpecMap.containsKey(column)) {
        genericRow.removeValue(column);
      } else if (aggFieldSpecMap.get(column).getFieldType() == DIMENSION && !_selectedDimensionList.contains(column)) {
        genericRow.putDefaultNullValue(column, aggFieldSpecMap.get(column).getDefaultNullValue());
      }
    }
  }

  private static void aggregateWithNullFields(GenericRow aggregatedRow, GenericRow rowToAggregate,
      List<AggregatorContext> aggregatorContextList) {
    for (AggregatorContext aggregatorContext : aggregatorContextList) {
      String column = aggregatorContext._column;

      // Skip aggregating on null fields
      if (rowToAggregate.isNullValue(column)) {
        continue;
      }

      column = column + "_" + aggregatorContext._aggregator.getValueType();
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
      String column = aggregatorContext._column + "_" + aggregatorContext._aggregator.getValueType();
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
