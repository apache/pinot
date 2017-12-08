/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.query;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Aggregation operator that utilizes metadata for serving aggregation queries.
 */
public class MetadataBasedAggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "MetadataBasedAggregationOperator";

  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final Map<String, BaseOperator> _dataSourceMap;
  private final SegmentMetadata _segmentMetadata;
  private ExecutionStatistics _executionStatistics;

  /**
   * Constructor for the class.
   *
   * @param aggregationFunctionContexts Aggregation function contexts.
   * @param segmentMetadata Segment metadata.
   * @param dataSourceMap Map of column to its data source.
   */
  public MetadataBasedAggregationOperator(AggregationFunctionContext[] aggregationFunctionContexts,
      SegmentMetadata segmentMetadata, Map<String, BaseOperator> dataSourceMap) {
    _aggregationFunctionContexts = aggregationFunctionContexts;

    // Datasource is currently not used, but will start getting used as we add support for aggregation
    // functions other than count(*).
    _dataSourceMap = dataSourceMap;
    _segmentMetadata = segmentMetadata;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numAggregationFunctions = _aggregationFunctionContexts.length;
    List<Object> aggregationResults = new ArrayList<>(numAggregationFunctions);
    int totalRawDocs = _segmentMetadata.getTotalRawDocs();

    for (AggregationFunctionContext aggregationFunctionContext : _aggregationFunctionContexts) {
      AggregationFunction function = aggregationFunctionContext.getAggregationFunction();
      AggregationFunctionFactory.AggregationFunctionType functionType =
          AggregationFunctionFactory.AggregationFunctionType.valueOf(function.getName().toUpperCase());
      String column = aggregationFunctionContext.getAggregationColumns()[0];

      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) _segmentMetadata;
      AggregationResultHolder resultHolder;
      switch (functionType) {
        case COUNT:
          resultHolder = new DoubleAggregationResultHolder(totalRawDocs);
          break;
        case MIN:
          String minValue = segmentMetadata.getColumnMetadataFor(column).getMinValue().toString();
          resultHolder = new DoubleAggregationResultHolder(Double.valueOf(minValue));
          break;
        case MAX:
          String maxValue = segmentMetadata.getColumnMetadataFor(column).getMaxValue().toString();
          resultHolder = new DoubleAggregationResultHolder(Double.valueOf(maxValue));
          break;
        case MINMAXRANGE:
          String minValueRange = segmentMetadata.getColumnMetadataFor(column).getMinValue().toString();
          String maxValueRange = segmentMetadata.getColumnMetadataFor(column).getMaxValue().toString();
          resultHolder = new ObjectAggregationResultHolder();
          resultHolder.setValue(new MinMaxRangePair(Double.valueOf(minValueRange), Double.valueOf(maxValueRange)));
          break;

        default:
          throw new UnsupportedOperationException(
              "Metadata based aggregation operator does not support function " + function.getName());
      }
      aggregationResults.add(function.extractAggregationResult(resultHolder));
    }

    // Create execution statistics. Set numDocsScanned to totalRawDocs for backward compatibility.
    _executionStatistics =
        new ExecutionStatistics(totalRawDocs, 0/*numEntriesScannedInFilter*/, 0/*numEntriesScannedPostFilter*/,
            totalRawDocs);

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctionContexts, aggregationResults, false);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
