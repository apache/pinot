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
package org.apache.pinot.core.operator.query;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;


/**
 * Aggregation operator that utilizes metadata for serving aggregation queries.
 */
public class MetadataBasedAggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "MetadataBasedAggregationOperator";

  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final Map<String, DataSource> _dataSourceMap;
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
      SegmentMetadata segmentMetadata, Map<String, DataSource> dataSourceMap) {
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
    long numTotalDocs = _segmentMetadata.getTotalDocs();
    for (AggregationFunctionContext aggregationFunctionContext : _aggregationFunctionContexts) {
      AggregationFunctionType functionType = aggregationFunctionContext.getAggregationFunction().getType();
      Preconditions.checkState(functionType == AggregationFunctionType.COUNT,
          "Metadata based aggregation operator does not support function type: " + functionType);
      aggregationResults.add(numTotalDocs);
    }

    // Create execution statistics. Set numDocsScanned to numTotalDocs for backward compatibility.
    _executionStatistics =
        new ExecutionStatistics(numTotalDocs, 0/*numEntriesScannedInFilter*/, 0/*numEntriesScannedPostFilter*/,
            numTotalDocs);

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
