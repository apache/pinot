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
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * Aggregation operator that utilizes metadata for serving aggregation queries.
 */
@SuppressWarnings("rawtypes")
public class MetadataBasedAggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "MetadataBasedAggregationOperator";

  private final AggregationFunction[] _aggregationFunctions;
  private final SegmentMetadata _segmentMetadata;
  private final Map<String, DataSource> _dataSourceMap;

  public MetadataBasedAggregationOperator(AggregationFunction[] aggregationFunctions, SegmentMetadata segmentMetadata,
      Map<String, DataSource> dataSourceMap) {
    _aggregationFunctions = aggregationFunctions;
    _segmentMetadata = segmentMetadata;

    // Datasource is currently not used, but will start getting used as we add support for aggregation
    // functions other than count(*).
    _dataSourceMap = dataSourceMap;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numAggregationFunctions = _aggregationFunctions.length;
    List<Object> aggregationResults = new ArrayList<>(numAggregationFunctions);
    long numTotalDocs = _segmentMetadata.getTotalDocs();
    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      Preconditions.checkState(aggregationFunction.getType() == AggregationFunctionType.COUNT,
          "Metadata based aggregation operator does not support function type: " + aggregationFunction.getType());
      aggregationResults.add(numTotalDocs);
    }

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctions, aggregationResults, false);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
    int numTotalDocs = _segmentMetadata.getTotalDocs();
    return new ExecutionStatistics(numTotalDocs, 0, 0, numTotalDocs);
  }
}
