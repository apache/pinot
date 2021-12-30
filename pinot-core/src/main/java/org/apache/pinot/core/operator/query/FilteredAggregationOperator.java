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

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation only query on a single segment.
 */
@SuppressWarnings("rawtypes")
public class FilteredAggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "FilteredAggregationOperator";
  private static final String EXPLAIN_NAME = "FILTERED_AGGREGATE";

  private final AggregationFunction[] _aggregationFunctions;
  private final List<Pair<AggregationFunction[], TransformOperator>> _filteredAggregations;
  private final long _numTotalDocs;

  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;

  public FilteredAggregationOperator(AggregationFunction[] aggregationFunctions,
      List<Pair<AggregationFunction[], TransformOperator>> filteredAggregations, long numTotalDocs) {
    _aggregationFunctions = aggregationFunctions;
    _filteredAggregations = filteredAggregations;
    _numTotalDocs = numTotalDocs;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numAggregations = _aggregationFunctions.length;
    Object[] result = new Object[numAggregations];
    IdentityHashMap<AggregationFunction, Integer> resultIndexMap = new IdentityHashMap<>(numAggregations);
    for (int i = 0; i < numAggregations; i++) {
      resultIndexMap.put(_aggregationFunctions[i], i);
    }
    for (Pair<AggregationFunction[], TransformOperator> filteredAggregation : _filteredAggregations) {
      AggregationFunction[] aggregationFunctions = filteredAggregation.getLeft();
      AggregationExecutor aggregationExecutor = new DefaultAggregationExecutor(aggregationFunctions);
      TransformOperator transformOperator = filteredAggregation.getRight();
      TransformBlock transformBlock;
      int numDocsScanned = 0;
      while ((transformBlock = transformOperator.nextBlock()) != null) {
        aggregationExecutor.aggregate(transformBlock);
        numDocsScanned += transformBlock.getNumDocs();
      }
      List<Object> filteredResult = aggregationExecutor.getResult();
      int numFilteredAggregations = aggregationFunctions.length;
      for (int i = 0; i < numFilteredAggregations; i++) {
        result[resultIndexMap.get(aggregationFunctions[i])] = filteredResult.get(i);
      }
      _numDocsScanned += numDocsScanned;
      _numEntriesScannedInFilter += transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
      _numEntriesScannedPostFilter += (long) numDocsScanned * transformOperator.getNumColumnsProjected();
    }
    return new IntermediateResultsBlock(_aggregationFunctions, Arrays.asList(result), false);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return _filteredAggregations.stream().map(Pair::getRight).collect(Collectors.toList());
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(_numDocsScanned, _numEntriesScannedInFilter, _numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    // TODO: To be added
    return null;
  }
}
