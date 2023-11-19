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
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.executor.StarTreeAggregationExecutor;


/**
 * This operator processes a collection of filtered (and potentially non filtered) aggregations.
 *
 * For a query with either all aggregations being filtered or a mix of filtered and non filtered aggregations,
 * FilteredAggregationOperator will come into execution.
 */
@SuppressWarnings("rawtypes")
public class FilteredAggregationOperator extends BaseOperator<AggregationResultsBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE_FILTERED";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final List<AggregationInfo> _aggregationInfos;
  private final long _numTotalDocs;

  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;

  public FilteredAggregationOperator(QueryContext queryContext, List<AggregationInfo> aggregationInfos,
      long numTotalDocs) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _aggregationInfos = aggregationInfos;
    _numTotalDocs = numTotalDocs;
  }

  @Override
  protected AggregationResultsBlock getNextBlock() {
    int numAggregations = _aggregationFunctions.length;
    Object[] result = new Object[numAggregations];
    IdentityHashMap<AggregationFunction, Integer> resultIndexMap = new IdentityHashMap<>(numAggregations);
    for (int i = 0; i < numAggregations; i++) {
      resultIndexMap.put(_aggregationFunctions[i], i);
    }

    for (AggregationInfo aggregationInfo : _aggregationInfos) {
      AggregationFunction[] aggregationFunctions = aggregationInfo.getFunctions();
      BaseProjectOperator<?> projectOperator = aggregationInfo.getProjectOperator();
      AggregationExecutor aggregationExecutor;
      if (aggregationInfo.isUseStarTree()) {
        aggregationExecutor = new StarTreeAggregationExecutor(aggregationFunctions);
      } else {
        aggregationExecutor = new DefaultAggregationExecutor(aggregationFunctions);
      }

      ValueBlock valueBlock;
      int numDocsScanned = 0;
      while ((valueBlock = projectOperator.nextBlock()) != null) {
        aggregationExecutor.aggregate(valueBlock);
        numDocsScanned += valueBlock.getNumDocs();
      }
      List<Object> filteredResult = aggregationExecutor.getResult();

      for (int i = 0; i < aggregationFunctions.length; i++) {
        result[resultIndexMap.get(aggregationFunctions[i])] = filteredResult.get(i);
      }
      _numDocsScanned += numDocsScanned;
      _numEntriesScannedInFilter += projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
      _numEntriesScannedPostFilter += (long) numDocsScanned * projectOperator.getNumColumnsProjected();
    }
    return new AggregationResultsBlock(_aggregationFunctions, Arrays.asList(result), _queryContext);
  }

  @Override
  public List<Operator> getChildOperators() {
    return _aggregationInfos.stream().map(AggregationInfo::getProjectOperator).collect(Collectors.toList());
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(_numDocsScanned, _numEntriesScannedInFilter, _numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
