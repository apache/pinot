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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * Aggregation operator that utilizes dictionary or column metadata for serving aggregation queries to avoid scanning.
 * The scanless operator is selected in the plan maker, if the query is of aggregation type min, max, minmaxrange,
 * distinctcount, distinctcounthll, distinctcountrawhll, segmentpartitioneddistinctcount, distinctcountsmarthll,
 * distinctcounthllplus, distinctcountrawhllplus, distinctcountUll, distinctcountsmartUll and the column has a
 * dictionary, or has column metadata with min and max value defined. It also supports count(*) if the query has
 * no filter.
 * We don't use this operator if the segment has star tree, as the dictionary will have aggregated values for the
 * metrics, and dimensions will have star node value.
 *
 * For min value, we use the first value from the dictionary, falling back to the column metadata min value if there
 * is no dictionary.
 * For max value we use the last value from dictionary, falling back to the column metadata max value if there
 * is no dictionary.
 */
@SuppressWarnings("rawtypes")
public class NonScanBasedAggregationOperator extends BaseOperator<AggregationResultsBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE_NO_SCAN";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final DataSource[] _dataSources;
  private final int _numTotalDocs;

  public NonScanBasedAggregationOperator(QueryContext queryContext, DataSource[] dataSources, int numTotalDocs) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _dataSources = dataSources;
    _numTotalDocs = numTotalDocs;
  }

  @Override
  protected AggregationResultsBlock getNextBlock() {
    List<Object> aggregationResults = new ArrayList<>(_aggregationFunctions.length);
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      // note that dataSource will be null for COUNT, sp do not interact with it until it's known this isn't a COUNT
      DataSource dataSource = _dataSources[i];
      Object result = AggregationFunctionUtils.getAggregationResult(aggregationFunction, dataSource,
          _numTotalDocs, EXPLAIN_NAME);
      aggregationResults.add(result);
    }

    // Build intermediate result block based on aggregation result from the executor.
    return new AggregationResultsBlock(_aggregationFunctions, aggregationResults, _queryContext);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
    return new ExecutionStatistics(_numTotalDocs, 0, 0, _numTotalDocs);
  }
}
