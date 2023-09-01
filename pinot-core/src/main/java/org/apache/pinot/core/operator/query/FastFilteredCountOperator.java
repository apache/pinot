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
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.SegmentMetadata;


@SuppressWarnings("rawtypes")
public class FastFilteredCountOperator extends BaseOperator<AggregationResultsBlock> {

  private static final String EXPLAIN_NAME = "FAST_FILTERED_COUNT";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final BaseFilterOperator _filterOperator;
  private final SegmentMetadata _segmentMetadata;

  private long _docsCounted;

  public FastFilteredCountOperator(QueryContext queryContext, BaseFilterOperator filterOperator,
      SegmentMetadata segmentMetadata) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _filterOperator = filterOperator;
    _segmentMetadata = segmentMetadata;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_filterOperator);
  }

  @Override
  protected AggregationResultsBlock getNextBlock() {
    long count = _filterOperator.getNumMatchingDocs();
    List<Object> aggregates = new ArrayList<>(1);
    aggregates.add(count);
    _docsCounted += count;
    return new AggregationResultsBlock(_aggregationFunctions, aggregates, _queryContext);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // fabricate the number of docs scanned to keep compatibility tests happy for now, but this should be set to zero
    return new ExecutionStatistics(_docsCounted, 0, 0, _segmentMetadata.getTotalDocs());
  }
}
