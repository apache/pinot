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
package org.apache.pinot.core.plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.TimeSeriesContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.timeseries.TimeSeriesAggregationOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;


public class TimeSeriesPlanNode implements PlanNode {
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final TimeSeriesContext _timeSeriesContext;
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _timeSeriesContext = Objects.requireNonNull(queryContext.getTimeSeriesContext(),
        "Missing time-series context in TimeSeriesPlanNode");
    _seriesBuilderFactory = TimeSeriesBuilderFactoryProvider.getSeriesBuilderFactory(_timeSeriesContext.getLanguage());
  }

  @Override
  public Operator<TimeSeriesResultsBlock> run() {
    FilterPlanNode filterPlanNode = new FilterPlanNode(_segmentContext, _queryContext);
    ProjectPlanNode projectPlanNode = new ProjectPlanNode(
        _segmentContext, _queryContext, getProjectPlanNodeExpressions(), DocIdSetPlanNode.MAX_DOC_PER_CALL,
        filterPlanNode.run());
    BaseProjectOperator<? extends ValueBlock> projectionOperator = projectPlanNode.run();
    return new TimeSeriesAggregationOperator(
        _timeSeriesContext.getTimeColumn(),
        _timeSeriesContext.getTimeUnit(),
        _timeSeriesContext.getOffsetSeconds(),
        _timeSeriesContext.getAggInfo(),
        _timeSeriesContext.getValueExpression(),
        getGroupByColumns(),
        _timeSeriesContext.getTimeBuckets(),
        projectionOperator,
        _seriesBuilderFactory,
        _segmentContext.getIndexSegment().getSegmentMetadata());
  }

  private List<ExpressionContext> getProjectPlanNodeExpressions() {
    List<ExpressionContext> result = new ArrayList<>(_queryContext.getSelectExpressions());
    if (CollectionUtils.isNotEmpty(_queryContext.getGroupByExpressions())) {
      result.addAll(_queryContext.getGroupByExpressions());
    }
    result.add(_queryContext.getTimeSeriesContext().getValueExpression());
    result.add(ExpressionContext.forIdentifier(_queryContext.getTimeSeriesContext().getTimeColumn()));
    return result;
  }

  private List<String> getGroupByColumns() {
    if (_queryContext.getGroupByExpressions() == null) {
      return new ArrayList<>();
    }
    List<String> groupByColumns = new ArrayList<>();
    for (ExpressionContext expression : _queryContext.getGroupByExpressions()) {
      Preconditions.checkState(expression.getType() == ExpressionContext.Type.IDENTIFIER);
      groupByColumns.add(expression.getIdentifier());
    }
    return groupByColumns;
  }
}
