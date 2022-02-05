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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>PreAggGapFillSelectionPlanNode</code> class provides the execution
 * plan for pre-aggregate gapfill query on a single segment.
 */
public class GapfillSelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public GapfillSelectionPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext.getSubQueryContext();
  }

  @Override
  public Operator<IntermediateResultsBlock> run() {
    int limit = _queryContext.getLimit();

    Preconditions.checkArgument(
        _queryContext.getOrderByExpressions() == null,
        "Subquery of Gapfill query should not have orderby expression.");

    ExpressionContext gapFillSelection = GapfillUtils.getPreAggregateGapfillExpressionContext(_queryContext);
    Preconditions.checkArgument(gapFillSelection != null, "PreAggregate Gapfill Expression is expected.");

    ExpressionContext timeSeriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    Preconditions.checkArgument(timeSeriesOn != null, "TimeSeriesOn Expression is expected.");

    List<ExpressionContext> expressions = GapfillUtils.getGroupByExpressions(_queryContext);
    Preconditions.checkArgument(expressions != null, "GroupByExpressions should not be null.");
    Set<ExpressionContext> expressionContextSet = new HashSet<>(expressions);

    List<ExpressionContext> selectionExpressions
        = SelectionOperatorUtils.extractExpressions(_queryContext, _indexSegment);
    for (ExpressionContext expressionContext : selectionExpressions) {
      if (!GapfillUtils.isGapfill(expressionContext) && !expressionContextSet.contains(expressionContext)) {
        expressions.add(expressionContext);
        expressionContextSet.add(expressionContext);
      }
    }

    TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext, expressions,
        Math.min(limit, DocIdSetPlanNode.MAX_DOC_PER_CALL)).run();
    return new SelectionOnlyOperator(_indexSegment, _queryContext, expressions, transformOperator);
  }
}
