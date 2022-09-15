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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>SelectionPlanNode</code> class provides the execution plan for selection query on a single segment.
 */
public class SelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public SelectionPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<SelectionResultsBlock> run() {
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(_queryContext, _indexSegment);
    int limit = _queryContext.getLimit();

    if (limit > 0) {
      List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
      if (orderByExpressions == null) {
        // Selection only
        TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext, expressions,
            Math.min(limit, DocIdSetPlanNode.MAX_DOC_PER_CALL)).run();
        return new SelectionOnlyOperator(_indexSegment, _queryContext, expressions, transformOperator);
      } else {
        // Selection order-by
        if (isAllOrderByColumnsSorted(orderByExpressions)) {
          // All order-by columns are sorted, no need to sort the records
          TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext, expressions,
              Math.min(limit + _queryContext.getOffset(), DocIdSetPlanNode.MAX_DOC_PER_CALL)).run();
          return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, transformOperator, true);
        } else if (orderByExpressions.size() == expressions.size()) {
          // All output expressions are ordered
          TransformOperator transformOperator =
              new TransformPlanNode(_indexSegment, _queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
          return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, transformOperator, false);
        } else {
          // Not all output expressions are ordered, only fetch the order-by expressions and docId to avoid the
          // unnecessary data fetch
          List<ExpressionContext> expressionsToTransform = new ArrayList<>(orderByExpressions.size());
          for (OrderByExpressionContext orderByExpression : orderByExpressions) {
            expressionsToTransform.add(orderByExpression.getExpression());
          }
          TransformOperator transformOperator =
              new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform,
                  DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
          return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, transformOperator, false);
        }
      }
    } else {
      // Empty selection (LIMIT 0)
      TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext, expressions, 0).run();
      return new EmptySelectionOperator(_indexSegment, expressions, transformOperator);
    }
  }

  /**
   *  This function checks whether all columns in order by clause are pre-sorted.
   *  This is used to optimize order by limit clauses.
   *  For eg:
   *  A query like "select * from table order by col1, col2 limit 10"
   *  will take all the n matching rows and add it to a priority queue of size 10.
   *  This is nlogk operation which can be quite expensive for a large n.
   *  In the above example, if the docs in the segment are already sorted by col1 and col2 then there is no need for
   *  sorting at all (only limit is needed).
   * @return true is all columns in order by clause are sorted . False otherwise
   */
  private boolean isAllOrderByColumnsSorted(List<OrderByExpressionContext> orderByExpressions) {
    for (OrderByExpressionContext orderByExpression : orderByExpressions) {
      if (!(orderByExpression.getExpression().getType() == ExpressionContext.Type.IDENTIFIER)
          || !orderByExpression.isAsc()) {
        return false;
      }
      String column = orderByExpression.getExpression().getIdentifier();
      if (!_indexSegment.getDataSource(column).getDataSourceMetadata().isSorted()) {
        return false;
      }
    }
    return true;
  }
}
