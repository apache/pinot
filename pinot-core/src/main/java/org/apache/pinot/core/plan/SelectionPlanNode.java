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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByAscOperator;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByDescOperation;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


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

    if (limit == 0) {
      // Empty selection (LIMIT 0)
      BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_indexSegment, _queryContext, expressions, 0).run();
      return new EmptySelectionOperator(_indexSegment, expressions, projectOperator);
    }

    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    if (orderByExpressions == null) {
      // Selection only
      // ie: SELECT ... FROM Table WHERE ... LIMIT 10
      int maxDocsPerCall = Math.min(limit, DocIdSetPlanNode.MAX_DOC_PER_CALL);
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(_indexSegment, _queryContext, expressions, maxDocsPerCall).run();
      return new SelectionOnlyOperator(_indexSegment, _queryContext, expressions, projectOperator);
    }
    int numOrderByExpressions = orderByExpressions.size();
    // Although it is a break of abstraction, some code, specially merging, assumes that if there is an order by
    // expression the operator will return a block whose selection result is a priority queue.
    int sortedColumnsPrefixSize = getSortedColumnsPrefix(orderByExpressions, _queryContext.isNullHandlingEnabled());
    OrderByAlgorithm orderByAlgorithm = OrderByAlgorithm.fromQueryContext(_queryContext);
    if (sortedColumnsPrefixSize > 0 && orderByAlgorithm != OrderByAlgorithm.NAIVE) {
      int maxDocsPerCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;
      // The first order by expressions are sorted (either asc or desc).
      // ie: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column DESC LIMIT 10 OFFSET 5
      // or: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column, not_sorted LIMIT 10 OFFSET 5
      // but not SELECT ... FROM Table WHERE predicates ORDER BY not_sorted, sorted_column LIMIT 10 OFFSET 5
      if (orderByExpressions.get(0).isAsc()) {
        if (sortedColumnsPrefixSize == orderByExpressions.size()) {
          maxDocsPerCall = Math.min(limit + _queryContext.getOffset(), DocIdSetPlanNode.MAX_DOC_PER_CALL);
        }
        BaseProjectOperator<?> projectOperator =
            new ProjectPlanNode(_indexSegment, _queryContext, expressions, maxDocsPerCall).run();
        return new SelectionPartiallyOrderedByAscOperator(_indexSegment, _queryContext, expressions, projectOperator,
            sortedColumnsPrefixSize);
      } else {
        BaseProjectOperator<?> projectOperator =
            new ProjectPlanNode(_indexSegment, _queryContext, expressions, maxDocsPerCall).run();
        return new SelectionPartiallyOrderedByDescOperation(_indexSegment, _queryContext, expressions, projectOperator,
            sortedColumnsPrefixSize);
      }
    }
    if (numOrderByExpressions == expressions.size()) {
      // All output expressions are ordered
      // ie: SELECT not_sorted1, not_sorted2 FROM Table WHERE ... ORDER BY not_sorted1, not_sorted2 LIMIT 10 OFFSET 5
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(_indexSegment, _queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
      return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, projectOperator);
    }
    // Not all output expressions are ordered, only fetch the order-by expressions and docId to avoid the
    // unnecessary data fetch
    // ie: SELECT ... FROM Table WHERE ... ORDER BY not_sorted1, not_sorted2 LIMIT 10
    List<ExpressionContext> expressionsToTransform = new ArrayList<>(numOrderByExpressions);
    for (OrderByExpressionContext orderByExpression : orderByExpressions) {
      expressionsToTransform.add(orderByExpression.getExpression());
    }
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_indexSegment, _queryContext, expressionsToTransform,
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, projectOperator);
  }

  /**
   * This functions returns the number of expressions that are sorted by the implicit order in the index.
   *
   * This means that query that uses these expressions in its order by doesn't actually need to sort from 0 to the
   * given number (excluded) of expressions, as they are returned in the correct order.
   *
   * This method supports ASC and DESC order and ensures that all prefix expressions follow the same order. For example,
   * ORDER BY sorted_col1 ASC, sorted_col2 ASC and ORDER BY sorted_col1 DESC, sorted_col2 DESC will return 2 but
   * ORDER BY sorted_col1 DESC, sorted_col2 ASC and ORDER BY sorted_col1 ASC, sorted_col2 DESC will return 1 while
   * ORDER BY not_sorted, sorted_col1 will return 0 because the first column is not sorted.
   *
   * It doesn't make sense to add literal expressions in an order by expression, but if they are included, they are
   * considered sorted and its ASC/DESC is ignored.
   *
   * @return the max number that guarantees that from the first expression to the returned number, the index is already
   * sorted.
   */
  private int getSortedColumnsPrefix(List<OrderByExpressionContext> orderByExpressions, boolean isNullHandlingEnabled) {
    boolean asc = orderByExpressions.get(0).isAsc();
    for (int i = 0; i < orderByExpressions.size(); i++) {
      if (!isSorted(orderByExpressions.get(i), asc, isNullHandlingEnabled)) {
        return i;
      }
    }
    // If we reach here, all are sorted
    return orderByExpressions.size();
  }

  private boolean isSorted(OrderByExpressionContext orderByExpression, boolean asc, boolean isNullHandlingEnabled) {
    switch (orderByExpression.getExpression().getType()) {
      case LITERAL: {
        return true;
      }
      case IDENTIFIER: {
        if (!orderByExpression.isAsc() == asc) {
          return false;
        }
        String column = orderByExpression.getExpression().getIdentifierName();
        DataSource dataSource = _indexSegment.getDataSource(column);
        // If there are null values, we cannot trust DataSourceMetadata.isSorted
        if (isNullHandlingEnabled) {
          NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
          if (nullValueVector != null && !nullValueVector.getNullBitmap().isEmpty()) {
            return false;
          }
        }
        return dataSource.getDataSourceMetadata().isSorted();
      }
      case FUNCTION: // we could optimize monotonically increasing functions
      default: {
        return false;
      }
    }
  }

  public enum OrderByAlgorithm {
    NAIVE;

    @Nullable
    public static OrderByAlgorithm fromQueryContext(QueryContext queryContext) {
      String orderByAlgorithm = QueryOptionsUtils.getOrderByAlgorithm(queryContext.getQueryOptions());
      if (orderByAlgorithm == null) {
        return null;
      }
      return OrderByAlgorithm.valueOf(orderByAlgorithm.toUpperCase());
    }
  }
}
