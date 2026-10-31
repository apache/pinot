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

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.DocIdOrderedOperator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByDescOperation;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByLinearOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


/// The `SelectionPlanNode` class provides the execution plan for selection query on a single segment.
public class SelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;

  public SelectionPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
  }

  @Override
  public Operator<SelectionResultsBlock> run() {
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(_queryContext, _indexSegment);
    int limit = _queryContext.getLimit();

    if (limit == 0) {
      // Empty selection (LIMIT 0)
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(_segmentContext, _queryContext, expressions, 0).run();
      return new EmptySelectionOperator(_indexSegment, _queryContext, expressions, projectOperator);
    }

    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    if (orderByExpressions == null) {
      // Selection only
      // ie: SELECT ... FROM Table WHERE ... LIMIT 10
      int maxDocsPerCall = Math.min(limit, DocIdSetPlanNode.MAX_DOC_PER_CALL);
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(_segmentContext, _queryContext, expressions, maxDocsPerCall).run();
      return new SelectionOnlyOperator(_indexSegment, _queryContext, expressions, projectOperator);
    }
    int numOrderByExpressions = orderByExpressions.size();
    // Order-by expressions come first; any remaining output expressions are fetched after selection.
    List<ExpressionContext> expressionsToProject = expressions.subList(0, numOrderByExpressions);
    int sortedColumnsPrefixSize = getSortedColumnsPrefix(orderByExpressions, _queryContext.isNullHandlingEnabled());
    if (sortedColumnsPrefixSize > 0) {
      int maxDocsPerCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;
      // The first order by expressions are sorted (either asc or desc).
      // ie: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column DESC LIMIT 10 OFFSET 5
      // or: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column, not_sorted LIMIT 10 OFFSET 5
      // but not SELECT ... FROM Table WHERE predicates ORDER BY not_sorted, sorted_column LIMIT 10 OFFSET 5

      if (sortedColumnsPrefixSize == numOrderByExpressions) {
        maxDocsPerCall = Math.min(limit + _queryContext.getOffset(), DocIdSetPlanNode.MAX_DOC_PER_CALL);
      }

      BaseProjectOperator<?> projectOperator =
          getSortedByProject(expressionsToProject, maxDocsPerCall, orderByExpressions);
      // Empty operators support both directions, so check compatibility with the query order.
      DocIdOrderedOperator.DocIdOrder queryOrder =
          DocIdOrderedOperator.DocIdOrder.fromAsc(orderByExpressions.get(0).isAsc());
      if (projectOperator.isCompatibleWith(queryOrder)) {
        return new SelectionPartiallyOrderedByLinearOperator(_indexSegment, _queryContext, expressions, projectOperator,
            sortedColumnsPrefixSize);
      } else {
        return new SelectionPartiallyOrderedByDescOperation(_indexSegment, _queryContext, expressions, projectOperator,
            sortedColumnsPrefixSize);
      }
    }
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_segmentContext, _queryContext, expressionsToProject,
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
    return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, projectOperator);
  }

  private BaseProjectOperator<?> getSortedByProject(List<ExpressionContext> expressions, int maxDocsPerCall,
      List<OrderByExpressionContext> orderByExpressions) {
    BaseProjectOperator<?> projectOperator =
        new ProjectPlanNode(_segmentContext, _queryContext, expressions, maxDocsPerCall).run();

    boolean asc = orderByExpressions.get(0).isAsc();
    if (!asc
        && QueryOptionsUtils.isReverseOrderAllowed(_queryContext.getQueryOptions())
        && !projectOperator.isCompatibleWith(DocIdOrderedOperator.DocIdOrder.DESC)) {
      try {
        return projectOperator.withOrder(DocIdOrderedOperator.DocIdOrder.DESC);
      } catch (IllegalArgumentException | UnsupportedOperationException e) {
        // Keep the ascending project operator for SelectionPartiallyOrderedByDescOperation.
        return projectOperator;
      }
    }
    return projectOperator;
  }

  /// This functions returns the number of expressions that are sorted by the implicit order in the index.
  ///
  /// This means that query that uses these expressions in its order by doesn't actually need to sort from 0 to the
  /// given number (excluded) of expressions, as they are returned in the correct order.
  ///
  /// This method supports ASC and DESC order and ensures that all prefix expressions follow the same order. For
  /// example, ORDER BY sorted_col1 ASC, sorted_col2 ASC and ORDER BY sorted_col1 DESC, sorted_col2 DESC will return 2
  /// but ORDER BY sorted_col1 DESC, sorted_col2 ASC and ORDER BY sorted_col1 ASC, sorted_col2 DESC will return 1 while
  /// ORDER BY not_sorted, sorted_col1 will return 0 because the first column is not sorted.
  ///
  /// Literal and function expressions cannot establish a sorted prefix because they are not backed by any data source
  /// column, so ORDER BY <literal>, ... or ORDER BY <function>, ... returns 0 even if followed by sorted identifiers.
  /// Once a sorted identifier has established the prefix, trailing literals are effectively constant and do not disrupt
  /// ordering, so they extend the prefix (e.g. ORDER BY sorted_col, 'x' returns 2).
  ///
  /// @return the max number that guarantees that from the first expression to the returned number, the index is already
  /// sorted.
  private int getSortedColumnsPrefix(List<OrderByExpressionContext> orderByExpressions, boolean isNullHandlingEnabled) {
    // LITERAL and FUNCTION expressions cannot establish a sorted prefix: they are not backed by a data source column,
    // so the segment's natural sort order cannot be relied on for them.  Only IDENTIFIER expressions can start the
    // prefix.  Once established, trailing LITERAL expressions extend the prefix because their constant value does not
    // disrupt ordering, but trailing FUNCTION expressions still terminate it (we don't know whether the function is
    // monotonic over the sorted column).
    OrderByExpressionContext firstExpr = orderByExpressions.get(0);
    switch (firstExpr.getExpression().getType()) {
      case LITERAL:
      case FUNCTION:
        return 0;
      default:
        // fall through
    }
    boolean asc = firstExpr.isAsc();
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
        // LITERAL expressions are effectively constant — their value does not change across rows, so they cannot
        // disrupt an established sort order once a sorted identifier has led the prefix.  A lone LITERAL (or one
        // appearing before any sorted identifier) is rejected by getSortedColumnsPrefix returning 0, so returning true
        // here is safe and preserves trailing-literal optimization without triggering the crash from issue #19701.
        return true;
      }
      case IDENTIFIER: {
        if (!orderByExpression.isAsc() == asc) {
          return false;
        }
        String column = orderByExpression.getExpression().getIdentifier();
        DataSource dataSource = _indexSegment.getDataSource(column, _queryContext.getSchema());
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
}
