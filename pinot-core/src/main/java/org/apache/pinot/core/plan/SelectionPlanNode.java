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
import org.apache.pinot.core.operator.query.StreamingSelectionOrderByOperator;
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
    // Although it is a break of abstraction, some code, specially merging, assumes that if there is an order by
    // expression the operator will return a block whose selection result is a priority queue.
    int sortedColumnsPrefixSize = getSortedColumnsPrefix(orderByExpressions);
    if (sortedColumnsPrefixSize > 0) {
      int maxDocsPerCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;
      // The first order by expressions are sorted (either asc or desc).
      // ie: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column DESC LIMIT 10 OFFSET 5
      // or: SELECT ... FROM Table WHERE predicates ORDER BY sorted_column, not_sorted LIMIT 10 OFFSET 5
      // but not SELECT ... FROM Table WHERE predicates ORDER BY not_sorted, sorted_column LIMIT 10 OFFSET 5

      if (sortedColumnsPrefixSize == orderByExpressions.size()) {
        maxDocsPerCall = Math.min(limit + _queryContext.getOffset(), DocIdSetPlanNode.MAX_DOC_PER_CALL);
      }

      boolean asc = orderByExpressions.get(0).isAsc();
      // Remember that we cannot use asc == projectOperator.isAscending() because empty operators are considered
      // both ascending and descending
      DocIdOrderedOperator.DocIdOrder queryOrder = DocIdOrderedOperator.DocIdOrder.fromAsc(asc);

      // Opt-in streaming path: emit one globally-sorted block at a time so a downstream k-way-merge combine can pull
      // lazily. Reaching here means the leading order-by is already a sorted prefix, so it is either a LITERAL or a
      // physically sorted IDENTIFIER; only the identifier case has a forward index to scan in order. The combine-side
      // gate deliberately does not repeat this check -- it accepts materialized children too, which is how
      // SortedSelectionMergeMode.ON forces the path for an expression order-by.
      //
      // The DESC-incompatible sorted case still falls back to the materialized SelectionPartiallyOrderedByDescOperation
      // below so global order stays correct.

      // Set when the streaming attempt below builds a project the materialized fallback can take over.
      BaseProjectOperator<?> reusableSortedByProject = null;
      if (_queryContext.isSortedSelectionMergeEnabled()
          && orderByExpressions.get(0).getExpression().getType() == ExpressionContext.Type.IDENTIFIER) {
        // When there are non-order-by output expressions, only fetch the order-by expressions during the forward scan
        // (the streaming operator fetches the rest in a second pass); otherwise fetch all expressions.
        boolean projectsAllExpressions = expressions.size() <= numOrderByExpressions;
        List<ExpressionContext> projectExpressions = expressions;
        if (!projectsAllExpressions) {
          projectExpressions = new ArrayList<>(numOrderByExpressions);
          for (OrderByExpressionContext orderByExpression : orderByExpressions) {
            projectExpressions.add(orderByExpression.getExpression());
          }
        }
        BaseProjectOperator<?> streamingProjectOperator =
            getSortedByProject(projectExpressions, maxDocsPerCall, orderByExpressions);
        if (streamingProjectOperator.isCompatibleWith(queryOrder)) {
          return new StreamingSelectionOrderByOperator(_indexSegment, _queryContext, expressions,
              streamingProjectOperator, sortedColumnsPrefixSize);
        }
        // The project cannot scan in the query's direction, which only a DESC query can hit. Fall through to the
        // materialized fallback, reusing this project when it already covers the full expression list: that is what
        // the fallback would rebuild, and it was never driven. A narrower one is dropped unclosed, per the standing
        // TODO on ProjectPlanNode#run.
        if (projectsAllExpressions) {
          reusableSortedByProject = streamingProjectOperator;
        }
      }

      BaseProjectOperator<?> projectOperator = reusableSortedByProject != null
          ? reusableSortedByProject
          : getSortedByProject(expressions, maxDocsPerCall, orderByExpressions);
      if (projectOperator.isCompatibleWith(queryOrder)) {
        return new SelectionPartiallyOrderedByLinearOperator(_indexSegment, _queryContext, expressions, projectOperator,
            sortedColumnsPrefixSize);
      } else {
        return new SelectionPartiallyOrderedByDescOperation(_indexSegment, _queryContext, expressions, projectOperator,
            sortedColumnsPrefixSize);
      }
    }
    if (numOrderByExpressions == expressions.size()) {
      // All output expressions are ordered
      // ie: SELECT not_sorted1, not_sorted2 FROM Table WHERE ... ORDER BY not_sorted1, not_sorted2 LIMIT 10 OFFSET 5
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(_segmentContext, _queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
      return new SelectionOrderByOperator(_indexSegment, _queryContext, expressions, projectOperator);
    }
    // Not all output expressions are ordered, only fetch the order-by expressions and docId to avoid the
    // unnecessary data fetch
    // ie: SELECT ... FROM Table WHERE ... ORDER BY not_sorted1, not_sorted2 LIMIT 10
    List<ExpressionContext> expressionsToTransform = new ArrayList<>(numOrderByExpressions);
    for (OrderByExpressionContext orderByExpression : orderByExpressions) {
      expressionsToTransform.add(orderByExpression.getExpression());
    }
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_segmentContext, _queryContext, expressionsToTransform,
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
        // This happens when the operator cannot provide the required order between blocks
        // Fallback to SelectionOrderByOperator
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
  /// It doesn't make sense to add literal expressions in an order by expression, but if they are included, they are
  /// considered sorted and its ASC/DESC is ignored.
  ///
  /// @return the max number that guarantees that from the first expression to the returned number, the index is already
  /// sorted.
  private int getSortedColumnsPrefix(List<OrderByExpressionContext> orderByExpressions) {
    boolean asc = orderByExpressions.get(0).isAsc();
    for (int i = 0; i < orderByExpressions.size(); i++) {
      if (!isSorted(orderByExpressions.get(i), asc)) {
        return i;
      }
    }
    // If we reach here, all are sorted
    return orderByExpressions.size();
  }

  private boolean isSorted(OrderByExpressionContext orderByExpression, boolean asc) {
    switch (orderByExpression.getExpression().getType()) {
      case LITERAL: {
        return true;
      }
      case IDENTIFIER: {
        if (!orderByExpression.isAsc() == asc) {
          return false;
        }
        return isColumnSorted(_indexSegment, _queryContext, orderByExpression.getExpression().getIdentifier());
      }
      case FUNCTION: // we could optimize monotonically increasing functions
      default: {
        return false;
      }
    }
  }

  /// Returns whether `column` can be relied on to be sorted (ascending) for this query in `segment`.
  ///
  /// This is [#isColumnPhysicallySorted] plus the null caveat: once null handling is on, the physical order is not
  /// the order the query asks for, so a column carrying nulls is not usable as sorted. Reading the null bitmap
  /// touches the segment's mapped buffer, so this must only be called once the segment has been acquired -- which is
  /// why [AcquireReleaseColumnsSegmentPlanNode] defers the whole plan build until after `acquire()`.
  public static boolean isColumnSorted(IndexSegment segment, QueryContext queryContext, String column) {
    DataSource dataSource = segment.getDataSource(column, queryContext.getSchema());
    // If there are null values, we cannot trust DataSourceMetadata.isSorted
    if (queryContext.isNullHandlingEnabled()) {
      NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
      if (nullValueVector != null && !nullValueVector.getNullBitmap().isEmpty()) {
        return false;
      }
    }
    return isColumnPhysicallySorted(segment, queryContext, column);
  }

  /// Returns whether `column` is physically sorted (ascending) in `segment`, ignoring nulls.
  ///
  /// Reads [org.apache.pinot.segment.spi.datasource.DataSourceMetadata] only, so it touches no column buffer and
  /// needs no segment acquire. That is what lets
  /// [org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2] resolve
  /// [org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode#AUTO] before any plan node is built.
  public static boolean isColumnPhysicallySorted(IndexSegment segment, QueryContext queryContext, String column) {
    return segment.getDataSource(column, queryContext.getSchema()).getDataSourceMetadata().isSorted();
  }
}
