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
package org.apache.pinot.core.operator.combine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * Combine operator for selection order-by queries.
 * <p>When the first order-by expression is an identifier (column), try to use
 * {@link org.apache.pinot.core.operator.combine.MinMaxValueBasedSelectionOrderByCombineOperator} first, which will
 * skip processing some segments based on the column min/max value. Otherwise fall back to the default combine
 * (process all segments).
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SelectionOrderByCombineOperator extends BaseCombineOperator {
  private static final String OPERATOR_NAME = "SelectionOrderByCombineOperator";

  private final List<Operator> _operators;
  private final QueryContext _queryContext;
  private final ExecutorService _executorService;
  private final long _endTimeMs;
  private final int _numRowsToKeep;

  public SelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs) {
    super(operators, queryContext, executorService, endTimeMs);
    _operators = operators;
    _queryContext = queryContext;
    _executorService = executorService;
    _endTimeMs = endTimeMs;
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  /**
   * {@inheritDoc}
   *
   * <p> Execute query on one or more segments in a single thread, and store multiple intermediate result blocks
   * into BlockingQueue. Try to use
   * {@link org.apache.pinot.core.operator.combine.MinMaxValueBasedSelectionOrderByCombineOperator} first, which
   * will skip processing some segments based on the column min/max value. Otherwise fall back to the default combine
   * (process all segments).
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    if (orderByExpressions.get(0).getExpression().getType() == ExpressionContext.Type.IDENTIFIER) {
      int numOrderByExpressions = orderByExpressions.size();
      assert numOrderByExpressions > 0;
      OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
      assert firstOrderByExpression.getExpression().getType() == ExpressionContext.Type.IDENTIFIER;
      String firstOrderByColumn = firstOrderByExpression.getExpression().getIdentifier();
      boolean asc = firstOrderByExpression.isAsc();

      int numOperators = _operators.size();
      List<MinMaxValueContext> minMaxValueContexts = new ArrayList<>(numOperators);
      for (Operator operator : _operators) {
        minMaxValueContexts.add(new MinMaxValueContext((SelectionOrderByOperator) operator, firstOrderByColumn));
      }
      try {
        if (asc) {
          // For ascending order, sort on column min value in ascending order
          minMaxValueContexts.sort((o1, o2) -> {
            // Put segments without column min value in the front because we always need to process them
            if (o1._minValue == null) {
              return o2._minValue == null ? 0 : -1;
            }
            if (o2._minValue == null) {
              return 1;
            }
            return o1._minValue.compareTo(o2._minValue);
          });
        } else {
          // For descending order, sort on column max value in descending order
          minMaxValueContexts.sort((o1, o2) -> {
            // Put segments without column max value in the front because we always need to process them
            if (o1._maxValue == null) {
              return o2._maxValue == null ? 0 : -1;
            }
            if (o2._maxValue == null) {
              return 1;
            }
            return o2._maxValue.compareTo(o1._maxValue);
          });
        }
      } catch (Exception e) {
        // Fall back to the default combine (process all segments) when segments have different data types for the first
        // order-by column
        LOGGER.warn("Segments have different data types for the first order-by column: {}, using the default combine",
            firstOrderByColumn);
        return super.getNextBlock();
      }

      return new MinMaxValueBasedSelectionOrderByCombineOperator(_operators, _queryContext, _executorService,
          _endTimeMs, minMaxValueContexts).getNextBlock();
    } else {
      return super.getNextBlock();
    }
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    DataSchema mergedDataSchema = mergedBlock.getDataSchema();
    DataSchema dataSchemaToMerge = blockToMerge.getDataSchema();
    assert mergedDataSchema != null && dataSchemaToMerge != null;
    if (!mergedDataSchema.equals(dataSchemaToMerge)) {
      String errorMessage = String
          .format("Data schema mismatch between merged block: %s and block to merge: %s, drop block to merge",
              mergedDataSchema, dataSchemaToMerge);
      // NOTE: This is segment level log, so log at debug level to prevent flooding the log.
      LOGGER.debug(errorMessage);
      mergedBlock
          .addToProcessingExceptions(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, errorMessage));
      return;
    }

    PriorityQueue<Object[]> mergedRows = (PriorityQueue<Object[]>) mergedBlock.getSelectionResult();
    Collection<Object[]> rowsToMerge = blockToMerge.getSelectionResult();
    assert mergedRows != null && rowsToMerge != null;
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge, _numRowsToKeep);
  }
}
