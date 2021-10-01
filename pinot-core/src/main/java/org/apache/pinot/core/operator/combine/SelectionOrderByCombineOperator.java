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

import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for selection order-by queries.
 * <p>When the first order-by expression is an identifier (column), try to use
 * {@link org.apache.pinot.core.operator.combine.MinMaxValueBasedSelectionOrderByCombineOperator} first, which will
 * skip processing some segments based on the column min/max value. Otherwise fall back to the default combine
 * (process all segments).
 */
@SuppressWarnings("rawtypes")
public class SelectionOrderByCombineOperator extends BaseCombineOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionOrderByCombineOperator.class);
  private static final String OPERATOR_NAME = "SelectionOrderByCombineOperator";
  private static final String EXPLAIN_NAME = "COMBINE_SELECT_ORDERBY";

  private final int _numRowsToKeep;

  public SelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs, int maxExecutionThreads) {
    super(operators, queryContext, executorService, endTimeMs, maxExecutionThreads);
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String getExplainPlanName() {
    return EXPLAIN_NAME;
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
      try {
        return new MinMaxValueBasedSelectionOrderByCombineOperator(_operators, _queryContext, _executorService,
            _endTimeMs, _numTasks).getNextBlock();
      } catch (Exception e) {
        LOGGER.warn("Caught exception while using min/max value based combine, using the default combine", e);
      }
    }
    return super.getNextBlock();
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    DataSchema mergedDataSchema = mergedBlock.getDataSchema();
    DataSchema dataSchemaToMerge = blockToMerge.getDataSchema();
    assert mergedDataSchema != null && dataSchemaToMerge != null;
    if (!mergedDataSchema.equals(dataSchemaToMerge)) {
      String errorMessage =
          String.format("Data schema mismatch between merged block: %s and block to merge: %s, drop block to merge",
              mergedDataSchema, dataSchemaToMerge);
      // NOTE: This is segment level log, so log at debug level to prevent flooding the log.
      LOGGER.debug(errorMessage);
      mergedBlock.addToProcessingExceptions(
          QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, errorMessage));
      return;
    }

    PriorityQueue<Object[]> mergedRows = (PriorityQueue<Object[]>) mergedBlock.getSelectionResult();
    Collection<Object[]> rowsToMerge = blockToMerge.getSelectionResult();
    assert mergedRows != null && rowsToMerge != null;
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge, _numRowsToKeep);
  }
}
