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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Optimized combine operator for selection order-by queries.
 * <p>When the first order-by expression is an identifier (column), skip processing the segments if possible based on
 * the column min/max value and keep enough documents to fulfill the LIMIT and OFFSET requirement.
 * <ul>
 *   <li>1. Sort all the segments by the column min/max value</li>
 *   <li>2. Keep processing segments until we get enough documents to fulfill the LIMIT and OFFSET requirement</li>
 *   <li>3. Skip processing the segments that cannot add values to the final result</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MinMaxValueBasedSelectionOrderByCombineOperator
    extends BaseSingleBlockCombineOperator<SelectionResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinMaxValueBasedSelectionOrderByCombineOperator.class);

  private static final String EXPLAIN_NAME = "COMBINE_SELECT_ORDERBY_MINMAX";

  // For min/max value based combine, when a thread detects that no more segment needs to be processed, it inserts this
  // special results block, which can be skipped during the merge phase
  private static final BaseResultsBlock EMPTY_RESULTS_BLOCK = new MetadataResultsBlock();

  // Use an AtomicInteger to track the end operator id, beyond which no operator needs to be processed
  private final AtomicInteger _endOperatorId;
  private final int _numRowsToKeep;
  private final List<MinMaxValueContext> _minMaxValueContexts;
  private final AtomicReference<Comparable> _globalBoundaryValue = new AtomicReference<>();

  public MinMaxValueBasedSelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(null, operators, queryContext, executorService);
    _endOperatorId = new AtomicInteger(_numOperators);
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();

    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    int numOrderByExpressions = orderByExpressions.size();
    assert numOrderByExpressions > 0;
    OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
    assert firstOrderByExpression.getExpression().getType() == ExpressionContext.Type.IDENTIFIER;
    String firstOrderByColumn = firstOrderByExpression.getExpression().getIdentifier();

    _minMaxValueContexts = new ArrayList<>(_numOperators);
    for (Operator<BaseResultsBlock> operator : _operators) {
      _minMaxValueContexts.add(new MinMaxValueContext(operator, firstOrderByColumn));
    }
    if (firstOrderByExpression.isAsc()) {
      // For ascending order, sort on column min value in ascending order
      _minMaxValueContexts.sort((o1, o2) -> {
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
      _minMaxValueContexts.sort((o1, o2) -> {
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
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /**
   * {@inheritDoc}
   *
   * <p> Execute query on one or more segments in a single thread, and store multiple intermediate result blocks
   * into BlockingQueue, skip processing the segments if possible based on the column min/max value and keep enough
   * documents to fulfill the LIMIT and OFFSET requirement.
   */
  @Override
  protected void processSegments() {
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    int numOrderByExpressions = orderByExpressions.size();
    assert numOrderByExpressions > 0;
    OrderByExpressionContext firstOrderByExpression = orderByExpressions.get(0);
    assert firstOrderByExpression.getExpression().getType() == ExpressionContext.Type.IDENTIFIER;
    boolean asc = firstOrderByExpression.isAsc();

    // Keep a boundary value for the thread
    // NOTE: The thread boundary value can be different from the global boundary value because thread boundary
    //       value is updated after processing the segment, while global boundary value is updated after the
    //       segment result is merged.
    Comparable threadBoundaryValue = null;

    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      if (operatorId >= _endOperatorId.get()) {
        _blockingQueue.offer(EMPTY_RESULTS_BLOCK);
        continue;
      }

      // Calculate the boundary value from global boundary and thread boundary
      Comparable boundaryValue = _globalBoundaryValue.get();
      if (boundaryValue == null) {
        boundaryValue = threadBoundaryValue;
      } else {
        if (threadBoundaryValue != null) {
          if (asc) {
            if (threadBoundaryValue.compareTo(boundaryValue) < 0) {
              boundaryValue = threadBoundaryValue;
            }
          } else {
            if (threadBoundaryValue.compareTo(boundaryValue) > 0) {
              boundaryValue = threadBoundaryValue;
            }
          }
        }
      }

      // Check if the segment can be skipped
      MinMaxValueContext minMaxValueContext = _minMaxValueContexts.get(operatorId);
      if (boundaryValue != null) {
        if (asc) {
          // For ascending order, no need to process more segments if the column min value is larger than the
          // boundary value, or is equal to the boundary value and the there is only one order-by expression
          if (minMaxValueContext._minValue != null) {
            int result = minMaxValueContext._minValue.compareTo(boundaryValue);
            if (result > 0 || (result == 0 && numOrderByExpressions == 1)) {
              _endOperatorId.set(operatorId);
              _blockingQueue.offer(EMPTY_RESULTS_BLOCK);
              continue;
            }
          }
        } else {
          // For descending order, no need to process more segments if the column max value is smaller than the
          // boundary value, or is equal to the boundary value and the there is only one order-by expression
          if (minMaxValueContext._maxValue != null) {
            int result = minMaxValueContext._maxValue.compareTo(boundaryValue);
            if (result < 0 || (result == 0 && numOrderByExpressions == 1)) {
              _endOperatorId.set(operatorId);
              _blockingQueue.offer(EMPTY_RESULTS_BLOCK);
              continue;
            }
          }
        }
      }

      // Process the segment
      Operator operator = minMaxValueContext._operator;
      SelectionResultsBlock resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        resultsBlock = (SelectionResultsBlock) operator.nextBlock();
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
      Collection<Object[]> rows = resultsBlock.getRows();
      if (rows != null && rows.size() >= _numRowsToKeep) {
        // Segment result has enough rows, update the boundary value

        Comparable segmentBoundaryValue;
        if (rows instanceof PriorityQueue) {
          // Results from SelectionOrderByOperator
          assert ((PriorityQueue<Object[]>) rows).peek() != null;
          segmentBoundaryValue = (Comparable) ((PriorityQueue<Object[]>) rows).peek()[0];
        } else {
          // Results from LinearSelectionOrderByOperator
          assert rows instanceof List;
          segmentBoundaryValue = (Comparable) ((List<Object[]>) rows).get(rows.size() - 1)[0];
        }

        if (boundaryValue == null) {
          boundaryValue = segmentBoundaryValue;
        } else {
          if (asc) {
            if (segmentBoundaryValue.compareTo(boundaryValue) < 0) {
              boundaryValue = segmentBoundaryValue;
            }
          } else {
            if (segmentBoundaryValue.compareTo(boundaryValue) > 0) {
              boundaryValue = segmentBoundaryValue;
            }
          }
        }
      }
      threadBoundaryValue = boundaryValue;
      _blockingQueue.offer(resultsBlock);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines intermediate selection result blocks from underlying operators and returns a merged one.
   * <ul>
   *   <li>
   *     Merges multiple intermediate selection result blocks as a merged one.
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  protected BaseResultsBlock mergeResults()
      throws Exception {
    SelectionResultsBlock mergedBlock = null;
    int numBlocksMerged = 0;
    long endTimeMs = _queryContext.getEndTimeMs();
    while (numBlocksMerged < _numOperators) {
      BaseResultsBlock blockToMerge =
          _blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      if (blockToMerge == null) {
        // Query times out, skip merging the remaining results blocks
        LOGGER.error("Timed out while polling results block, numBlocksMerged: {} (query: {})", numBlocksMerged,
            _queryContext);
        return new ExceptionResultsBlock(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR,
            new TimeoutException("Timed out while polling results block")));
      }
      if (blockToMerge == EMPTY_RESULTS_BLOCK) {
        numBlocksMerged++;
        continue;
      }
      if (blockToMerge.getProcessingExceptions() != null) {
        // Caught exception while processing segment, skip merging the remaining results blocks and directly return
        // the exception
        return blockToMerge;
      }
      if (mergedBlock == null) {
        mergedBlock = (SelectionResultsBlock) blockToMerge;
      } else {
        mergeResultsBlocks(mergedBlock, (SelectionResultsBlock) blockToMerge);
      }
      numBlocksMerged++;

      // Update the boundary value if enough rows are collected
      List<Object[]> rows = mergedBlock.getRows();
      if (rows.size() == _numRowsToKeep) {
        _globalBoundaryValue.set((Comparable) rows.get(_numRowsToKeep - 1)[0]);
      }
    }
    return mergedBlock;
  }

  protected void mergeResultsBlocks(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge) {
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
    SelectionOperatorUtils.mergeWithOrdering(mergedBlock, blockToMerge, _numRowsToKeep);
  }

  private static class MinMaxValueContext {
    final Operator<BaseResultsBlock> _operator;
    final Comparable _minValue;
    final Comparable _maxValue;

    MinMaxValueContext(Operator<BaseResultsBlock> operator, String column) {
      _operator = operator;
      DataSourceMetadata dataSourceMetadata = operator.getIndexSegment().getDataSource(column).getDataSourceMetadata();
      _minValue = dataSourceMetadata.getMinValue();
      _maxValue = dataSourceMetadata.getMaxValue();
    }
  }
}
