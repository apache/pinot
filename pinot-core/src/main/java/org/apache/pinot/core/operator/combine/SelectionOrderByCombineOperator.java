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
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.query.exception.EarlyTerminationException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;


/**
 * Combine operator for selection order-by queries.
 * <p>When the first order-by expression is an identifier (column), skip processing the segments if possible based on
 * the column min/max value and keep enough documents to fulfill the LIMIT and OFFSET requirement.
 * <ul>
 *   <li>1. Sort all the segments by the column min/max value</li>
 *   <li>2. Keep processing segments until we get enough documents to fulfill the LIMIT and OFFSET requirement</li>
 *   <li>3. Skip processing the segments that cannot add values to the final result</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SelectionOrderByCombineOperator extends BaseCombineOperator {
  private static final String OPERATOR_NAME = "SelectionOrderByCombineOperator";

  // For min/max value based combine, when a thread detects that no more segments need to be processed, it inserts this
  // special IntermediateResultsBlock into the BlockingQueue to awake the main thread
  private static final IntermediateResultsBlock LAST_RESULTS_BLOCK =
      new IntermediateResultsBlock(new DataSchema(new String[0], new DataSchema.ColumnDataType[0]),
          Collections.emptyList());

  private final int _numRowsToKeep;

  public SelectionOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long timeOutMs) {
    super(operators, queryContext, executorService, timeOutMs);
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
    if (orderByExpressions.get(0).getExpression().getType() == ExpressionContext.Type.IDENTIFIER) {
      return minMaxValueBasedCombine();
    } else {
      return super.getNextBlock();
    }
  }

  private IntermediateResultsBlock minMaxValueBasedCombine() {
    long startTimeMs = System.currentTimeMillis();
    long endTimeMs = startTimeMs + _timeOutMs;

    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
    assert orderByExpressions != null;
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

    int numThreads = CombineOperatorUtils.getNumThreadsForQuery(numOperators);
    AtomicReference<Comparable> globalBoundaryValue = new AtomicReference<>();

    // Use a BlockingQueue to store the per-segment result
    BlockingQueue<IntermediateResultsBlock> blockingQueue = new ArrayBlockingQueue<>(numOperators);
    // Use an AtomicInteger to track the number of operators skipped (no result inserted into the BlockingQueue)
    AtomicInteger numOperatorsSkipped = new AtomicInteger();
    // Use a Phaser to ensure all the Futures are done (not scheduled, finished or interrupted) before the main thread
    // returns. We need to ensure this because the main thread holds the reference to the segments. If a segment is
    // deleted/refreshed, the segment will be released after the main thread returns, which would lead to undefined
    // behavior (even JVM crash) when processing queries against it.
    Phaser phaser = new Phaser(1);

    Future[] futures = new Future[numThreads];
    for (int i = 0; i < numThreads; i++) {
      int threadIndex = i;
      futures[i] = _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          try {
            // Register the thread to the phaser
            // NOTE: If the phaser is terminated (returning negative value) when trying to register the thread, that
            //       means the query execution has finished, and the main thread has deregistered itself and returned
            //       the result. Directly return as no execution result will be taken.
            if (phaser.register() < 0) {
              return;
            }

            // Keep a boundary value for the thread
            // NOTE: The thread boundary value can be different from the global boundary value because thread boundary
            //       value is updated after processing the segment, while global boundary value is updated after the
            //       segment result is merged.
            Comparable threadBoundaryValue = null;

            for (int operatorIndex = threadIndex; operatorIndex < numOperators; operatorIndex += numThreads) {
              // Calculate the boundary value from global boundary and thread boundary
              Comparable boundaryValue = globalBoundaryValue.get();
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
              MinMaxValueContext minMaxValueContext = minMaxValueContexts.get(operatorIndex);
              if (boundaryValue != null) {
                if (asc) {
                  // For ascending order, no need to process more segments if the column min value is larger than the
                  // boundary value, or is equal to the boundary value and the there is only one order-by expression
                  if (minMaxValueContext._minValue != null) {
                    int result = minMaxValueContext._minValue.compareTo(boundaryValue);
                    if (result > 0 || (result == 0 && numOrderByExpressions == 1)) {
                      numOperatorsSkipped.getAndAdd((numOperators - operatorIndex - 1) / numThreads);
                      blockingQueue.offer(LAST_RESULTS_BLOCK);
                      return;
                    }
                  }
                } else {
                  // For descending order, no need to process more segments if the column max value is smaller than the
                  // boundary value, or is equal to the boundary value and the there is only one order-by expression
                  if (minMaxValueContext._maxValue != null) {
                    int result = minMaxValueContext._maxValue.compareTo(boundaryValue);
                    if (result < 0 || (result == 0 && numOrderByExpressions == 1)) {
                      numOperatorsSkipped.getAndAdd((numOperators - operatorIndex - 1) / numThreads);
                      blockingQueue.offer(LAST_RESULTS_BLOCK);
                      return;
                    }
                  }
                }
              }

              // Process the segment
              try {
                IntermediateResultsBlock resultsBlock = minMaxValueContext._operator.nextBlock();
                PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
                if (selectionResult != null && selectionResult.size() == _numRowsToKeep) {
                  // Segment result has enough rows, update the boundary value
                  assert selectionResult.peek() != null;
                  Comparable segmentBoundaryValue = (Comparable) selectionResult.peek()[0];
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
                blockingQueue.offer(resultsBlock);
              } catch (EarlyTerminationException e) {
                // Early-terminated by interruption (canceled by the main thread)
                return;
              } catch (Exception e) {
                // Caught exception, skip processing the remaining operators
                LOGGER.error("Caught exception while executing operator of index: {} (query: {})", operatorIndex,
                    _queryContext, e);
                blockingQueue.offer(new IntermediateResultsBlock(e));
                return;
              }
            }
          } finally {
            phaser.arriveAndDeregister();
          }
        }
      });
    }

    IntermediateResultsBlock mergedBlock = null;
    try {
      int numBlocksMerged = 0;
      while (numBlocksMerged + numOperatorsSkipped.get() < numOperators) {
        IntermediateResultsBlock blockToMerge =
            blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        if (blockToMerge == null) {
          // Query times out, skip merging the remaining results blocks
          LOGGER.error("Timed out while polling results block, numBlocksMerged: {} (query: {})", numBlocksMerged,
              _queryContext);
          mergedBlock = new IntermediateResultsBlock(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR,
              new TimeoutException("Timed out while polling results block")));
          break;
        }
        if (blockToMerge.getProcessingExceptions() != null) {
          // Caught exception while processing segment, skip merging the remaining results blocks and directly return
          // the exception
          mergedBlock = blockToMerge;
          break;
        }
        if (mergedBlock == null) {
          mergedBlock = blockToMerge;
        } else {
          if (blockToMerge != LAST_RESULTS_BLOCK) {
            mergeResultsBlocks(mergedBlock, blockToMerge);
          }
        }
        numBlocksMerged++;

        // Update the boundary value if enough rows are collected
        PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) mergedBlock.getSelectionResult();
        if (selectionResult != null && selectionResult.size() == _numRowsToKeep) {
          assert selectionResult.peek() != null;
          globalBoundaryValue.set((Comparable) selectionResult.peek()[0]);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while merging results blocks (query: {})", _queryContext, e);
      mergedBlock = new IntermediateResultsBlock(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    } finally {
      // Cancel all ongoing jobs
      for (Future future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      // Deregister the main thread and wait for all threads done
      phaser.awaitAdvance(phaser.arriveAndDeregister());
    }

    CombineOperatorUtils.setExecutionStatistics(mergedBlock, _operators);
    return mergedBlock;
  }

  private static class MinMaxValueContext {
    final SelectionOrderByOperator _operator;
    final Comparable _minValue;
    final Comparable _maxValue;

    MinMaxValueContext(SelectionOrderByOperator operator, String column) {
      _operator = operator;
      DataSourceMetadata dataSourceMetadata = operator.getIndexSegment().getDataSource(column).getDataSourceMetadata();
      _minValue = dataSourceMetadata.getMinValue();
      _maxValue = dataSourceMetadata.getMaxValue();
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
