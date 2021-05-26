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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.*;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for aggregation group-by queries with SQL semantic.
 * TODO: Use CombineOperatorUtils.getNumThreadsForQuery() to get the parallelism of the query instead of using
 *   all threads
 */
@SuppressWarnings("rawtypes")
public class GroupByOrderByCombineOperator extends BaseCombineOperator {
  public static final int MAX_TRIM_THRESHOLD = 1_000_000_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByOrderByCombineOperator.class);
  private static final String OPERATOR_NAME = "GroupByOrderByCombineOperator";
  private final int _trimSize;
  private final int _trimThreshold;
  private final Lock _initLock;
  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  private final ConcurrentLinkedQueue<ProcessingException> _mergedProcessingExceptions = new ConcurrentLinkedQueue<>();
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;
  private DataSchema _dataSchema;
  private ConcurrentIndexedTable _indexedTable;

  public GroupByOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs, int trimThreshold) {
    // GroupByOrderByCombineOperator use numOperators as numThreads
    super(operators, queryContext, executorService, endTimeMs, operators.size());
    _initLock = new ReentrantLock();
    _trimSize = GroupByUtils.getTableCapacity(_queryContext);
    _trimThreshold = trimThreshold;

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
    int numOperators = _operators.size();
    _operatorLatch = new CountDownLatch(numOperators);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  /**
   * Executes query on one segment in a worker thread and merges the results into the indexed table.
   */
  @Override
  protected void processSegments(int threadIndex) {
    try {
      IntermediateResultsBlock intermediateResultsBlock =
          (IntermediateResultsBlock) _operators.get(threadIndex).nextBlock();

      _initLock.lock();
      try {
        if (_dataSchema == null) {
          _dataSchema = intermediateResultsBlock.getDataSchema();
          if (_trimThreshold >= MAX_TRIM_THRESHOLD) {
            // special case of trim threshold where it is set to max value.
            // there won't be any trimming during upsert in this case.
            // thus we can avoid the overhead of read-lock and write-lock
            // in the upsert method.
            _indexedTable = new UnboundedConcurrentIndexedTable(_dataSchema, _queryContext, _trimSize, _trimThreshold);
          } else {
            _indexedTable = new ConcurrentIndexedTable(_dataSchema, _queryContext, _trimSize, _trimThreshold);
          }
        }
      } finally {
        _initLock.unlock();
      }

      // Merge processing exceptions.
      List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
      if (processingExceptionsToMerge != null) {
        _mergedProcessingExceptions.addAll(processingExceptionsToMerge);
      }

      // Merge aggregation group-by result.
        // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
        //TODO: change upsert api so that it accepts intermediateRecord directly
        Iterator<TableResizer.fullIntermediateResult> groupKeyIterator = intermediateResultsBlock.getIntermediateResultIterator();
        while (groupKeyIterator.hasNext()) {
          TableResizer.fullIntermediateResult intermediateResult = groupKeyIterator.next();
          _indexedTable.upsert(intermediateResult.getKey(), intermediateResult.getRecord());
        }

    } catch (EarlyTerminationException e) {
      // Early-terminated because query times out or is already satisfied
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while processing and combining group-by order-by for index: {}, operator: {}, queryContext: {}",
          threadIndex, _operators.get(threadIndex).getClass().getName(), _queryContext, e);
      _mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    } finally {
      _operatorLatch.countDown();
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
  protected IntermediateResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _endTimeMs - System.currentTimeMillis();
    boolean opCompleted = _operatorLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      // If this happens, the broker side should already timed out, just log the error and return
      String errorMessage = String
          .format("Timed out while combining group-by order-by results after %dms, queryContext = %s", timeoutMs,
              _queryContext);
      LOGGER.error(errorMessage);
      return new IntermediateResultsBlock(new TimeoutException(errorMessage));
    }

    _indexedTable.finish(false);
    IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(_indexedTable);

    // Set the processing exceptions.
    if (!_mergedProcessingExceptions.isEmpty()) {
      mergedBlock.setProcessingExceptions(new ArrayList<>(_mergedProcessingExceptions));
    }

    mergedBlock.setNumResizes(_indexedTable.getNumResizes());
    mergedBlock.setResizeTimeMs(_indexedTable.getResizeTimeMs());
    // TODO - set numGroupsLimitReached
    return mergedBlock;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
  }
}
