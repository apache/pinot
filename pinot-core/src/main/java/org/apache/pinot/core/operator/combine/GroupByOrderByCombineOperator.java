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
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.exception.EarlyTerminationException;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.spi.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for aggregation group-by queries with SQL semantic.
 * TODO:
 *   - Use CombineOperatorUtils.getNumThreadsForQuery() to get the parallelism of the query instead of using all threads
 *   - Try to extend BaseCombineOperator to reduce duplicate code
 */
@SuppressWarnings("rawtypes")
public class GroupByOrderByCombineOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByOrderByCombineOperator.class);
  private static final String OPERATOR_NAME = "GroupByOrderByCombineOperator";

  private final List<Operator> _operators;
  private final QueryContext _queryContext;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  private final int _indexedTableCapacity;
  private final Lock _initLock;
  private DataSchema _dataSchema;
  private ConcurrentIndexedTable _indexedTable;

  public GroupByOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long timeOutMs) {
    _operators = operators;
    _queryContext = queryContext;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
    _initLock = new ReentrantLock();
    _indexedTableCapacity = GroupByUtils.getTableCapacity(_queryContext);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines the group-by result blocks from underlying operators and returns a merged and trimmed group-by
   * result block.
   * <ul>
   *   <li>
   *     Concurrently merge group-by results from multiple result blocks into {@link org.apache.pinot.core.data.table.IndexedTable}
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    int numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    int numColumns = numGroupByExpressions + numAggregationFunctions;
    ConcurrentLinkedQueue<ProcessingException> mergedProcessingExceptions = new ConcurrentLinkedQueue<>();

    // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
    // futures (try to interrupt the execution if it already started).
    // Besides the CountDownLatch, we also use a Phaser to ensure all the Futures are done (not scheduled, finished or
    // interrupted) before the main thread returns. We need to ensure no execution left before the main thread returning
    // because the main thread holds the reference to the segments, and if the segments are deleted/refreshed, the
    // segments can be released after the main thread returns, which would lead to undefined behavior (even JVM crash)
    // when executing queries against them.
    int numOperators = _operators.size();
    CountDownLatch operatorLatch = new CountDownLatch(numOperators);
    Phaser phaser = new Phaser(1);

    Future[] futures = new Future[numOperators];
    for (int i = 0; i < numOperators; i++) {
      int index = i;
      futures[i] = _executorService.submit(new TraceRunnable() {
        @SuppressWarnings("unchecked")
        @Override
        public void runJob() {
          try {
            // Register the thread to the phaser.
            // If the phaser is terminated (returning negative value) when trying to register the thread, that means the
            // query execution has timed out, and the main thread has deregistered itself and returned the result.
            // Directly return as no execution result will be taken.
            if (phaser.register() < 0) {
              return;
            }

            IntermediateResultsBlock intermediateResultsBlock =
                (IntermediateResultsBlock) _operators.get(index).nextBlock();

            _initLock.lock();
            try {
              if (_dataSchema == null) {
                _dataSchema = intermediateResultsBlock.getDataSchema();
                _indexedTable = new ConcurrentIndexedTable(_dataSchema, _queryContext, _indexedTableCapacity);
              }
            } finally {
              _initLock.unlock();
            }

            // Merge processing exceptions.
            List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
            if (processingExceptionsToMerge != null) {
              mergedProcessingExceptions.addAll(processingExceptionsToMerge);
            }

            // Merge aggregation group-by result.
            AggregationGroupByResult aggregationGroupByResult = intermediateResultsBlock.getAggregationGroupByResult();
            if (aggregationGroupByResult != null) {
              if (numGroupByExpressions == 1) {
                // Get converter function
                Function converterFunction = getConverterFunction(_dataSchema.getColumnDataType(0));

                // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
                Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
                while (groupKeyIterator.hasNext()) {
                  Object[] values = new Object[numColumns];
                  GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
                  Object convertedKey = converterFunction.apply(groupKey._stringKey);
                  values[0] = convertedKey;
                  for (int i = 0; i < numAggregationFunctions; i++) {
                    values[i + 1] = aggregationGroupByResult.getResultForKey(groupKey, i);
                  }
                  Key key = new Key(new Object[]{convertedKey});
                  Record record = new Record(values);
                  _indexedTable.upsert(key, record);
                }
              } else {
                // Get converter functions
                Function[] converterFunctions = new Function[numGroupByExpressions];
                for (int i = 0; i < numGroupByExpressions; i++) {
                  converterFunctions[i] = getConverterFunction(_dataSchema.getColumnDataType(i));
                }

                // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
                Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
                while (groupKeyIterator.hasNext()) {
                  Object[] values = new Object[numColumns];
                  int columnIndex = 0;
                  GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
                  String[] stringKeys = groupKey.getKeys();
                  Object[] objectKeys = new Object[numGroupByExpressions];
                  for (int i = 0; i < numGroupByExpressions; i++) {
                    Object convertedKey = converterFunctions[i].apply(stringKeys[i]);
                    objectKeys[columnIndex] = convertedKey;
                    values[columnIndex] = convertedKey;
                    columnIndex++;
                  }
                  for (int i = 0; i < numAggregationFunctions; i++) {
                    values[columnIndex] = aggregationGroupByResult.getResultForKey(groupKey, i);
                    columnIndex++;
                  }
                  Key key = new Key(objectKeys);
                  Record record = new Record(values);
                  _indexedTable.upsert(key, record);
                }
              }
            }
          } catch (EarlyTerminationException e) {
            // Early-terminated because query times out or is already satisfied
          } catch (Exception e) {
            LOGGER.error(
                "Caught exception while processing and combining group-by order-by for index: {}, operator: {}, queryContext: {}",
                index, _operators.get(index).getClass().getName(), _queryContext, e);
            mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
          } finally {
            operatorLatch.countDown();
            phaser.arriveAndDeregister();
          }
        }
      });
    }

    try {
      boolean opCompleted = operatorLatch.await(_timeOutMs, TimeUnit.MILLISECONDS);
      if (!opCompleted) {
        // If this happens, the broker side should already timed out, just log the error and return
        String errorMessage = String
            .format("Timed out while combining group-by order-by results after %dms, queryContext = %s", _timeOutMs,
                _queryContext);
        LOGGER.error(errorMessage);
        return new IntermediateResultsBlock(new TimeoutException(errorMessage));
      }

      _indexedTable.finish(false);
      IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(_indexedTable);

      // Set the processing exceptions.
      if (!mergedProcessingExceptions.isEmpty()) {
        mergedBlock.setProcessingExceptions(new ArrayList<>(mergedProcessingExceptions));
      }

      // Set the execution statistics.
      CombineOperatorUtils.setExecutionStatistics(mergedBlock, _operators);

      if (_indexedTable.size() >= _indexedTableCapacity) {
        mergedBlock.setNumGroupsLimitReached(true);
      }

      return mergedBlock;
    } catch (Exception e) {
      return new IntermediateResultsBlock(e);
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
  }

  private Function<String, Object> getConverterFunction(DataSchema.ColumnDataType columnDataType) {
    switch (columnDataType) {
      case INT:
        return Integer::valueOf;
      case LONG:
        return Long::valueOf;
      case FLOAT:
        return Float::valueOf;
      case DOUBLE:
        return Double::valueOf;
      case STRING:
        return s -> s;
      case BYTES:
        return BytesUtils::toByteArray;
      // Add other group-by column type supports here
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
