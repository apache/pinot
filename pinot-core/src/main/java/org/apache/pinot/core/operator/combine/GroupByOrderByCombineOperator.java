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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptions;
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
  private static final String EXPLAIN_NAME = "COMBINE_GROUPBY_ORDERBY";
  private final int _trimSize;
  private final int _trimThreshold;
  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  private final ConcurrentLinkedQueue<ProcessingException> _mergedProcessingExceptions = new ConcurrentLinkedQueue<>();
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;

  private volatile IndexedTable _indexedTable;

  public GroupByOrderByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs, int maxExecutionThreads, int minTrimSize, int trimThreshold) {
    // NOTE: For group-by queries, when maxExecutionThreads is not explicitly configured, create one thread per operator
    super(operators, queryContext, executorService, endTimeMs,
        maxExecutionThreads > 0 ? maxExecutionThreads : operators.size());

    Map<String, String> queryOptions = queryContext.getQueryOptions();
    if (queryOptions != null) {
      Integer minTrimSizeOption = QueryOptions.getMinServerGroupTrimSize(queryOptions);
      if (minTrimSizeOption != null) {
        minTrimSize = minTrimSizeOption;
      }
    }
    if (minTrimSize > 0) {
      int limit = queryContext.getLimit();
      if (queryContext.getOrderByExpressions() != null || queryContext.getHavingFilter() != null) {
        _trimSize = GroupByUtils.getTableCapacity(limit, minTrimSize);
      } else {
        // TODO: Keeping only 'LIMIT' groups can cause inaccurate result because the groups are randomly selected
        //       without ordering. Consider ordering on group-by columns if no ordering is specified.
        _trimSize = limit;
      }
      _trimThreshold = trimThreshold;
    } else {
      // Server trim is disabled
      _trimSize = Integer.MAX_VALUE;
      _trimThreshold = Integer.MAX_VALUE;
    }

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
    _operatorLatch = new CountDownLatch(_numTasks);
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
   * Executes query on one segment in a worker thread and merges the results into the indexed table.
   */
  @Override
  protected void processSegments(int taskIndex) {
    for (int operatorIndex = taskIndex; operatorIndex < _numOperators; operatorIndex += _numTasks) {
      Operator operator = _operators.get(operatorIndex);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) operator.nextBlock();
        if (_indexedTable == null) {
          synchronized (this) {
            if (_indexedTable == null) {
              DataSchema dataSchema = resultsBlock.getDataSchema();
              // NOTE: Use trimSize as resultSize on server size.
              if (_trimThreshold >= MAX_TRIM_THRESHOLD) {
                // special case of trim threshold where it is set to max value.
                // there won't be any trimming during upsert in this case.
                // thus we can avoid the overhead of read-lock and write-lock
                // in the upsert method.
                _indexedTable = new UnboundedConcurrentIndexedTable(dataSchema, _queryContext, _trimSize);
              } else {
                _indexedTable =
                    new ConcurrentIndexedTable(dataSchema, _queryContext, _trimSize, _trimSize, _trimThreshold);
              }
            }
          }
        }

        // Merge processing exceptions.
        List<ProcessingException> processingExceptionsToMerge = resultsBlock.getProcessingExceptions();
        if (processingExceptionsToMerge != null) {
          _mergedProcessingExceptions.addAll(processingExceptionsToMerge);
        }

        // Merge aggregation group-by result.
        // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
        Collection<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
        // For now, only GroupBy OrderBy query has pre-constructed intermediate records
        if (intermediateRecords == null) {
          // Merge aggregation group-by result.
          AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
          if (aggregationGroupByResult != null) {
            // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
            Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
            while (dicGroupKeyIterator.hasNext()) {
              GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
              Object[] keys = groupKey._keys;
              Object[] values = Arrays.copyOf(keys, _numColumns);
              int groupId = groupKey._groupId;
              for (int i = 0; i < _numAggregationFunctions; i++) {
                values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
              }
              _indexedTable.upsert(new Key(keys), new Record(values));
            }
          }
        } else {
          for (IntermediateRecord intermediateResult : intermediateRecords) {
            //TODO: change upsert api so that it accepts intermediateRecord directly
            _indexedTable.upsert(intermediateResult._key, intermediateResult._record);
          }
        }
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
  }

  @Override
  protected void onException(Exception e) {
    _mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
  }

  @Override
  protected void onFinish() {
    _operatorLatch.countDown();
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
      String errorMessage =
          String.format("Timed out while combining group-by order-by results after %dms, queryContext = %s", timeoutMs,
              _queryContext);
      LOGGER.error(errorMessage);
      return new IntermediateResultsBlock(new TimeoutException(errorMessage));
    }

    IndexedTable indexedTable = _indexedTable;
    indexedTable.finish(false);
    IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(indexedTable);

    // Set the processing exceptions.
    if (!_mergedProcessingExceptions.isEmpty()) {
      mergedBlock.setProcessingExceptions(new ArrayList<>(_mergedProcessingExceptions));
    }

    mergedBlock.setNumResizes(indexedTable.getNumResizes());
    mergedBlock.setResizeTimeMs(indexedTable.getResizeTimeMs());
    // TODO - set numGroupsLimitReached
    return mergedBlock;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
  }
}
