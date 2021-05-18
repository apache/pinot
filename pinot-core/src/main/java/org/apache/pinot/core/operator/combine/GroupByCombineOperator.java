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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for aggregation group-by queries with PQL semantic.
 * TODO: Use CombineOperatorUtils.getNumThreadsForQuery() to get the parallelism of the query instead of using
 *   all threads
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByCombineOperator extends BaseCombineOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByCombineOperator.class);
  private static final String OPERATOR_NAME = "GroupByCombineOperator";

  // Use a higher limit for groups stored across segments. For most cases, most groups from each segment should be the
  // same, thus the total number of groups across segments should be equal or slightly higher than the number of groups
  // in each segment. We still put a limit across segments to protect cases where data is very skewed across different
  // segments.
  private static final int INTER_SEGMENT_NUM_GROUPS_LIMIT_FACTOR = 2;
  // Limit on number of groups stored, beyond which no new group will be created
  private final int _innerSegmentNumGroupsLimit;
  private final int _interSegmentNumGroupsLimit;

  private final ConcurrentHashMap<String, Object[]> _resultsMap = new ConcurrentHashMap<>();
  private final AtomicInteger _numGroups = new AtomicInteger();
  private final ConcurrentLinkedQueue<ProcessingException> _mergedProcessingExceptions = new ConcurrentLinkedQueue<>();
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numAggregationFunctions;
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;

  public GroupByCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService,
      long endTimeMs, int innerSegmentNumGroupsLimit) {
    // GroupByCombineOperator use numOperators as numThreads
    super(operators, queryContext, executorService, endTimeMs, operators.size());
    _innerSegmentNumGroupsLimit = innerSegmentNumGroupsLimit;
    _interSegmentNumGroupsLimit =
        (int) Math.min((long) innerSegmentNumGroupsLimit * INTER_SEGMENT_NUM_GROUPS_LIMIT_FACTOR, Integer.MAX_VALUE);

    _aggregationFunctions = _queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    _operatorLatch = new CountDownLatch(_numOperators);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  /**
   * Executes query on one segment in a worker thread and merges the results into the results map.
   */
  @Override
  protected void processSegments(int threadIndex) {
    try {
      IntermediateResultsBlock intermediateResultsBlock =
          (IntermediateResultsBlock) _operators.get(threadIndex).nextBlock();

      // Merge processing exceptions.
      List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
      if (processingExceptionsToMerge != null) {
        _mergedProcessingExceptions.addAll(processingExceptionsToMerge);
      }

      // Merge aggregation group-by result.
      AggregationGroupByResult aggregationGroupByResult = intermediateResultsBlock.getAggregationGroupByResult();
      if (aggregationGroupByResult != null) {
        // Iterate over the group-by keys, for each key, update the group-by result in the _resultsMap.
        Iterator<GroupKeyGenerator.StringGroupKey> groupKeyIterator =
            aggregationGroupByResult.getStringGroupKeyIterator();
        while (groupKeyIterator.hasNext()) {
          GroupKeyGenerator.StringGroupKey groupKey = groupKeyIterator.next();
          _resultsMap.compute(groupKey._stringKey, (key, value) -> {
            if (value == null) {
              if (_numGroups.getAndIncrement() < _interSegmentNumGroupsLimit) {
                value = new Object[_numAggregationFunctions];
                for (int i = 0; i < _numAggregationFunctions; i++) {
                  value[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
                }
              }
            } else {
              for (int i = 0; i < _numAggregationFunctions; i++) {
                value[i] =
                    _aggregationFunctions[i].merge(value[i], aggregationGroupByResult.getResultForKey(groupKey, i));
              }
            }
            return value;
          });
        }
      }
    } catch (EarlyTerminationException e) {
      // Early-terminated because query times out or is already satisfied
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while processing and combining group-by for index: {}, operator: {}, queryContext: {}",
          threadIndex, _operators.get(threadIndex).getClass().getName(), _queryContext, e);
      _mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    } finally {
      _operatorLatch.countDown();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines the group-by result blocks from underlying operators and returns a merged, sorted and trimmed group-by
   * result block.
   * <ul>
   *   <li>
   *     Merge group-by results form multiple result blocks into a map from group key to group results
   *   </li>
   *   <li>
   *     Sort and trim the results map based on {@code TOP N} in the request
   *     <p>Results map will be converted from {@code Map<String, Object[]>} to {@code List<Map<String, Object>>} which
   *     is expected by the broker
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
          .format("Timed out while combining group-by results after %dms, queryContext = %s", timeoutMs, _queryContext);
      LOGGER.error(errorMessage);
      return new IntermediateResultsBlock(new TimeoutException(errorMessage));
    }

    // Trim the results map.
    AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
        new AggregationGroupByTrimmingService(_queryContext);
    List<Map<String, Object>> trimmedResults =
        aggregationGroupByTrimmingService.trimIntermediateResultsMap(_resultsMap);
    IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(_aggregationFunctions, trimmedResults, true);

    // Set the processing exceptions.
    if (!_mergedProcessingExceptions.isEmpty()) {
      mergedBlock.setProcessingExceptions(new ArrayList<>(_mergedProcessingExceptions));
    }
    // TODO: this value should be set in the inner-segment operators. Setting it here might cause false positive as we
    //       are comparing number of groups across segments with the groups limit for each segment.
    if (_resultsMap.size() >= _innerSegmentNumGroupsLimit) {
      mergedBlock.setNumGroupsLimitReached(true);
    }

    return mergedBlock;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
  }
}
