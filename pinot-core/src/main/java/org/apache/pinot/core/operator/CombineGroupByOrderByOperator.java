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
package org.apache.pinot.core.operator;

import com.google.common.base.Preconditions;
import io.netty.util.internal.ConcurrentSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.TrimmingService;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineGroupByOperator</code> class is the operator to combine aggregation group-by results.
 */
public class CombineGroupByOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineGroupByOrderByOperator.class);
  private static final String OPERATOR_NAME = "CombineGroupByOperator";

  // Use a higher limit for groups stored across segments. For most cases, most groups from each segment should be the
  // same, thus the total number of groups across segments should be equal or slightly higher than the number of groups
  // in each segment. We still put a limit across segments to protect cases where data is very skewed across different
  // segments.
  private static final int INTER_SEGMENT_NUM_GROUPS_LIMIT_FACTOR = 2;

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  // Limit on number of groups stored, beyond which no new group will be created
  private final int _innerSegmentNumGroupsLimit;
  private final int _interSegmentNumGroupsLimit;
  private final boolean _aggregtionsInOrderBy;
  private final List<OrderByDefn> _orderByDefns;

  public CombineGroupByOrderByOperator(List<Operator> operators, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs, int innerSegmentNumGroupsLimit) {
    Preconditions.checkArgument(brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy());

    _operators = operators;
    _brokerRequest = brokerRequest;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
    _innerSegmentNumGroupsLimit = innerSegmentNumGroupsLimit;
    _interSegmentNumGroupsLimit =
        (int) Math.min((long) innerSegmentNumGroupsLimit * INTER_SEGMENT_NUM_GROUPS_LIMIT_FACTOR, Integer.MAX_VALUE);

    _orderByDefns =
        OrderByDefn.getOrderByDefnsFromBrokerRequest(_brokerRequest.getOrderBy(), _brokerRequest.getGroupBy(),
            _brokerRequest.getAggregationsInfo());
    _aggregtionsInOrderBy = OrderByDefn.isAggregationsInOrderBy(_orderByDefns);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines the group-by result blocks from underlying operators and returns a merged, sorted and trimmed group-by
   * result block.
   * <ul>
   *   <li>
   *     Concurrently merge group-by results form multiple result blocks into a map from group key to group results
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
  protected IntermediateResultsBlock getNextBlock() {
    int numOperators = _operators.size();
    CountDownLatch operatorLatch = new CountDownLatch(numOperators);
    ConcurrentHashMap<String, GroupByRow> resultsMap = new ConcurrentHashMap<>();
    // for the case with no aggregations in order by
    ConcurrentSet<String> keysAlreadyRemoved = new ConcurrentSet<>();
    OrderByExecutor orderByExecutor = new OrderByExecutor();
    PriorityBlockingQueue<GroupByRow> priorityQueue =
        new PriorityBlockingQueue<>(10, orderByExecutor.getComparator(_orderByDefns));

    ConcurrentLinkedQueue<ProcessingException> mergedProcessingExceptions = new ConcurrentLinkedQueue<>();

    AggregationFunctionContext[] aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(_brokerRequest.getAggregationsInfo(), null);
    int numAggregationFunctions = aggregationFunctionContexts.length;
    AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      aggregationFunctions[i] = aggregationFunctionContexts[i].getAggregationFunction();
    }

    Future[] futures = new Future[numOperators];
    for (int i = 0; i < numOperators; i++) {
      int index = i;
      futures[i] = _executorService.submit(new TraceRunnable() {
        @SuppressWarnings("unchecked")
        @Override
        public void runJob() {
          AggregationGroupByResult aggregationGroupByResult;

          try {
            IntermediateResultsBlock intermediateResultsBlock =
                (IntermediateResultsBlock) _operators.get(index).nextBlock();

            // Merge processing exceptions.
            List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
            if (processingExceptionsToMerge != null) {
              mergedProcessingExceptions.addAll(processingExceptionsToMerge);
            }

            // Merge aggregation group-by result.
            aggregationGroupByResult = intermediateResultsBlock.getAggregationGroupByResult();

            if (aggregationGroupByResult != null) {
              Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();

              // 2. If order by, we should not trim results at merge arbitrarily
              // 2.a if order by is on group keys, can compare while merging
              // 2.b if order by is on aggregation, trimming anything at all is wrong
              if (_aggregtionsInOrderBy) {
                while (groupKeyIterator.hasNext()) {
                  GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
                  resultsMap.compute(groupKey._stringKey, (key, value) -> {
                    if (value == null) {
                      Object[] resultToInsert = new Object[numAggregationFunctions];
                      for (int i = 0; i < numAggregationFunctions; i++) {
                        resultToInsert[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
                      }
                      GroupByRow rowToAdd = new GroupByRow(groupKey._stringKey, resultToInsert);
                      return rowToAdd;
                    } else {
                      Object[] resultToMerge = new Object[numAggregationFunctions];
                      for (int i = 0; i < numAggregationFunctions; i++) {
                        resultToMerge[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
                      }
                      GroupByRow rowToMerge = new GroupByRow(groupKey._stringKey, resultToMerge);
                      value.merge(rowToMerge, aggregationFunctions, numAggregationFunctions);
                      return value;
                    }
                  });
                }
              } else { // can trim while merging, but based on sort order
                // Iterate over the group-by keys, for each key, update the group-by result in the resultsMap. No trimming
                while (groupKeyIterator.hasNext()) {
                  GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
                  if (keysAlreadyRemoved.contains(groupKey._stringKey)) {
                    continue;
                  }
                  resultsMap.compute(groupKey._stringKey, (key, value) -> {
                    if (value == null) {
                      Object[] resultToInsert = new Object[numAggregationFunctions];
                      for (int i = 0; i < numAggregationFunctions; i++) {
                        resultToInsert[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
                      }
                      GroupByRow rowToAdd = new GroupByRow(groupKey._stringKey, resultToInsert);
                      priorityQueue.offer(rowToAdd);
                      return rowToAdd;
                    } else {
                      priorityQueue.remove(value);
                      Object[] resultToMerge = new Object[numAggregationFunctions];
                      for (int i = 0; i < numAggregationFunctions; i++) {
                        resultToMerge[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
                      }
                      GroupByRow rowToMerge = new GroupByRow(groupKey._stringKey, resultToMerge);
                      value.merge(rowToMerge, aggregationFunctions, numAggregationFunctions);
                      priorityQueue.offer(value);
                      return value;
                    }
                  });
                  if (priorityQueue.size() > _interSegmentNumGroupsLimit) { // this block is not thread safe
                    GroupByRow poll = priorityQueue.poll(); // poll in while?
                    resultsMap.remove(poll.getStringKey());
                    keysAlreadyRemoved.add(
                        poll.getStringKey()); // others could be adding to results before the key reaches here. think about this more
                  }
                }
              }
            }
          } catch (Exception e) {
            LOGGER.error("Exception processing CombineGroupBy for index {}, operator {}", index,
                _operators.get(index).getClass().getName(), e);
            mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
          }

          operatorLatch.countDown();
        }
      });
    }

    try {
      boolean opCompleted = operatorLatch.await(_timeOutMs, TimeUnit.MILLISECONDS);
      if (!opCompleted) {
        // If this happens, the broker side should already timed out, just log the error and return
        String errorMessage = "Timed out while combining group-by results after " + _timeOutMs + "ms";
        LOGGER.error(errorMessage);
        return new IntermediateResultsBlock(new TimeoutException(errorMessage));
      }

      List<GroupByRow> groupByRows = new ArrayList<>(resultsMap.values());

      List<GroupByRow> trimmedRows;
      // aggregations in order by, do not trim anything
      if (_aggregtionsInOrderBy) {
        trimmedRows = groupByRows;
      } else { // else, order, and then trim
        TrimmingService trimmingService =
            new TrimmingService(_interSegmentNumGroupsLimit, _brokerRequest.getGroupBy().getTopN(), _orderByDefns);
        trimmedRows = trimmingService.trim(groupByRows);
      }
      IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(aggregationFunctionContexts, trimmedRows);

      // Set the processing exceptions.
      if (!mergedProcessingExceptions.isEmpty()) {
        mergedBlock.setProcessingExceptions(new ArrayList<>(mergedProcessingExceptions));
      }

      // Set the execution statistics.
      ExecutionStatistics executionStatistics = new ExecutionStatistics();
      for (Operator operator : _operators) {
        ExecutionStatistics executionStatisticsToMerge = operator.getExecutionStatistics();
        if (executionStatisticsToMerge != null) {
          executionStatistics.merge(executionStatisticsToMerge);
        }
      }
      mergedBlock.setNumDocsScanned(executionStatistics.getNumDocsScanned());
      mergedBlock.setNumEntriesScannedInFilter(executionStatistics.getNumEntriesScannedInFilter());
      mergedBlock.setNumEntriesScannedPostFilter(executionStatistics.getNumEntriesScannedPostFilter());
      mergedBlock.setNumSegmentsProcessed(executionStatistics.getNumSegmentsProcessed());
      mergedBlock.setNumSegmentsMatched(executionStatistics.getNumSegmentsMatched());
      mergedBlock.setNumTotalRawDocs(executionStatistics.getNumTotalRawDocs());

      // TODO: this value should be set in the inner-segment operators. Setting it here might cause false positive as we
      //       are comparing number of groups across segments with the groups limit for each segment.
      if (resultsMap.size() >= _innerSegmentNumGroupsLimit) {
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
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
