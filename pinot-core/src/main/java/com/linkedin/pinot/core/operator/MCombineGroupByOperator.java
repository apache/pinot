/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.util.trace.TraceRunnable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>MCombineGroupByOperator</code> class is the operator to combine aggregation group-by results.
 */
public class MCombineGroupByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MCombineGroupByOperator.class);
  private static final String OPERATOR_NAME = "MCombineGroupByOperator";

  // TODO: check whether it is better to use thread local.
  // Choose a proper prime number for the number of locks.
  // Use prime number to reduce the conflict rate of different hashcodes.
  // Too small number of locks will cause high conflict rate.
  // Too large number of locks will consume too much memory.
  private static final int NUM_LOCKS = 10007;
  private static final Object[] LOCKS = new Object[NUM_LOCKS];

  static {
    for (int i = 0; i < NUM_LOCKS; i++) {
      LOCKS[i] = new Object();
    }
  }

  private final List<Operator> _operators;
  private final ExecutorService _executorService;
  private final BrokerRequest _brokerRequest;
  private final long _timeOutMs;

  /**
   * Constructor for the class.
   * - Initializes lock objects to synchronize updating aggregation group-by results.
   *
   * @param operators List of operators, whose result needs to be combined.
   * @param executorService Executor service to use for multi-threaded portions of combine.
   * @param timeOutMs Timeout for combine.
   * @param brokerRequest BrokerRequest corresponding to the query.
   */
  public MCombineGroupByOperator(List<Operator> operators, ExecutorService executorService, long timeOutMs,
      BrokerRequest brokerRequest) {
    Preconditions.checkArgument(brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy());

    _operators = operators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
    _timeOutMs = timeOutMs;
  }

  /**
   * {@inheritDoc}
   * Builds and returns a block containing result of combine:
   * - Group-by blocks from underlying operators are merged.
   * - Merged results are sorted and trimmed (for 'TOP N').
   * - Any exceptions encountered are also set in the merged result block
   *   that is returned.
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    return combineBlocks();
  }

  /**
   * This method combines the result blocks from underlying operators and builds a
   * merged, sorted and trimmed result block.
   * 1. Result blocks from underlying operators are merged concurrently into a
   *   HashMap, with appropriate synchronizations. Result blocks themselves are stored
   *   in the specified blocks[].
   *   - The key in this concurrent map is the group-by key, and value is an array of
   *     Objects (one for each aggregation function).
   *   - Synchronization is provided by locking the group-key that is to be modified.
   *
   * 2. The result of the concurrent map is then translated into what is expected by
   *    the broker (List<Map<String, Object>>).
   *
   * 3. This result is then sorted and then trimmed as per 'TOP N' in the brokerRequest.
   *
   * @return IntermediateResultBlock containing the final results from combine operation.
   */
  private IntermediateResultsBlock combineBlocks() {
    int numOperators = _operators.size();
    final CountDownLatch operatorLatch = new CountDownLatch(numOperators);
    final Map<String, Object[]> resultsMap = new ConcurrentHashMap<>();
    final ConcurrentLinkedQueue<ProcessingException> mergedProcessingExceptions = new ConcurrentLinkedQueue<>();

    AggregationFunctionContext[] aggregationFunctionContexts =
        AggregationFunctionUtils.getAggregationFunctionContexts(_brokerRequest.getAggregationsInfo(), null);
    final int numAggregationFunctions = aggregationFunctionContexts.length;
    final AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      aggregationFunctions[i] = aggregationFunctionContexts[i].getAggregationFunction();
    }

    Future[] futures = new Future[numOperators];
    for (int i = 0; i < numOperators; i++) {
      final int index = i;
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
              // Iterate over the group-by keys, for each key, update the group-by result in the resultsMap.
              Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
              while (groupKeyIterator.hasNext()) {
                GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
                String groupKeyString = groupKey._stringKey;

                // HashCode method might return negative value, make it non-negative
                int lockIndex = (groupKeyString.hashCode() & Integer.MAX_VALUE) % NUM_LOCKS;
                synchronized (LOCKS[lockIndex]) {
                  Object[] results = resultsMap.get(groupKeyString);

                  if (results == null) {
                    results = new Object[numAggregationFunctions];
                    for (int j = 0; j < numAggregationFunctions; j++) {
                      results[j] = aggregationGroupByResult.getResultForKey(groupKey, j);
                    }
                    resultsMap.put(groupKeyString, results);
                  } else {
                    for (int j = 0; j < numAggregationFunctions; j++) {
                      results[j] = aggregationFunctions[j].merge(results[j],
                          aggregationGroupByResult.getResultForKey(groupKey, j));
                    }
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

      // Trim the results map.
      AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
          new AggregationGroupByTrimmingService(aggregationFunctions, (int) _brokerRequest.getGroupBy().getTopN());
      List<Map<String, Object>> trimmedResults =
          aggregationGroupByTrimmingService.trimIntermediateResultsMap(resultsMap);
      IntermediateResultsBlock mergedBlock =
          new IntermediateResultsBlock(aggregationFunctionContexts, trimmedResults, true);

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
      mergedBlock.setNumTotalRawDocs(executionStatistics.getNumTotalRawDocs());

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
