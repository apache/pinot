/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.operator.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.util.trace.TraceRunnable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Operator to combine the aggregation group by results.
 *
 * MCombineGroupByOperator will take the arguments below:
 *  1. BrokerRequest;
 *  2. Parallelism Parameters:
 *      ExecutorService;
 *  3. Inner-Segment Operators of type:
 *          {@link MAggregationGroupByOperator}
 *      Number of Operators is based on the pruned segments:
 *          one segment to one Operator.
 *
 *
 */
public class MCombineGroupByOperator extends BaseOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MCombineGroupByOperator.class);

  private static final int NUM_LOCKS = 1000;
  private static int MAX_THREADS_PER_QUERY = 10;
  private long _timeOutMs;

  private final BrokerRequest _brokerRequest;
  private final List<Operator> _operators;
  private final ExecutorService _executorService;
  final Object[] _locks;

  static {
    // No more than 10 threads per query
    int numCores = Runtime.getRuntime().availableProcessors();
    MAX_THREADS_PER_QUERY = Math.min(MAX_THREADS_PER_QUERY, (int) (numCores * .5));
  }

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

    _locks = new Object[NUM_LOCKS];
    for (int i = 0; i < NUM_LOCKS; i++) {
      _locks[i] = new Object();
    }
  }

  /**
   * {@inheritDoc}
   * Calls 'open' on all the underlying operators.
   *
   * @return
   */
  @Override
  public boolean open() {
    for (Operator operator : _operators) {
      operator.open();
    }
    return true;
  }

  /**
   * {@inheritDoc}
   * Builds and returns a block containing result of combine:
   * - Group-by blocks from underlying operators are merged.
   * - Merged results are sorted and trimmed (for 'TOP N').
   * - Any exceptions encountered are also set in the merged result block
   *   that is returned.
   *
   * @return
   */
  @Override
  public Block getNextBlock() {
    final IntermediateResultsBlock blocks[] = new IntermediateResultsBlock[_operators.size()];
    IntermediateResultsBlock mergedBlock = null;

    try {
      mergedBlock = combineBlocks(_brokerRequest, _operators, _executorService, blocks);
    } catch (InterruptedException e) {
      LOGGER.error("InterruptedException caught while executing CombineGroupBy", e);

      if (mergedBlock == null) {
        mergedBlock = new IntermediateResultsBlock(e);
      }

      List<ProcessingException> exceptions = mergedBlock.getExceptions();
      if (exceptions == null) {
        exceptions = new ArrayList<ProcessingException>();
      }
      exceptions.add(QueryException.getException(QueryException.COMBINE_GROUP_BY_EXCEPTION_ERROR, e));
      mergedBlock.setExceptionsList(exceptions);
    }

    return mergedBlock;
  }

  /**
   * This method combines the result blocks from underlying operators and builds a
   * merged, sorted and trimmed result block.
   * 1. Result blocks from underlying operators are merged concurrently into a
   *   HashMap, with appropriate synchronizations. Result blocks themselves are stored
   *   in the specified blocks[].
   *   - The key in this concurrent map is the group-by key, and value is an array of
   *     Serializables (one for each aggregation function).
   *   - Synchronization is provided by locking the group-key that is to be modified.
   *
   * 2. The result of the concurrent map is then translated into what is expected by
   *    the broker (Map<String, Serializable).
   *
   * 3. This result is then sorted and then trimmed as per 'TOP N' in the brokerRequest.
   *
   * @param brokerRequest BrokerRequest that is being serviced.
   * @param operators List of underlying operators whose results is to be combined.
   * @param executorService Executor service to use for concurrent executions.
   * @param blocks Array of blocks where the
   * @return IntermediateResultBlock containing the final results from combine operation.
   */
  private IntermediateResultsBlock combineBlocks(BrokerRequest brokerRequest, final List<Operator> operators,
      ExecutorService executorService, final IntermediateResultsBlock[] blocks)
      throws InterruptedException {
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();

    final List<AggregationFunction> aggregationFunctions =
        AggregationFunctionFactory.getAggregationFunction(brokerRequest);

    final int numAggrFunctions = aggregationsInfo.size();
    final int numOperators = operators.size();

    final Map<String, Serializable[]> resultsMap = new ConcurrentHashMap<>();
    final CountDownLatch operatorLatch = new CountDownLatch(numOperators);

    for (int i = 0; i < numOperators; i++) {
      final int index = i;

      executorService.execute(new TraceRunnable() {
        @Override
        public void runJob() {
          AggregationGroupByResult groupByResult;

          // If block is null, or there's no group-by result, then return as there's nothing to do.
          blocks[index] = (IntermediateResultsBlock) operators.get(index).nextBlock();
          if (blocks[index] == null) {
            return;
          }

          groupByResult = blocks[index].getAggregationGroupByResult();
          if (groupByResult == null) {
            return;
          }

          // Iterate over the group-by keys, for each key, update the group-by result in the resultsMap.
          final Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
          while (groupKeyIterator.hasNext()) {

            GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
            String groupKeyString = groupKey.getStringKey();

            int lockIndex = (groupKeyString.hashCode() & Integer.MAX_VALUE) % NUM_LOCKS;
            synchronized (_locks[(lockIndex)]) {
              Serializable[] results = resultsMap.get(groupKeyString);

              if (results == null) {
                results = new Serializable[numAggrFunctions];
                for (int j = 0; j < numAggrFunctions; j++) {
                  results[j] = groupByResult.getResultForKey(groupKey, j);
                }

                resultsMap.put(groupKeyString, results);
              } else {
                for (int j = 0; j < numAggrFunctions; j++) {
                  results[j] = aggregationFunctions.get(j)
                      .combineTwoValues(results[j], groupByResult.getResultForKey(groupKey, j));
                }
              }
            }
          }

          operatorLatch.countDown();
        }
      });
    }

    operatorLatch.await(_timeOutMs, TimeUnit.SECONDS);
    List<Map<String, Serializable>> untrimmedResults = buildMergedResults(resultsMap, numAggrFunctions);
    return buildResultBlock(aggregationFunctions, untrimmedResults, blocks);
  }

  /**
   * Helper method to build a list of maps (one element per aggregation function) containing a
   * group-key to aggregation function value mapping.
   *
   * @param numAggrFunctions
   * @param combinedResultsMap
   * @return
   */
  private List<Map<String, Serializable>> buildMergedResults(Map<String, Serializable[]> combinedResultsMap,
      int numAggrFunctions) {
    List<Map<String, Serializable>> mergedResults = new ArrayList<>();

    for (int i = 0; i < numAggrFunctions; i++) {
      mergedResults.add(new HashMap<String, Serializable>(combinedResultsMap.size()));
    }

    Iterator<Map.Entry<String, Serializable[]>> iterator = combinedResultsMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Serializable[]> entry = iterator.next();
      String groupKey = entry.getKey();

      Serializable[] results = entry.getValue();
      for (int i = 0; i < numAggrFunctions; i++) {
        mergedResults.get(i).put(groupKey, results[i]);
      }
    }

    return mergedResults;
  }

  /**
   * Helper method to builds and returns an IntermediateResultBlock containing the
   * merged results from all underlying operators.
   *
   * @param aggregationFunctions List of aggregation functions.
   * @param mergedResults Map containing the merged results.
   * @param blocks Array of blocks for which the results are being merged.
   * @return IntermediateResultsBlock containing merged results.
   */
  private IntermediateResultsBlock buildResultBlock(List<AggregationFunction> aggregationFunctions,
      List<Map<String, Serializable>> mergedResults, IntermediateResultsBlock[] blocks) {

    IntermediateResultsBlock resultBlock = new IntermediateResultsBlock(aggregationFunctions, mergedResults, true);
    List<ProcessingException> exceptions = null;

    long numDocsScanned = 0;
    long totalRawDocs = 0;

    for (int i = 0; i < blocks.length; i++) {
      numDocsScanned += blocks[i].getNumDocsScanned();
      totalRawDocs += blocks[i].getTotalRawDocs();

      List<ProcessingException> blockExceptions = blocks[i].getExceptions();
      if (blockExceptions != null) {
        if (exceptions == null) {
          exceptions = blockExceptions;
        } else {
          exceptions.addAll(blockExceptions);
        }
      }
    }

    resultBlock.setNumDocsScanned(numDocsScanned);
    resultBlock.setTotalRawDocs(totalRawDocs);
    resultBlock.setExceptionsList(exceptions);

    trimToSize(_brokerRequest, resultBlock);
    return resultBlock;
  }

  /**
   * Sort and trim the result based on 'TOP N' specified in the brokerRequest.
   * Sorting and trimming done in-place on the specified mergedBlock.
   *
   * @param brokerRequest
   * @param mergedBlock IntermediateResultsBlock containing the merged results, that need to be trimmed.
   */
  private void trimToSize(BrokerRequest brokerRequest, IntermediateResultsBlock mergedBlock) {
    AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
    List<Map<String, Serializable>> trimmedResults =
        aggregationGroupByOperatorService.trimToSize(mergedBlock.getAggregationGroupByOperatorResult());
    mergedBlock.setAggregationGroupByResult(trimmedResults);
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "MCombineGroupByOperator";
  }

  @Override
  public boolean close() {
    for (Operator operator : _operators) {
      operator.close();
    }
    return true;
  }
}
