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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.reduce.CombineService;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineOperator</code> class is the operator to combine selection results and aggregation only results.
 */
public class CombineOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineOperator.class);
  private static final String OPERATOR_NAME = "CombineOperator";

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  //Make this configurable
  //These two control the parallelism on a per query basis, depending on the number of segments to process
  private static final int MIN_THREADS_PER_QUERY;
  private static final int MAX_THREADS_PER_QUERY;
  private static final int MIN_SEGMENTS_PER_THREAD = 10;

  static {
    int numCores = Runtime.getRuntime().availableProcessors();
    MIN_THREADS_PER_QUERY = Math.max(1, (int) (numCores * .5));
    //Dont have more than 10 threads per query
    MAX_THREADS_PER_QUERY = Math.min(10, (int) (numCores * .5));
  }

  public CombineOperator(List<Operator> operators, ExecutorService executorService, long timeOutMs,
      BrokerRequest brokerRequest) {
    _operators = operators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
    _timeOutMs = timeOutMs;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    final long startTime = System.currentTimeMillis();
    final long queryEndTime = System.currentTimeMillis() + _timeOutMs;
    final int numOperators = _operators.size();
    // Ensure that the number of groups is not more than the number of segments
    final int numGroups = Math.min(numOperators, Math.max(MIN_THREADS_PER_QUERY,
        Math.min(MAX_THREADS_PER_QUERY, (numOperators + MIN_SEGMENTS_PER_THREAD - 1) / MIN_SEGMENTS_PER_THREAD)));

    final List<List<Operator>> operatorGroups = new ArrayList<>(numGroups);
    for (int i = 0; i < numGroups; i++) {
      operatorGroups.add(new ArrayList<>());
    }
    for (int i = 0; i < numOperators; i++) {
      operatorGroups.get(i % numGroups).add(_operators.get(i));
    }

    // We use a BlockingQueue to store the results for each operator group, and track if all operator groups are
    // finished by the query timeout, and cancel the unfinished futures (try to interrupt the execution if it already
    // started).
    // Besides the BlockingQueue, we also use a Phaser to ensure all the Futures are done (not scheduled, finished or
    // interrupted) before the main thread returns. We need to ensure no execution left before the main thread returning
    // because the main thread holds the reference to the segments, and if the segments are deleted/refreshed, the
    // segments can be released after the main thread returns, which would lead to undefined behavior (even JVM crash)
    // when executing queries against them.
    BlockingQueue<Block> blockingQueue = new ArrayBlockingQueue<>(numGroups);
    Phaser phaser = new Phaser(1);

    // Submit operator group execution jobs
    Future[] futures = new Future[numGroups];
    for (int i = 0; i < numGroups; i++) {
      List<Operator> operatorGroup = operatorGroups.get(i);
      futures[i] = _executorService.submit(new TraceRunnable() {
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

            IntermediateResultsBlock mergedBlock = null;
            for (Operator operator : operatorGroup) {
              IntermediateResultsBlock blockToMerge = (IntermediateResultsBlock) operator.nextBlock();
              if (mergedBlock == null) {
                mergedBlock = blockToMerge;
              } else {
                try {
                  CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
                } catch (Exception e) {
                  LOGGER.error("Caught exception while merging two blocks (step 1).", e);
                  mergedBlock
                      .addToProcessingExceptions(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
                }
              }
            }
            assert mergedBlock != null;
            blockingQueue.offer(mergedBlock);
          } catch (Exception e) {
            LOGGER.error("Caught exception while executing query.", e);
            blockingQueue.offer(new IntermediateResultsBlock(e));
          } finally {
            phaser.arriveAndDeregister();
          }
        }
      });
    }
    LOGGER.debug("Submitting operators to be run in parallel and it took:" + (System.currentTimeMillis() - startTime));

    // Submit operator groups merge job
    Future<IntermediateResultsBlock> mergedBlockFuture =
        _executorService.submit(new TraceCallable<IntermediateResultsBlock>() {
          @Override
          public IntermediateResultsBlock callJob()
              throws Exception {
            int mergedBlocksNumber = 0;
            IntermediateResultsBlock mergedBlock = null;
            while (mergedBlocksNumber < numGroups) {
              if (mergedBlock == null) {
                mergedBlock = (IntermediateResultsBlock) blockingQueue
                    .poll(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (mergedBlock != null) {
                  mergedBlocksNumber++;
                }
                LOGGER.debug("Got response from operator 0 after: {}", (System.currentTimeMillis() - startTime));
              } else {
                IntermediateResultsBlock blockToMerge = (IntermediateResultsBlock) blockingQueue
                    .poll(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                if (blockToMerge != null) {
                  try {
                    LOGGER.debug("Got response from operator {} after: {}", mergedBlocksNumber,
                        (System.currentTimeMillis() - startTime));
                    CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
                    LOGGER.debug("Merged response from operator {} after: {}", mergedBlocksNumber,
                        (System.currentTimeMillis() - startTime));
                  } catch (Exception e) {
                    LOGGER.error("Caught exception while merging two blocks (step 2).", e);
                    mergedBlock
                        .addToProcessingExceptions(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
                  }
                  mergedBlocksNumber++;
                }
              }
            }
            return mergedBlock;
          }
        });

    // Get merge results.
    IntermediateResultsBlock mergedBlock;
    try {
      mergedBlock = mergedBlockFuture.get(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Caught InterruptedException.", e);
      mergedBlock = new IntermediateResultsBlock(QueryException.getException(QueryException.FUTURE_CALL_ERROR, e));
    } catch (ExecutionException e) {
      LOGGER.error("Caught ExecutionException.", e);
      mergedBlock = new IntermediateResultsBlock(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
    } catch (TimeoutException e) {
      LOGGER.error("Caught TimeoutException", e);
      mergedBlockFuture.cancel(true);
      mergedBlock =
          new IntermediateResultsBlock(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR, e));
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

    // Update execution statistics.
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
    mergedBlock.setNumSegmentsProcessed(executionStatistics.getNumSegmentsProcessed());
    mergedBlock.setNumSegmentsMatched(executionStatistics.getNumSegmentsMatched());

    return mergedBlock;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
