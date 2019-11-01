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

  // Use at most 10 or half of the processors threads for each query.
  // If there are less than 2 processors, use 1 thread.
  // Runtime.getRuntime().availableProcessors() may return value < 2 in container based environment, e.g. Kubernetes.
  private static final int MAX_NUM_THREADS_PER_QUERY =
      Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;

  public CombineOperator(List<Operator> operators, ExecutorService executorService, long timeOutMs,
      BrokerRequest brokerRequest) {
    _operators = operators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
    _timeOutMs = timeOutMs;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    long startTimeMs = System.currentTimeMillis();
    long endTimeMs = startTimeMs + _timeOutMs;
    int numOperators = _operators.size();
    // Try to use all MAX_NUM_THREADS_PER_QUERY threads for the query, but ensure each thread has at least one operator
    int numThreads = Math.min(numOperators, MAX_NUM_THREADS_PER_QUERY);
    int numOperatorsPerThread = (numOperators + numThreads - 1) / numThreads;

    // We use a BlockingQueue to store the results for each operator group, and track if all operator groups are
    // finished by the query timeout, and cancel the unfinished futures (try to interrupt the execution if it already
    // started).
    // Besides the BlockingQueue, we also use a Phaser to ensure all the Futures are done (not scheduled, finished or
    // interrupted) before the main thread returns. We need to ensure no execution left before the main thread returning
    // because the main thread holds the reference to the segments, and if the segments are deleted/refreshed, the
    // segments can be released after the main thread returns, which would lead to undefined behavior (even JVM crash)
    // when executing queries against them.
    BlockingQueue<IntermediateResultsBlock> blockingQueue = new ArrayBlockingQueue<>(numThreads);
    Phaser phaser = new Phaser(1);

    // Submit operator group execution jobs
    Future[] futures = new Future[numThreads];
    for (int i = 0; i < numThreads; i++) {
      int index = i;
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

            int start = index * numOperatorsPerThread;
            int end = Math.min(start + numOperatorsPerThread, numOperators);
            IntermediateResultsBlock mergedBlock = (IntermediateResultsBlock) _operators.get(start).nextBlock();
            for (int i = start + 1; i < end; i++) {
              IntermediateResultsBlock blockToMerge = (IntermediateResultsBlock) _operators.get(i).nextBlock();
              try {
                CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
              } catch (Exception e) {
                LOGGER.error("Caught exception while merging two blocks (step 1).", e);
                mergedBlock
                    .addToProcessingExceptions(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
              }
            }
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

    // Submit operator groups merge job
    Future<IntermediateResultsBlock> mergedBlockFuture =
        _executorService.submit(new TraceCallable<IntermediateResultsBlock>() {
          @Override
          public IntermediateResultsBlock callJob()
              throws Exception {
            IntermediateResultsBlock mergedBlock =
                blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            if (mergedBlock == null) {
              throw new TimeoutException("Timed out while polling result from first thread");
            }
            int numMergedBlocks = 1;
            while (numMergedBlocks < numThreads) {
              IntermediateResultsBlock blockToMerge =
                  blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
              if (blockToMerge == null) {
                throw new TimeoutException("Timed out while polling result from thread: " + numMergedBlocks);
              }
              try {
                CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
              } catch (Exception e) {
                LOGGER.error("Caught exception while merging two blocks (step 2).", e);
                mergedBlock
                    .addToProcessingExceptions(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
              }
              numMergedBlocks++;
            }
            return mergedBlock;
          }
        });

    // Get merge results.
    IntermediateResultsBlock mergedBlock;
    try {
      mergedBlock = mergedBlockFuture.get(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
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
