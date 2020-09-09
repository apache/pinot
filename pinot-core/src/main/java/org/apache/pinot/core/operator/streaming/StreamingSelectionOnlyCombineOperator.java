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
package org.apache.pinot.core.operator.streaming;

import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.CombineOperatorUtils;
import org.apache.pinot.core.query.exception.EarlyTerminationException;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for selection only streaming queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSelectionOnlyCombineOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingSelectionOnlyCombineOperator.class);
  private static final String OPERATOR_NAME = "StreamingSelectionOnlyCombineOperator";

  // Special IntermediateResultsBlock to indicate that this is the last results block for an operator
  private static final IntermediateResultsBlock LAST_RESULTS_BLOCK =
      new IntermediateResultsBlock(new DataSchema(new String[0], new DataSchema.ColumnDataType[0]),
          Collections.emptyList());

  private final List<Operator> _operators;
  private final QueryContext _queryContext;
  private final ExecutorService _executorService;
  private final long _endTimeMs;
  private final StreamObserver<Server.ServerResponse> _streamObserver;
  private final int _limit;

  public StreamingSelectionOnlyCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs, StreamObserver<Server.ServerResponse> streamObserver) {
    _operators = operators;
    _queryContext = queryContext;
    _executorService = executorService;
    _endTimeMs = endTimeMs;
    _streamObserver = streamObserver;
    _limit = queryContext.getLimit();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numOperators = _operators.size();
    int numThreads = CombineOperatorUtils.getNumThreadsForQuery(numOperators);

    // Use a BlockingQueue to store all the results blocks
    BlockingQueue<IntermediateResultsBlock> blockingQueue = new LinkedBlockingQueue<>();
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

            int numRowsCollected = 0;
            for (int operatorIndex = threadIndex; operatorIndex < numOperators; operatorIndex += numThreads) {
              Operator<IntermediateResultsBlock> operator = _operators.get(operatorIndex);
              try {
                IntermediateResultsBlock resultsBlock;
                while ((resultsBlock = operator.nextBlock()) != null) {
                  Collection<Object[]> rows = resultsBlock.getSelectionResult();
                  assert rows != null;
                  numRowsCollected += rows.size();
                  blockingQueue.offer(resultsBlock);
                  if (numRowsCollected >= _limit) {
                    return;
                  }
                }
                blockingQueue.offer(LAST_RESULTS_BLOCK);
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

    try {
      int numRowsCollected = 0;
      int numOperatorsFinished = 0;
      while (numRowsCollected < _limit && numOperatorsFinished < numOperators) {
        IntermediateResultsBlock resultsBlock =
            blockingQueue.poll(_endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        if (resultsBlock == null) {
          // Query times out, skip streaming the remaining results blocks
          LOGGER.error("Timed out while polling results block (query: {})", _queryContext);
          return new IntermediateResultsBlock(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR,
              new TimeoutException("Timed out while polling results block")));
        }
        if (resultsBlock.getProcessingExceptions() != null) {
          // Caught exception while processing segment, skip streaming the remaining results blocks and directly return
          // the exception
          return resultsBlock;
        }
        if (resultsBlock == LAST_RESULTS_BLOCK) {
          numOperatorsFinished++;
          continue;
        }
        DataSchema dataSchema = resultsBlock.getDataSchema();
        Collection<Object[]> rows = resultsBlock.getSelectionResult();
        assert dataSchema != null && rows != null;
        numRowsCollected += rows.size();
        DataTable dataTable = SelectionOperatorUtils.getDataTableFromRows(rows, dataSchema);
        _streamObserver.onNext(StreamingResponseUtils.getDataResponse(dataTable));
      }
      IntermediateResultsBlock metadataBlock = new IntermediateResultsBlock();
      CombineOperatorUtils.setExecutionStatistics(metadataBlock, _operators);
      return metadataBlock;
    } catch (Exception e) {
      LOGGER.error("Caught exception while streaming results blocks (query: {})", _queryContext, e);
      return new IntermediateResultsBlock(QueryException.INTERNAL_ERROR, e);
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
}
