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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.CombineOperatorUtils;
import org.apache.pinot.core.query.exception.EarlyTerminationException;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for selection only streaming queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingSelectionOnlyCombineOperator extends BaseCombineOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingSelectionOnlyCombineOperator.class);
  private static final String OPERATOR_NAME = "StreamingSelectionOnlyCombineOperator";

  // Special IntermediateResultsBlock to indicate that this is the last results block for an operator
  private static final IntermediateResultsBlock LAST_RESULTS_BLOCK =
      new IntermediateResultsBlock(new DataSchema(new String[0], new DataSchema.ColumnDataType[0]),
          Collections.emptyList());
  private final StreamObserver<Server.ServerResponse> _streamObserver;
  private final int _limit;

  public StreamingSelectionOnlyCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs, StreamObserver<Server.ServerResponse> streamObserver) {
    super(operators, queryContext, executorService, endTimeMs);
    _streamObserver = streamObserver;
    _limit = queryContext.getLimit();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  /**
   * {@inheritDoc}
   *
   * <p> Execute query on one or more segments in a single thread, and store multiple intermediate result blocks
   * into BlockingQueue, skip processing the more segments if there are enough documents to fulfill the LIMIT and
   * OFFSET requirement.
   */
  protected void processSegments(int threadIndex) {

    try {
      // Register the thread to the phaser
      // NOTE: If the phaser is terminated (returning negative value) when trying to register the thread, that
      //       means the query execution has finished, and the main thread has deregistered itself and returned
      //       the result. Directly return as no execution result will be taken.
      if (_phaser.register() < 0) {
        return;
      }

      int numRowsCollected = 0;
      for (int operatorIndex = threadIndex; operatorIndex < _numOperators; operatorIndex += _numThreads) {
        Operator<IntermediateResultsBlock> operator = _operators.get(operatorIndex);
        try {
          IntermediateResultsBlock resultsBlock;
          while ((resultsBlock = operator.nextBlock()) != null) {
            Collection<Object[]> rows = resultsBlock.getSelectionResult();
            assert rows != null;
            numRowsCollected += rows.size();
            _blockingQueue.offer(resultsBlock);
            if (numRowsCollected >= _limit) {
              return;
            }
          }
          _blockingQueue.offer(LAST_RESULTS_BLOCK);
        } catch (EarlyTerminationException e) {
          // Early-terminated by interruption (canceled by the main thread)
          return;
        } catch (Exception e) {
          // Caught exception, skip processing the remaining operators
          LOGGER
              .error("Caught exception while executing operator of index: {} (query: {})", operatorIndex, _queryContext,
                  e);
          _blockingQueue.offer(new IntermediateResultsBlock(e));
          return;
        }
      }
    } finally {
      _phaser.arriveAndDeregister();
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
  protected IntermediateResultsBlock mergeResultsFromSegments() {
    try {
      int numRowsCollected = 0;
      int numOperatorsFinished = 0;
      while (numRowsCollected < _limit && numOperatorsFinished < _numOperators) {
        IntermediateResultsBlock resultsBlock =
            _blockingQueue.poll(_endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
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
      for (Future future : _futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      // Deregister the main thread and wait for all threads done
      _phaser.awaitAdvance(_phaser.arriveAndDeregister());
    }
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {

  }
}
