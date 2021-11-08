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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.spi.data.DataSchema;
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
  private final AtomicLong _numRowsCollected = new AtomicLong();

  public StreamingSelectionOnlyCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, StreamObserver<Server.ServerResponse> streamObserver) {
    super(operators, queryContext, executorService);
    _streamObserver = streamObserver;
    _limit = queryContext.getLimit();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  protected void processSegments(int taskIndex) {
    for (int operatorIndex = taskIndex; operatorIndex < _numOperators; operatorIndex += _numTasks) {
      Operator<IntermediateResultsBlock> operator = _operators.get(operatorIndex);
      IntermediateResultsBlock resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        while ((resultsBlock = operator.nextBlock()) != null) {
          Collection<Object[]> rows = resultsBlock.getSelectionResult();
          assert rows != null;
          long numRowsCollected = _numRowsCollected.addAndGet(rows.size());
          _blockingQueue.offer(resultsBlock);
          if (numRowsCollected >= _limit) {
            return;
          }
        }
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
      _blockingQueue.offer(LAST_RESULTS_BLOCK);
    }
  }

  @Override
  protected IntermediateResultsBlock mergeResults()
      throws Exception {
    long numRowsCollected = 0;
    int numOperatorsFinished = 0;
    long endTimeMs = _queryContext.getEndTimeMs();
    while (numRowsCollected < _limit && numOperatorsFinished < _numOperators) {
      IntermediateResultsBlock resultsBlock =
          _blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
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
    // Return an empty results block for the metadata
    return new IntermediateResultsBlock();
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
  }
}
