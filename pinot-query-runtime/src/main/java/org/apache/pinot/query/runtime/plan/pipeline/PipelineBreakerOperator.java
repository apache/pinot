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
package org.apache.pinot.query.runtime.plan.pipeline;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


public class PipelineBreakerOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "PIPELINE_BREAKER";
  private final Deque<Map.Entry<Integer, Operator<TransferableBlock>>> _workerEntries;
  private final Map<Integer, List<TransferableBlock>> _resultMap;
  private final CountDownLatch _workerDoneLatch;
  private TransferableBlock _errorBlock;


  public PipelineBreakerOperator(OpChainExecutionContext context,
      Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap) {
    super(context);
    _resultMap = new HashMap<>();
    _workerEntries = new ArrayDeque<>();
    _workerEntries.addAll(pipelineWorkerMap.entrySet());
    _workerDoneLatch = new CountDownLatch(1);
  }

  public Map<Integer, List<TransferableBlock>> getResult()
      throws Exception {
    boolean isWorkerDone =
        _workerDoneLatch.await(_context.getDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    if (isWorkerDone && _errorBlock == null) {
      return _resultMap;
    } else {
      throw new RuntimeException("Unable to construct pipeline breaker results due to timeout.");
    }
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (System.currentTimeMillis() > _context.getDeadlineMs()) {
      _errorBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
      _workerDoneLatch.countDown();
      return _errorBlock;
    }

    // Poll from every mailbox operator in round-robin fashion:
    // - Return the first content block
    // - If no content block found but there are mailboxes not finished, return no-op block
    // - If all content blocks are already returned, return end-of-stream block
    int numWorkers = _workerEntries.size();
    for (int i = 0; i < numWorkers; i++) {
      Map.Entry<Integer, Operator<TransferableBlock>> worker = _workerEntries.remove();
      TransferableBlock block = worker.getValue().nextBlock();

      // Release the mailbox when the block is end-of-stream
      if (block != null && block.isSuccessfulEndOfStreamBlock()) {
        continue;
      }

      // Add the worker back to the queue if the block is not end-of-stream
      _workerEntries.add(worker);
      if (block != null) {
        if (block.isErrorBlock()) {
          _errorBlock = block;
          _workerDoneLatch.countDown();
          return _errorBlock;
        }
        List<TransferableBlock> blockList = _resultMap.computeIfAbsent(worker.getKey(), (k) -> new ArrayList<>());
        // TODO: only data block is handled, we also need to handle metadata block from upstream in the future.
        if (!block.isEndOfStreamBlock()) {
          blockList.add(block);
        }
      }
    }

    if (_workerEntries.isEmpty()) {
      // NOTIFY results are ready.
      _workerDoneLatch.countDown();
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    } else {
      return TransferableBlockUtils.getNoOpTransferableBlock();
    }
  }
}
