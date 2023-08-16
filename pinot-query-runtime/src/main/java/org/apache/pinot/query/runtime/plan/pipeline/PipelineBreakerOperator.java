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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


class PipelineBreakerOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "PIPELINE_BREAKER";
  private final Deque<Map.Entry<Integer, Operator<TransferableBlock>>> _workerEntries;
  private final Map<Integer, List<TransferableBlock>> _resultMap;
  private final ImmutableSet<Integer> _expectedKeySet;
  private TransferableBlock _errorBlock;


  public PipelineBreakerOperator(OpChainExecutionContext context,
      Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap) {
    super(context);
    _resultMap = new HashMap<>();
    _expectedKeySet = ImmutableSet.copyOf(pipelineWorkerMap.keySet());
    _workerEntries = new ArrayDeque<>();
    _workerEntries.addAll(pipelineWorkerMap.entrySet());
    for (int workerKey : _expectedKeySet) {
      _resultMap.put(workerKey, new ArrayList<>());
    }
  }

  public Map<Integer, List<TransferableBlock>> getResultMap() {
    return _resultMap;
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
      constructErrorResponse(_errorBlock);
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

      // Release the mailbox worker when the block is end-of-stream
      if (block != null && !block.isNoOpBlock() && block.isSuccessfulEndOfStreamBlock()) {
        continue;
      }

      // Add the worker back to the queue if the block is not end-of-stream
      _workerEntries.add(worker);
      if (block != null && !block.isNoOpBlock()) {
        if (block.isErrorBlock()) {
          _errorBlock = block;
          constructErrorResponse(block);
          return _errorBlock;
        }
        if (!block.isEndOfStreamBlock()) {
          _resultMap.get(worker.getKey()).add(block);
        }
        return block;
      }
    }

    if (_workerEntries.isEmpty()) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    } else {
      return TransferableBlockUtils.getNoOpTransferableBlock();
    }
  }

  /**
   * Setting all result map to error if any of the pipeline breaker returns an ERROR.
   */
  private void constructErrorResponse(TransferableBlock errorBlock) {
    for (int key : _expectedKeySet) {
      _resultMap.put(key, Collections.singletonList(errorBlock));
    }
  }
}
