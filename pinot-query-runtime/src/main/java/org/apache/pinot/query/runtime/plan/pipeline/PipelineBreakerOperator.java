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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class PipelineBreakerOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineBreakerOperator.class);
  private static final String EXPLAIN_NAME = "PIPELINE_BREAKER";
  private final Deque<Map.Entry<Integer, Operator<TransferableBlock>>> _workerEntries;
  private final Map<Integer, List<TransferableBlock>> _resultMap;
  private final ImmutableSet<Integer> _expectedKeySet;
  private TransferableBlock _finalBlock;

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
    // Poll from every mailbox operator:
    // - Return the first content block
    // - If no content block found but there are mailboxes not finished, try again
    // - If all content blocks are already returned, return end-of-stream block
    while (!_workerEntries.isEmpty()) {
      if (_finalBlock != null) {
        return _finalBlock;
      }
      if (System.currentTimeMillis() > _context.getDeadlineMs()) {
        _finalBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
        constructErrorResponse(_finalBlock);
        return _finalBlock;
      }

      Map.Entry<Integer, Operator<TransferableBlock>> worker = _workerEntries.getLast();
      TransferableBlock block = worker.getValue().nextBlock();

      if (block == null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("==[PB]== Null block on " + _context.getId() + " worker " + worker.getKey());
        }
        continue;
      }

      // Release the mailbox worker when the block is end-of-stream
      if (block.isSuccessfulEndOfStreamBlock()) {
        _workerEntries.removeLast();
        continue;
      }

      if (block.isErrorBlock()) {
        _finalBlock = block;
      }
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("==[PB]== Returned block from : " + _context.getId() + " block: " + block);
      }
      _resultMap.get(worker.getKey()).add(block);
      return block;
    }
    if (System.currentTimeMillis() > _context.getDeadlineMs()) {
      _finalBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
      return _finalBlock;
    } else if (_finalBlock == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[PB]== Finished : " + _context.getId());
      }
      _finalBlock = TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    return _finalBlock;
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
