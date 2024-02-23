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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;
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

  private final Map<Integer, Operator<TransferableBlock>> _workerMap;

  private Map<Integer, List<TransferableBlock>> _resultMap;
  private TransferableBlock _errorBlock;

  public PipelineBreakerOperator(OpChainExecutionContext context, Map<Integer, Operator<TransferableBlock>> workerMap) {
    super(context);
    _workerMap = workerMap;
    _resultMap = new HashMap<>();
    for (int workerKey : workerMap.keySet()) {
      _resultMap.put(workerKey, new ArrayList<>());
    }
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  public Map<Integer, List<TransferableBlock>> getResultMap() {
    return _resultMap;
  }

  @Nullable
  public TransferableBlock getErrorBlock() {
    return _errorBlock;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_errorBlock != null) {
      return _errorBlock;
    }
    // NOTE: Put an empty list for each worker in case there is no data block returned from that worker
    if (_workerMap.size() == 1) {
      Map.Entry<Integer, Operator<TransferableBlock>> entry = _workerMap.entrySet().iterator().next();
      List<TransferableBlock> dataBlocks = new ArrayList<>();
      _resultMap = Collections.singletonMap(entry.getKey(), dataBlocks);
      Operator<TransferableBlock> operator = entry.getValue();
      TransferableBlock block = operator.nextBlock();
      while (!block.isSuccessfulEndOfStreamBlock()) {
        if (block.isErrorBlock()) {
          _errorBlock = block;
          return block;
        }
        dataBlocks.add(block);
        block = operator.nextBlock();
      }
    } else {
      _resultMap = new HashMap<>();
      for (int workerKey : _workerMap.keySet()) {
        _resultMap.put(workerKey, new ArrayList<>());
      }
      // Keep polling from every operator in round-robin fashion
      Queue<Map.Entry<Integer, Operator<TransferableBlock>>> entries = new ArrayDeque<>(_workerMap.entrySet());
      while (!entries.isEmpty()) {
        Map.Entry<Integer, Operator<TransferableBlock>> entry = entries.poll();
        TransferableBlock block = entry.getValue().nextBlock();
        if (block.isErrorBlock()) {
          _errorBlock = block;
          return block;
        }
        if (block.isDataBlock()) {
          _resultMap.get(entry.getKey()).add(block);
          entries.offer(entry);
        }
      }
    }
    return TransferableBlockUtils.getEndOfStreamTransferableBlock();
  }
}
