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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiConsumer;
import org.apache.pinot.query.runtime.operator.utils.BlockingStream;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


class PipelineBreakerOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "PIPELINE_BREAKER";
  private final Map<Integer, List<TransferableBlock>> _resultMap;
  private final ImmutableSet<Integer> _expectedKeySet;
  private final BlockingMultiConsumer<Pair<Integer, TransferableBlock>> _blockConsumer;


  public PipelineBreakerOperator(OpChainExecutionContext context,
      Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap) {
    super(context);
    _resultMap = new HashMap<>();
    _expectedKeySet = ImmutableSet.copyOf(pipelineWorkerMap.keySet());
    for (int workerKey : _expectedKeySet) {
      _resultMap.put(workerKey, new ArrayList<>());
    }
    _blockConsumer = new MyBlockingMultiConsumer(context.getId(), context.getDeadlineMs(), context.getExecutor(),
        pipelineWorkerMap.entrySet());
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
    Pair<Integer, TransferableBlock> pair = _blockConsumer.readBlockBlocking();
    TransferableBlock block = pair.getRight();
    if (block.isDataBlock()) {
      _resultMap.get(pair.getLeft()).add(block);
      return block;
    } else if (block.isErrorBlock()) {
      constructErrorResponse(block);
      return block;
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
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

  private static class MyBlockingMultiConsumer extends BlockingMultiConsumer<Pair<Integer, TransferableBlock>> {
    public MyBlockingMultiConsumer(Object id, long deadlineMs, Executor executor,
        Collection<Map.Entry<Integer, Operator<TransferableBlock>>> entries) {
      super(id, deadlineMs, executor, entries.stream()
          .map(PipelineBlockProducer::new)
          .collect(Collectors.toList()));
    }

    @Override
    protected boolean isError(Pair<Integer, TransferableBlock> element) {
      return element.getRight().isErrorBlock();
    }

    @Override
    protected boolean isEos(Pair<Integer, TransferableBlock> element) {
      return element.getRight().isSuccessfulEndOfStreamBlock();
    }

    @Override
    protected Pair<Integer, TransferableBlock> onTimeout() {
      return Pair.of(-1, TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR));
    }

    @Override
    protected Pair<Integer, TransferableBlock> onException(Exception e) {
      return Pair.of(-1, TransferableBlockUtils.getErrorTransferableBlock(e));
    }

    @Override
    protected Pair<Integer, TransferableBlock> onEos() {
      return Pair.of(-1, TransferableBlockUtils.getEndOfStreamTransferableBlock());
    }
  }

  private static class PipelineBlockProducer implements BlockingStream<Pair<Integer, TransferableBlock>> {
    private final Map.Entry<Integer, Operator<TransferableBlock>> _workEntry;

    public PipelineBlockProducer(Map.Entry<Integer, Operator<TransferableBlock>> workEntry) {
      _workEntry = workEntry;
    }

    @Override
    public Object getId() {
      return _workEntry.getKey();
    }

    @Override
    public Pair<Integer, TransferableBlock> get() {
      return Pair.of(_workEntry.getKey(), _workEntry.getValue().nextBlock());
    }

    @Override
    public void cancel() {
      // Nothing to do
    }
  }
}
