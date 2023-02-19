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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.LoggerFactory;


public abstract class MultiStageOperator implements Operator<TransferableBlock>, AutoCloseable {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MultiStageOperator.class);

  // TODO: Move to OperatorContext class.
  protected final long _requestId;
  protected final int _stageId;
  protected final VirtualServerAddress _serverAddress;
  protected final OperatorStats _operatorStats;
  protected final Map<String, OperatorStats> _operatorStatsMap;

  public MultiStageOperator(long requestId, int stageId, @Nullable VirtualServerAddress serverAddress) {
    _requestId = requestId;
    _stageId = stageId;
    _operatorStats = new OperatorStats(requestId, stageId, toExplainString());
    _serverAddress = serverAddress;
    _operatorStatsMap = new HashMap<>();
  }

  public Map<String, OperatorStats> getOperatorStatsMap() {
    return _operatorStatsMap;
  }

  @Override
  public TransferableBlock nextBlock() {
    if (Tracing.ThreadAccountantOps.isInterrupted()) {
      throw new EarlyTerminationException("Interrupted while processing next block");
    }
    try (InvocationScope ignored = Tracing.getTracer().createScope(getClass())) {
      _operatorStats.startTimer();
      TransferableBlock nextBlock = getNextBlock();
      _operatorStats.recordRow(1, nextBlock.getNumRows());
      _operatorStats.endTimer();
      // TODO: move this to centralized reporting in broker
      if (nextBlock.isEndOfStreamBlock()) {
        if (nextBlock.isSuccessfulEndOfStreamBlock()) {
          for (MultiStageOperator op : getChildOperators()) {
            _operatorStatsMap.putAll(op.getOperatorStatsMap());
          }

          if (!_operatorStats.getExecutionStats().isEmpty()) {
            String operatorId;
            if (_serverAddress != null) {
               operatorId = Joiner.on("_").join(toExplainString(), _requestId, _stageId, _serverAddress);
            } else {
               operatorId = Joiner.on("_").join(toExplainString(), _requestId, _stageId);
            }
            _operatorStatsMap.put(operatorId, _operatorStats);
          }
          return TransferableBlockUtils.getEndOfStreamTransferableBlock(
              OperatorUtils.getMetadataFromOperatorStats(_operatorStatsMap));
        }
      }
      return nextBlock;
    }
  }

  // Make it protected because we should always call nextBlock()
  protected abstract TransferableBlock getNextBlock();

  @Override
  public List<MultiStageOperator> getChildOperators() {
    throw new UnsupportedOperationException();
  }

  // TODO: Ideally close() call should finish within request deadline.
  // TODO: Consider passing deadline as part of the API.
  @Override
  public void close() {
    for (MultiStageOperator op : getChildOperators()) {
      try {
        op.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close operator: " + op + " with exception:" + e);
        // Continue processing because even one operator failed to be close, we should still close the rest.
      }
    }
  }

  public void cancel(Throwable e) {
    for (MultiStageOperator op : getChildOperators()) {
      try {
        op.cancel(e);
      } catch (Exception e2) {
        LOGGER.error("Failed to cancel operator:" + op + "with error:" + e + " with exception:" + e2);
        // Continue processing because even one operator failed to be cancelled, we should still cancel the rest.
      }
    }
  }
}
