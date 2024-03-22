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
import java.util.List;
import java.util.function.Consumer;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;


public abstract class MultiStageOperator<K extends Enum<K> & StatMap.Key>
    implements Operator<TransferableBlock>, AutoCloseable {

  protected final OpChainExecutionContext _context;
  protected final String _operatorId;
  protected final OpChainStats _opChainStats;
  protected boolean _isEarlyTerminated;

  public MultiStageOperator(OpChainExecutionContext context) {
    _context = context;
    _operatorId = Joiner.on("_").join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
    _opChainStats = _context.getStats();
    _isEarlyTerminated = false;
  }

  protected abstract Logger logger();

  @Override
  public TransferableBlock nextBlock() {
    if (Tracing.ThreadAccountantOps.isInterrupted()) {
      throw new EarlyTerminationException("Interrupted while processing next block");
    }
    try (InvocationScope ignored = Tracing.getTracer().createScope(getClass())) {
      TransferableBlock nextBlock;
      if (shouldCollectStats()) {
        OperatorStats operatorStats = _opChainStats.getOperatorStats(_context, _operatorId);
        operatorStats.startTimer();
        try {
          nextBlock = getNextBlock();
        } catch (Exception e) {
          nextBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
        }
        operatorStats.recordRow(1, nextBlock.getNumRows());
        operatorStats.endTimer(nextBlock);
      } else {
        try {
          nextBlock = getNextBlock();
        } catch (Exception e) {
          nextBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
        }
      }
      return nextBlock;
    }
  }

  public String getOperatorId() {
    return _operatorId;
  }

  // Make it protected because we should always call nextBlock()
  protected abstract TransferableBlock getNextBlock()
      throws Exception;

  protected void earlyTerminate() {
    _isEarlyTerminated = true;
    for (MultiStageOperator child : getChildOperators()) {
      child.earlyTerminate();
    }
  }

  protected boolean shouldCollectStats() {
    return _context.isTraceEnabled();
  }

  @Override
  public abstract List<MultiStageOperator<?>> getChildOperators();

  // TODO: Ideally close() call should finish within request deadline.
  // TODO: Consider passing deadline as part of the API.
  @Override
  public void close() {
    for (MultiStageOperator<?> op : getChildOperators()) {
      try {
        op.close();
      } catch (Exception e) {
        logger().error("Failed to close operator: " + op + " with exception:" + e);
        // Continue processing because even one operator failed to be close, we should still close the rest.
      }
    }
  }

  public void cancel(Throwable e) {
    for (MultiStageOperator<?> op : getChildOperators()) {
      try {
        op.cancel(e);
      } catch (Exception e2) {
        logger().error("Failed to cancel operator:" + op + "with error:" + e + " with exception:" + e2);
        // Continue processing because even one operator failed to be cancelled, we should still cancel the rest.
      }
    }
  }

  public void forEachOperator(Consumer<MultiStageOperator<?>> consumer) {
    consumer.accept(this);
    for (MultiStageOperator<?> child : getChildOperators()) {
      child.forEachOperator(consumer);
    }
  }

  public abstract Class<K> getStatKeyClass();

  public enum Type {
    AGGREGATE,
    FILTER,
    HASH_JOIN,
    INTERSECT,
    LEAF,
    MAILBOX_RECEIVE,
    MAILBOX_SEND,
    PIPELINE,
    SORT,
    TRANSFORM,
    UNION,
    WINDOW
  }

  public enum BaseStatKeys implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG),
    EMITTED_ROWS(StatMap.Type.LONG),;
    private final StatMap.Type _type;

    BaseStatKeys(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
