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
import com.google.common.base.Stopwatch;
import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;


public abstract class MultiStageOperator<K extends Enum<K> & StatMap.Key>
    implements Operator<TransferableBlock>, AutoCloseable {

  protected final OpChainExecutionContext _context;
  protected final String _operatorId;
  protected final StatMap<K> _statMap = new StatMap<>(getStatKeyClass());
  protected boolean _isEarlyTerminated;

  public MultiStageOperator(OpChainExecutionContext context) {
    _context = context;
    _operatorId = Joiner.on("_").join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
    _isEarlyTerminated = false;
  }

  protected abstract Logger logger();

  public abstract Type getOperatorType();

  /**
   * Returns the class of the stat key.
   *
   * Note for implementations: This method may be called before the constructor of the implementing class is finished.
   * Therefore must not relay on any state of the implementing class.
   */
  public abstract Class<K> getStatKeyClass();

  protected abstract void recordExecutionStats(long executionTimeMs, TransferableBlock block);

  @Override
  public TransferableBlock nextBlock() {
    if (Tracing.ThreadAccountantOps.isInterrupted()) {
      throw new EarlyTerminationException("Interrupted while processing next block");
    }
    try (InvocationScope ignored = Tracing.getTracer().createScope(getClass())) {
      TransferableBlock nextBlock;
      if (shouldCollectStats()) {
        Stopwatch executeStopwatch = Stopwatch.createStarted();
        try {
          nextBlock = getNextBlock();
        } catch (Exception e) {
          nextBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
        }
        recordExecutionStats(executeStopwatch.elapsed(TimeUnit.MILLISECONDS), nextBlock);
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
    for (MultiStageOperator<?> child : getChildOperators()) {
      child.earlyTerminate();
    }
  }

  protected void addStats(MultiStageQueryStats holder) {
    holder.getCurrentStats().addLastOperator(getOperatorType(), _statMap);
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

  protected TransferableBlock updateEosBlock(TransferableBlock upstreamEos) {
    assert upstreamEos.isSuccessfulEndOfStreamBlock();
    MultiStageQueryStats queryStats = upstreamEos.getQueryStats();
    assert queryStats != null;
    addStats(queryStats);
    return upstreamEos;
  }

  public abstract static class WithBasicStats extends MultiStageOperator<BaseStatKeys> {
    public WithBasicStats(OpChainExecutionContext context) {
      super(context);
    }

    @Override
    public Class<BaseStatKeys> getStatKeyClass() {
      return BaseStatKeys.class;
    }

    @Override
    protected void recordExecutionStats(long executionTimeMs, TransferableBlock block) {
      _statMap.merge(BaseStatKeys.EXECUTION_TIME_MS, executionTimeMs);
      _statMap.merge(BaseStatKeys.EMITTED_ROWS, block.getNumRows());
    }
  }

  public enum Type {
    AGGREGATE {
      @Override
      public StatMap<AggregateOperator.AggregateStats> deserializeStats(DataInput input)
          throws IOException {
        return StatMap.deserialize(input, AggregateOperator.AggregateStats.class);
      }

      @Override
      public StatMap<AggregateOperator.AggregateStats> emptyStats() {
        return new StatMap<>(AggregateOperator.AggregateStats.class);
      }
    },
    FILTER,
    HASH_JOIN {
      @Override
      public StatMap<HashJoinOperator.HashJoinStats> deserializeStats(DataInput input)
          throws IOException {
        return StatMap.deserialize(input, HashJoinOperator.HashJoinStats.class);
      }

      @Override
      public StatMap<HashJoinOperator.HashJoinStats> emptyStats() {
        return new StatMap<>(HashJoinOperator.HashJoinStats.class);
      }
    },
    INTERSECT,
    LEAF {
      @Override
      public StatMap<DataTable.MetadataKey> deserializeStats(DataInput input)
          throws IOException {
        return StatMap.deserialize(input, DataTable.MetadataKey.class);
      }

      @Override
      public StatMap<DataTable.MetadataKey> emptyStats() {
        return new StatMap<>(DataTable.MetadataKey.class);
      }
    },
    LITERAL,
    MAILBOX_RECEIVE {
      @Override
      public StatMap<BaseMailboxReceiveOperator.StatKey> deserializeStats(DataInput input)
          throws IOException {
        return StatMap.deserialize(input, BaseMailboxReceiveOperator.StatKey.class);
      }

      @Override
      public StatMap<BaseMailboxReceiveOperator.StatKey> emptyStats() {
        return new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
      }
    },
    MAILBOX_SEND {
      @Override
      public StatMap<MailboxSendOperator.StatKey> deserializeStats(DataInput input)
          throws IOException {
        return StatMap.deserialize(input, MailboxSendOperator.StatKey.class);
      }

      @Override
      public StatMap<MailboxSendOperator.StatKey> emptyStats() {
        return new StatMap<>(MailboxSendOperator.StatKey.class);
      }
    },
    MINUS,
    PIPELINE_BREAKER,
    SORT,
    TRANSFORM,
    UNION,
    WINDOW;

    public StatMap<?> deserializeStats(DataInput input)
        throws IOException {
      return StatMap.deserialize(input, BaseStatKeys.class);
    }

    public StatMap<?> emptyStats() {
      return new StatMap<>(BaseStatKeys.class);
    }
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
