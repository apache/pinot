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
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerOperator;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;


public abstract class MultiStageOperator<K extends Enum<K> & StatMap.Key>
    implements Operator<TransferableBlock>, AutoCloseable {

  protected final OpChainExecutionContext _context;
  protected final String _operatorId;
  protected final StatMap<K> _statMap;
  protected boolean _isEarlyTerminated;

  public MultiStageOperator(OpChainExecutionContext context, Class<K> keyStatClass) {
    _context = context;
    _operatorId = Joiner.on("_").join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
    _isEarlyTerminated = false;
    _statMap = new StatMap<>(keyStatClass);
  }

  protected abstract Logger logger();

  public abstract Type getOperatorType();

  public abstract K getExecutionTimeKey();

  public abstract K getEmittedRowsKey();

  @Override
  public TransferableBlock nextBlock() {
    if (Tracing.ThreadAccountantOps.isInterrupted()) {
      throw new EarlyTerminationException("Interrupted while processing next block");
    }
    if (logger().isDebugEnabled()) {
      logger().debug("Operator {}: Reading next block", _operatorId);
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
        _statMap.merge(getExecutionTimeKey(), executeStopwatch.elapsed(TimeUnit.MILLISECONDS));
        _statMap.merge(getEmittedRowsKey(), nextBlock.getNumRows());
      } else {
        try {
          nextBlock = getNextBlock();
        } catch (Exception e) {
          nextBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
        }
      }
      if (logger().isDebugEnabled()) {
        logger().debug("Operator {}. Block of type {} ready to send", _operatorId, nextBlock.getType());
      }
      return nextBlock;
    }
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

  /**
   * Adds the current operator stats as the last operator in the open stats of the given holder.
   *
   * It is assumed that:
   * <ol>
   *   <li>The current stage of the holder is equal to the stage id of this operator.</li>
   *   <li>The holder already contains the stats of the previous operators of the same stage in inorder</li>
   * </ol>
   */
  protected void addStats(MultiStageQueryStats holder) {
    Preconditions.checkArgument(holder.getCurrentStageId() == _context.getStageId(),
        "The holder's stage id should be the same as the current operator's stage id. Expected %s, got %s",
        _context.getStageId(), holder.getCurrentStageId());
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

  /**
   * Receives the EOS block from upstream operator and updates the stats.
   * <p>
   * The fact that the EOS belongs to the upstream operator is not an actual requirement. Actual requirements are listed
   * in {@link #addStats(MultiStageQueryStats)}
   * @param upstreamEos
   * @return
   */
  protected TransferableBlock updateEosBlock(TransferableBlock upstreamEos) {
    assert upstreamEos.isSuccessfulEndOfStreamBlock();
    MultiStageQueryStats queryStats = upstreamEos.getQueryStats();
    assert queryStats != null;
    addStats(queryStats);
    return upstreamEos;
  }

  public enum Type {
    AGGREGATE(AggregateOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<AggregateOperator.StatKey> stats = (StatMap<AggregateOperator.StatKey>) map;
        response.mergeNumGroupsLimitReached(stats.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED));
        response.mergeMaxRows(stats.getLong(AggregateOperator.StatKey.EMITTED_ROWS));
      }
    },
    FILTER(FilterOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<FilterOperator.StatKey> stats = (StatMap<FilterOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(FilterOperator.StatKey.EMITTED_ROWS));
      }
    },
    HASH_JOIN(HashJoinOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<HashJoinOperator.StatKey> stats = (StatMap<HashJoinOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(HashJoinOperator.StatKey.EMITTED_ROWS));
        response.mergeMaxRowsInJoinReached(stats.getBoolean(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
      }
    },
    INTERSECT(SetOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SetOperator.StatKey> stats = (StatMap<SetOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(SetOperator.StatKey.EMITTED_ROWS));
      }
    },
    LEAF(LeafStageTransferableBlockOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<LeafStageTransferableBlockOperator.StatKey> stats =
            (StatMap<LeafStageTransferableBlockOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(LeafStageTransferableBlockOperator.StatKey.EMITTED_ROWS));

        StatMap<DataTable.MetadataKey> v1Stats = new StatMap<>(DataTable.MetadataKey.class);
        for (LeafStageTransferableBlockOperator.StatKey statKey : stats.keySet()) {
          statKey.updateV1Metadata(v1Stats, stats);
        }
        response.addServerStats(v1Stats);
      }
    },
    LITERAL(LiteralValueOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        // Do nothing
      }
    },
    MAILBOX_RECEIVE(BaseMailboxReceiveOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<BaseMailboxReceiveOperator.StatKey> stats = (StatMap<BaseMailboxReceiveOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS));
      }
    },
    MAILBOX_SEND(MailboxSendOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<MailboxSendOperator.StatKey> stats = (StatMap<MailboxSendOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(MailboxSendOperator.StatKey.EMITTED_ROWS));
      }
    },
    MINUS(SetOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SetOperator.StatKey> stats = (StatMap<SetOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(SetOperator.StatKey.EMITTED_ROWS));
      }
    },
    PIPELINE_BREAKER(PipelineBreakerOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<PipelineBreakerOperator.StatKey> stats = (StatMap<PipelineBreakerOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(PipelineBreakerOperator.StatKey.EMITTED_ROWS));
      }
    },
    SORT(SortOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SortOperator.StatKey> stats = (StatMap<SortOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(SortOperator.StatKey.EMITTED_ROWS));
      }
    },
    TRANSFORM(TransformOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<TransformOperator.StatKey> stats = (StatMap<TransformOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(TransformOperator.StatKey.EMITTED_ROWS));
      }
    },
    UNION(SetOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SetOperator.StatKey> stats = (StatMap<SetOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(SetOperator.StatKey.EMITTED_ROWS));
      }
    },
    WINDOW(WindowAggregateOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<WindowAggregateOperator.StatKey> stats = (StatMap<WindowAggregateOperator.StatKey>) map;
        response.mergeMaxRows(stats.getLong(WindowAggregateOperator.StatKey.EMITTED_ROWS));
      }
    },;

    private final Class _statKeyClass;

    Type(Class<? extends StatMap.Key> statKeyClass) {
      _statKeyClass = statKeyClass;
    }

    @SuppressWarnings("unchecked")
    public StatMap<?> deserializeStats(DataInput input)
        throws IOException {
      return StatMap.deserialize(input, _statKeyClass);
    }

    public Class getStatKeyClass() {
      return _statKeyClass;
    }

    public abstract void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map);
  }
}
