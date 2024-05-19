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
import java.util.List;
import java.util.concurrent.TimeUnit;
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


public abstract class MultiStageOperator
    implements Operator<TransferableBlock>, AutoCloseable {

  protected final OpChainExecutionContext _context;
  protected final String _operatorId;
  protected boolean _isEarlyTerminated;

  public MultiStageOperator(OpChainExecutionContext context) {
    _context = context;
    _operatorId = Joiner.on("_").join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
    _isEarlyTerminated = false;
  }

  /**
   * Returns the logger for the operator.
   * <p>
   * This method is used to generic multi-stage operator messages using the name of the specific operator.
   * Implementations should not allocate new loggers for each call but instead reuse some (probably static and final)
   * attribute.
   */
  protected abstract Logger logger();

  public abstract Type getOperatorType();

  public abstract void registerExecution(long time, int numRows);

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
      Stopwatch executeStopwatch = Stopwatch.createStarted();
      try {
        nextBlock = getNextBlock();
      } catch (Exception e) {
        nextBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
      }
      registerExecution(executeStopwatch.elapsed(TimeUnit.MILLISECONDS), nextBlock.getNumRows());

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
    for (MultiStageOperator child : getChildOperators()) {
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
  protected void addStats(MultiStageQueryStats holder, StatMap<?> statMap) {
    Preconditions.checkArgument(holder.getCurrentStageId() == _context.getStageId(),
        "The holder's stage id should be the same as the current operator's stage id. Expected %s, got %s",
        _context.getStageId(), holder.getCurrentStageId());
    holder.getCurrentStats().addLastOperator(getOperatorType(), statMap);
  }

  @Override
  public abstract List<MultiStageOperator> getChildOperators();

  // TODO: Ideally close() call should finish within request deadline.
  // TODO: Consider passing deadline as part of the API.
  @Override
  public void close() {
    for (MultiStageOperator op : getChildOperators()) {
      try {
        op.close();
      } catch (Exception e) {
        logger().error("Failed to close operator: " + op + " with exception:" + e);
        // Continue processing because even one operator failed to be close, we should still close the rest.
      }
    }
  }

  public void cancel(Throwable e) {
    for (MultiStageOperator op : getChildOperators()) {
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
   * in {@link #addStats(MultiStageQueryStats, StatMap)}
   * @param upstreamEos
   * @return
   */
  protected TransferableBlock updateEosBlock(TransferableBlock upstreamEos, StatMap<?> statMap) {
    assert upstreamEos.isSuccessfulEndOfStreamBlock();
    MultiStageQueryStats queryStats = upstreamEos.getQueryStats();
    assert queryStats != null;
    addStats(queryStats, statMap);
    return upstreamEos;
  }

  /**
   * This enum is used to identify the operation type.
   * <p>
   * This is mostly used in the context of stats collection, where we use this enum in the serialization form in order
   * to identify the type of the stats in an efficient way.
   */
  public enum Type {
    AGGREGATE(AggregateOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<AggregateOperator.StatKey> stats = (StatMap<AggregateOperator.StatKey>) map;
        response.mergeNumGroupsLimitReached(stats.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED));
        response.mergeMaxRowsInOperator(stats.getLong(AggregateOperator.StatKey.EMITTED_ROWS));
      }
    },
    FILTER(FilterOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<FilterOperator.StatKey> stats = (StatMap<FilterOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(FilterOperator.StatKey.EMITTED_ROWS));
      }
    },
    HASH_JOIN(HashJoinOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<HashJoinOperator.StatKey> stats = (StatMap<HashJoinOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(HashJoinOperator.StatKey.EMITTED_ROWS));
        response.mergeMaxRowsInJoinReached(stats.getBoolean(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED));
      }
    },
    INTERSECT(SetOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SetOperator.StatKey> stats = (StatMap<SetOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(SetOperator.StatKey.EMITTED_ROWS));
      }
    },
    LEAF(LeafStageTransferableBlockOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<LeafStageTransferableBlockOperator.StatKey> stats =
            (StatMap<LeafStageTransferableBlockOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(LeafStageTransferableBlockOperator.StatKey.EMITTED_ROWS));

        StatMap<BrokerResponseNativeV2.StatKey> brokerStats = new StatMap<>(BrokerResponseNativeV2.StatKey.class);
        for (LeafStageTransferableBlockOperator.StatKey statKey : stats.keySet()) {
          statKey.updateBrokerMetadata(brokerStats, stats);
        }
        response.addBrokerStats(brokerStats);
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
        response.mergeMaxRowsInOperator(stats.getLong(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS));
      }
    },
    MAILBOX_SEND(MailboxSendOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<MailboxSendOperator.StatKey> stats = (StatMap<MailboxSendOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(MailboxSendOperator.StatKey.EMITTED_ROWS));
      }
    },
    MINUS(SetOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SetOperator.StatKey> stats = (StatMap<SetOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(SetOperator.StatKey.EMITTED_ROWS));
      }
    },
    PIPELINE_BREAKER(PipelineBreakerOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<PipelineBreakerOperator.StatKey> stats = (StatMap<PipelineBreakerOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(PipelineBreakerOperator.StatKey.EMITTED_ROWS));
      }
    },
    SORT_OR_LIMIT(SortOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SortOperator.StatKey> stats = (StatMap<SortOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(SortOperator.StatKey.EMITTED_ROWS));
      }
    },
    TRANSFORM(TransformOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<TransformOperator.StatKey> stats = (StatMap<TransformOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(TransformOperator.StatKey.EMITTED_ROWS));
      }
    },
    UNION(SetOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<SetOperator.StatKey> stats = (StatMap<SetOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(SetOperator.StatKey.EMITTED_ROWS));
      }
    },
    WINDOW(WindowAggregateOperator.StatKey.class) {
      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
        @SuppressWarnings("unchecked")
        StatMap<WindowAggregateOperator.StatKey> stats = (StatMap<WindowAggregateOperator.StatKey>) map;
        response.mergeMaxRowsInOperator(stats.getLong(WindowAggregateOperator.StatKey.EMITTED_ROWS));
        response.mergeMaxRowsInWindowReached(
            stats.getBoolean(WindowAggregateOperator.StatKey.MAX_ROWS_IN_WINDOW_REACHED));
      }
    },;

    private final Class _statKeyClass;

    Type(Class<? extends StatMap.Key> statKeyClass) {
      _statKeyClass = statKeyClass;
    }

    /**
     * Gets the class of the stat key for this operator type.
     * <p>
     * Notice that this is not including the generic type parameter, because Java generic types are not expressive
     * enough indicate what we want to say, so generics here are more problematic than useful.
     */
    public Class getStatKeyClass() {
      return _statKeyClass;
    }

    /**
     * Merges the stats from the given map into the given broker response.
     * <p>
     * Each literal has its own implementation of this method, which assumes the given map is of the correct type
     * (compatible with {@link #getStatKeyClass()}). This is a way to avoid casting in the caller.
     */
    public abstract void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map);
  }
}
