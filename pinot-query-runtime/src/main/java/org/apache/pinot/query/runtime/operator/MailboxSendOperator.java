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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.RoutingInfo;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxSendOperator} is created to send {@link TransferableBlock}s to the receiving end.
 *
 * TODO: Add support to sort the data prior to sending if sorting is enabled
 */
public class MailboxSendOperator extends MultiStageOperator {
  public static final EnumSet<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPES =
      EnumSet.of(RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED,
          RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED);

  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_SEND";

  private final MultiStageOperator _input;
  private final BlockExchange _exchange;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  // TODO: Support sort on sender
  public MailboxSendOperator(OpChainExecutionContext context, MultiStageOperator input, MailboxSendNode node) {
    this(context, input,
        statMap -> getBlockExchange(context, node.getReceiverStageIds(), node.getDistributionType(), node.getKeys(),
            statMap));
    _statMap.merge(StatKey.STAGE, context.getStageId());
    _statMap.merge(StatKey.PARALLELISM, 1);
  }

  @VisibleForTesting
  MailboxSendOperator(OpChainExecutionContext context, MultiStageOperator input,
      Function<StatMap<StatKey>, BlockExchange> exchangeFactory) {
    super(context);
    _input = input;
    _exchange = exchangeFactory.apply(_statMap);
  }

  private static BlockExchange getBlockExchange(OpChainExecutionContext context, Iterable<Integer> receiverStageIds,
      RelDistribution.Type distributionType, List<Integer> keys, StatMap<StatKey> statMap) {
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPES.contains(distributionType), "Unsupported distribution type: %s",
        distributionType);
    MailboxService mailboxService = context.getMailboxService();
    long requestId = context.getRequestId();
    long deadlineMs = context.getDeadlineMs();

    List<RoutingInfo> routingInfos = new ArrayList<>();
    for (Integer receiverStageId : receiverStageIds) {
      List<MailboxInfo> mailboxInfos =
          context.getWorkerMetadata().getMailboxInfosMap().get(receiverStageId).getMailboxInfos();
      List<RoutingInfo> stageRoutingInfos =
          MailboxIdUtils.toRoutingInfos(requestId, context.getStageId(), context.getWorkerId(), receiverStageId,
              mailboxInfos);
      routingInfos.addAll(stageRoutingInfos);
    }
    List<SendingMailbox> sendingMailboxes = routingInfos.stream()
        .map(v -> mailboxService.getSendingMailbox(v.getHostname(), v.getPort(), v.getMailboxId(), deadlineMs, statMap))
        .collect(Collectors.toList());
    statMap.merge(StatKey.FAN_OUT, sendingMailboxes.size());
    return BlockExchange.getExchange(sendingMailboxes, distributionType, keys, TransferableBlockUtils::splitBlock);
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.MAILBOX_SEND;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.singletonList(_input);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      TransferableBlock block = _input.nextBlock();
      if (block.isSuccessfulEndOfStreamBlock()) {
        updateEosBlock(block, _statMap);
        // no need to check early terminate signal b/c the current block is already EOS
        sendTransferableBlock(block);
        // After sending its own stats, the sending operator of the stage 1 has the complete view of all stats
        // Therefore this is the only place we can update some of the metrics like total seen rows or time spent.
        if (_context.getStageId() == 1) {
          updateMetrics(block);
        }
      } else {
        if (sendTransferableBlock(block)) {
          earlyTerminate();
        }
      }
      sampleAndCheckInterruption();
      return block;
    } catch (QueryCancelledException e) {
      LOGGER.debug("Query was cancelled! for opChain: {}", _context.getId());
      return createLeafBlock();
    } catch (TimeoutException e) {
      LOGGER.warn("Timed out transferring data on opChain: {}", _context.getId(), e);
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    } catch (Exception e) {
      TransferableBlock errorBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
      try {
        LOGGER.error("Exception while transferring data on opChain: {}", _context.getId(), e);
        sendTransferableBlock(errorBlock);
      } catch (Exception e2) {
        LOGGER.error("Exception while sending error block.", e2);
      }
      return errorBlock;
    }
  }

  protected TransferableBlock createLeafBlock() {
    return TransferableBlockUtils.getEndOfStreamTransferableBlock(
        MultiStageQueryStats.createCancelledSend(_context.getStageId(), _statMap));
  }

  private boolean sendTransferableBlock(TransferableBlock block)
      throws Exception {
    boolean isEarlyTerminated = _exchange.send(block);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[SEND]== Block " + block + " sent from: " + _context.getId());
    }
    return isEarlyTerminated;
  }

  @Override
  public void close() {
    super.close();
    _exchange.close();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _exchange.cancel(t);
  }

  private void updateMetrics(TransferableBlock block) {
    ServerMetrics serverMetrics = ServerMetrics.get();
    MultiStageQueryStats queryStats = block.getQueryStats();
    if (queryStats == null) {
      LOGGER.info("Query stats not found in the EOS block.");
    } else {
      for (MultiStageQueryStats.StageStats.Closed closed : queryStats.getClosedStats()) {
        if (closed != null) {
          closed.forEach((type, stats) -> type.updateServerMetrics(stats, serverMetrics));
        }
      }
      queryStats.getCurrentStats().forEach((type, stats) -> {
        type.updateServerMetrics(stats, serverMetrics);
      });
    }
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    STAGE(StatMap.Type.INT) {
      @Override
      public int merge(int value1, int value2) {
        return StatMap.Key.eqNotZero(value1, value2);
      }

      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    /**
     * Number of parallelism of the stage this operator is the root of.
     * <p>
     * The CPU times reported by this stage will be proportional to this number.
     */
    PARALLELISM(StatMap.Type.INT),
    /**
     * How many receive mailboxes are being written by this send operator.
     */
    FAN_OUT(StatMap.Type.INT) {
      @Override
      public int merge(int value1, int value2) {
        return Math.max(value1, value2);
      }
    },
    /**
     * How many messages have been sent in heap format by this mailbox.
     * <p>
     * The lower the relation between RAW_MESSAGES and IN_MEMORY_MESSAGES, the more efficient the exchange is.
     */
    IN_MEMORY_MESSAGES(StatMap.Type.INT),
    /**
     * How many messages have been sent in raw format and therefore serialized by this mailbox.
     * <p>
     * The higher the relation between RAW_MESSAGES and IN_MEMORY_MESSAGES, the less efficient the exchange is.
     */
    RAW_MESSAGES(StatMap.Type.INT),
    /**
     * How many bytes have been serialized by this mailbox.
     * <p>
     * A high number here indicates that the mailbox is sending a lot of data to other servers.
     */
    SERIALIZED_BYTES(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    /**
     * How long (in CPU time) it took to serialize the raw messages sent by this mailbox.
     */
    SERIALIZATION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    };
    //@formatter:on

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
