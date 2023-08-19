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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxSendOperator} is created to send {@link TransferableBlock}s to the receiving end.
 *
 * TODO: Add support to sort the data prior to sending if sorting is enabled
 */
public class MailboxSendOperator extends MultiStageOperator {
  public static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPES =
      ImmutableSet.of(RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED,
          RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED);

  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_SEND";

  private final MultiStageOperator _sourceOperator;
  private final BlockExchange _exchange;
  private final List<RexExpression> _collationKeys;
  private final List<RelFieldCollation.Direction> _collationDirections;
  private final boolean _isSortOnSender;

  public MailboxSendOperator(OpChainExecutionContext context, MultiStageOperator sourceOperator,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> keySelector,
      @Nullable List<RexExpression> collationKeys, @Nullable List<RelFieldCollation.Direction> collationDirections,
      boolean isSortOnSender, int receiverStageId) {
    this(context, sourceOperator, getBlockExchange(context, exchangeType, keySelector, receiverStageId), collationKeys,
        collationDirections, isSortOnSender);
  }

  @VisibleForTesting
  MailboxSendOperator(OpChainExecutionContext context, MultiStageOperator sourceOperator, BlockExchange exchange,
      @Nullable List<RexExpression> collationKeys, @Nullable List<RelFieldCollation.Direction> collationDirections,
      boolean isSortOnSender) {
    super(context);
    _sourceOperator = sourceOperator;
    _exchange = exchange;
    _collationKeys = collationKeys;
    _collationDirections = collationDirections;
    _isSortOnSender = isSortOnSender;
    _context.getMailboxService().submitExchangeRequest(context.getId(), exchange);
  }

  private static BlockExchange getBlockExchange(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      KeySelector<Object[], Object[]> keySelector, int receiverStageId) {
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPES.contains(exchangeType), "Unsupported exchange type: %s",
        exchangeType);
    MailboxService mailboxService = context.getMailboxService();
    long requestId = context.getRequestId();
    long deadlineMs = context.getDeadlineMs();

    int workerId = context.getServer().workerId();
    MailboxMetadata receiverMailboxMetadatas =
        context.getStageMetadata().getWorkerMetadataList().get(workerId).getMailBoxInfosMap().get(receiverStageId);
    List<String> sendingMailboxIds = MailboxIdUtils.toMailboxIds(requestId, receiverMailboxMetadatas);
    List<SendingMailbox> sendingMailboxes = new ArrayList<>(sendingMailboxIds.size());
    for (int i = 0; i < receiverMailboxMetadatas.getMailBoxIdList().size(); i++) {
      sendingMailboxes.add(mailboxService.getSendingMailbox(receiverMailboxMetadatas.getVirtualAddress(i).hostname(),
          receiverMailboxMetadatas.getVirtualAddress(i).port(), sendingMailboxIds.get(i), deadlineMs));
    }
    return BlockExchange.getExchange(context.getId(), sendingMailboxes, exchangeType, keySelector,
        TransferableBlockUtils::splitBlock, context.getCallback(), context.getDeadlineMs());
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.singletonList(_sourceOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      TransferableBlock block = _sourceOperator.nextBlock();
      if (block.isErrorBlock()) {
        sendTransferableBlock(block);
        return block;
      } else if (block.isSuccessfulEndOfStreamBlock()) {
        // Stats need to be populated here because the block is being sent to the mailbox
        // and the receiving opChain will not be able to access the stats from the previous opChain
        TransferableBlock eosBlockWithStats = TransferableBlockUtils.getEndOfStreamTransferableBlock(
            OperatorUtils.getMetadataFromOperatorStats(_opChainStats.getOperatorStatsMap()));
        sendTransferableBlock(eosBlockWithStats);
        return block;
      } else {
        // Data block
        sendTransferableBlock(block);
        // Yield if we cannot continue to put transferable block into the sending queue
        return block;
      }
    } catch (EarlyTerminationException e) {
      // TODO: Query stats are not sent when opChain is early terminated
      LOGGER.debug("Early terminating opChain: {}", _context.getId());
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
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

  private void sendTransferableBlock(TransferableBlock block)
      throws Exception {
    long timeoutMs = _context.getDeadlineMs() - System.currentTimeMillis();
    boolean success = false;
    try {
      if (!_exchange.offerBlock(block, timeoutMs)) {
        throw new TimeoutException(
            String.format("Timed out while offering block into the sending queue after %dms", timeoutMs));
      }
      success = true;
    } finally {
      if (success) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("==[SEND]== Block " + block + " correctly sent from: " + _context.getId());
        }
      } else {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("==[SEND]== Block " + block + " cannot be sent from: " + _context.getId());
        }
      }
    }
  }

  /**
   * This method is overridden to return true because this operator is last in the chain and needs to collect
   * execution time stats
   */
  @Override
  protected boolean shouldCollectStats() {
    return true;
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
}
