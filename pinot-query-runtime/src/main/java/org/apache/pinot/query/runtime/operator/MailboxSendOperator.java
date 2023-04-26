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
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
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
  }

  private static BlockExchange getBlockExchange(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      KeySelector<Object[], Object[]> keySelector, int receiverStageId) {
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPES.contains(exchangeType), "Unsupported exchange type: %s",
        exchangeType);
    MailboxService mailboxService = context.getMailboxService();
    long requestId = context.getRequestId();
    int senderStageId = context.getStageId();
    int senderWorkerId = context.getServer().workerId();
    long deadlineMs = context.getDeadlineMs();
    List<WorkerMetadata> receivingMetadataList = context.getStageMetadataList().get(receiverStageId)
        .getWorkerMetadataList();
    List<SendingMailbox> sendingMailboxes;
    if (exchangeType == RelDistribution.Type.SINGLETON) {
      // TODO: this logic should be moved into SingletonExchange
      VirtualServerAddress singletonAddress = null;
      for (WorkerMetadata receivingMetadata : receivingMetadataList) {
        VirtualServerAddress receiver = receivingMetadata.getVirtualServerAddress();
        if (receiver.hostname().equals(mailboxService.getHostname())
            && receiver.port() == mailboxService.getPort()) {
          Preconditions.checkState(singletonAddress == null, "Multiple instances found for SINGLETON exchange type");
          singletonAddress = receiver;
        }
      }
      Preconditions.checkState(singletonAddress != null, "Failed to find instance for SINGLETON exchange type");
      String mailboxId = MailboxIdUtils.toMailboxId(requestId, senderStageId, senderWorkerId, receiverStageId,
          singletonAddress.workerId());
      sendingMailboxes = Collections.singletonList(
          mailboxService.getSendingMailbox(singletonAddress.hostname(), singletonAddress.port(), mailboxId,
              deadlineMs));
    } else {
      sendingMailboxes = new ArrayList<>(receivingMetadataList.size());
      for (WorkerMetadata receivingMetadata : receivingMetadataList) {
        VirtualServerAddress receiver = receivingMetadata.getVirtualServerAddress();
        String mailboxId =
            MailboxIdUtils.toMailboxId(requestId, senderStageId, senderWorkerId, receiverStageId, receiver.workerId());
        sendingMailboxes.add(
            mailboxService.getSendingMailbox(receiver.hostname(), receiver.port(), mailboxId, deadlineMs));
      }
    }
    return BlockExchange.getExchange(sendingMailboxes, exchangeType, keySelector, TransferableBlockUtils::splitBlock);
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
    TransferableBlock transferableBlock;
    try {
      transferableBlock = _sourceOperator.nextBlock();
      while (!transferableBlock.isNoOpBlock()) {
        if (transferableBlock.isEndOfStreamBlock()) {
          if (transferableBlock.isSuccessfulEndOfStreamBlock()) {
            //Stats need to be populated here because the block is being sent to the mailbox
            // and the receiving opChain will not be able to access the stats from the previous opChain
            TransferableBlock eosBlockWithStats = TransferableBlockUtils.getEndOfStreamTransferableBlock(
                OperatorUtils.getMetadataFromOperatorStats(_opChainStats.getOperatorStatsMap()));
            _exchange.send(eosBlockWithStats);
          } else {
            _exchange.send(transferableBlock);
          }
          return transferableBlock;
        }
        _exchange.send(transferableBlock);
        transferableBlock = _sourceOperator.nextBlock();
      }
    } catch (Exception e) {
      transferableBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
      try {
        _exchange.send(transferableBlock);
      } catch (Exception e2) {
        LOGGER.error("Exception while sending block to mailbox.", e2);
      }
    }
    return transferableBlock;
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
