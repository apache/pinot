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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendOperator.class);

  private static final String EXPLAIN_NAME = "MAILBOX_SEND";
  private static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPE =
      ImmutableSet.of(RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED,
          RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED);

  private final MultiStageOperator _dataTableBlockBaseOperator;
  private final BlockExchange _exchange;
  private final List<RexExpression> _collationKeys;
  private final List<RelFieldCollation.Direction> _collationDirections;
  private final boolean _isSortOnSender;

  @VisibleForTesting
  interface BlockExchangeFactory {
    BlockExchange build(MailboxService<TransferableBlock> mailboxService, List<MailboxIdentifier> destinations,
        RelDistribution.Type exchange, KeySelector<Object[], Object[]> selector, BlockSplitter splitter,
        long deadlineMs);
  }

  @VisibleForTesting
  interface MailboxIdGenerator {
    MailboxIdentifier generate(VirtualServer server);
  }

  public MailboxSendOperator(OpChainExecutionContext context, MultiStageOperator dataTableBlockBaseOperator,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> keySelector, List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections, boolean isSortOnSender, int senderStageId,
      int receiverStageId) {
    this(context, dataTableBlockBaseOperator, exchangeType, keySelector, collationKeys, collationDirections,
        isSortOnSender,
        (server) -> toMailboxId(server, context.getRequestId(), senderStageId, receiverStageId, context.getServer()),
        BlockExchange::getExchange, receiverStageId);
  }

  @VisibleForTesting
  MailboxSendOperator(OpChainExecutionContext context, MultiStageOperator dataTableBlockBaseOperator,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> keySelector, List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections, boolean isSortOnSender,
      MailboxIdGenerator mailboxIdGenerator, BlockExchangeFactory blockExchangeFactory, int receiverStageId) {
    super(context);
    _dataTableBlockBaseOperator = dataTableBlockBaseOperator;
    MailboxService<TransferableBlock> mailboxService = context.getMailboxService();
    List<VirtualServer> receivingStageInstances =
        context.getMetadataMap().get(receiverStageId).getServerInstances();
    List<MailboxIdentifier> receivingMailboxes;
    if (exchangeType == RelDistribution.Type.SINGLETON) {
      // TODO: this logic should be moved into SingletonExchange
      VirtualServer singletonInstance = null;
      for (VirtualServer serverInstance : receivingStageInstances) {
        if (serverInstance.getHostname().equals(mailboxService.getHostname())
            && serverInstance.getQueryMailboxPort() == mailboxService.getMailboxPort()) {
          if (singletonInstance != null && singletonInstance.getServer().equals(serverInstance.getServer())) {
            throw new IllegalArgumentException("Cannot issue query with stageParallelism > 1 for queries that "
                + "use SINGLETON exchange. This is an internal limitation that is being worked on - reissue "
                + "your query again without stageParallelism.");
          }
          Preconditions.checkState(singletonInstance == null, "multiple instance found for singleton exchange type!");
          singletonInstance = serverInstance;
        }
      }
      Preconditions.checkNotNull(singletonInstance, "Unable to find receiving instance for singleton exchange");
      receivingMailboxes = Collections.singletonList(mailboxIdGenerator.generate(singletonInstance));
    } else {
      receivingMailboxes = receivingStageInstances
          .stream()
          .map(mailboxIdGenerator::generate)
          .collect(Collectors.toList());
    }

    BlockSplitter splitter = TransferableBlockUtils::splitBlock;
    _exchange =
        blockExchangeFactory.build(context.getMailboxService(), receivingMailboxes, exchangeType, keySelector, splitter,
            context.getDeadlineMs());

    _collationKeys = collationKeys;
    _collationDirections = collationDirections;
    _isSortOnSender = isSortOnSender;

    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPE.contains(exchangeType),
        String.format("Exchange type '%s' is not supported yet", exchangeType));
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_dataTableBlockBaseOperator);
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
      transferableBlock = _dataTableBlockBaseOperator.nextBlock();
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
        transferableBlock = _dataTableBlockBaseOperator.nextBlock();
      }
    } catch (final Exception e) {
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

  private static JsonMailboxIdentifier toMailboxId(
      VirtualServer destination, long jobId, int senderStageId, int receiverStageId, VirtualServerAddress sender) {
    return new JsonMailboxIdentifier(
        String.format("%s_%s", jobId, senderStageId),
        sender,
        new VirtualServerAddress(destination), senderStageId, receiverStageId);
  }
}
