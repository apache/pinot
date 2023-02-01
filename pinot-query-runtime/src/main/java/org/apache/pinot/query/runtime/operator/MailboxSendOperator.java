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
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxSendOperator} is created to send {@link TransferableBlock}s to the receiving end.
 */
public class MailboxSendOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendOperator.class);

  private static final String EXPLAIN_NAME = "MAILBOX_SEND";
  private static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPE =
      ImmutableSet.of(RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED,
          RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED);

  private final MultiStageOperator _dataTableBlockBaseOperator;
  private final BlockExchange _exchange;
  private OperatorStats _operatorStats;

  @VisibleForTesting
  interface BlockExchangeFactory {
    BlockExchange build(MailboxService<TransferableBlock> mailboxService, List<MailboxIdentifier> destinations,
        RelDistribution.Type exchange, KeySelector<Object[], Object[]> selector, BlockSplitter splitter);
  }

  @VisibleForTesting
  interface MailboxIdGenerator {
    MailboxIdentifier generate(VirtualServer server);
  }

  public MailboxSendOperator(MailboxService<TransferableBlock> mailboxService,
      MultiStageOperator dataTableBlockBaseOperator, List<VirtualServer> receivingStageInstances,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> keySelector,
      VirtualServerAddress sendingServer, long jobId, int stageId) {
    this(mailboxService, dataTableBlockBaseOperator, receivingStageInstances, exchangeType, keySelector,
        server -> toMailboxId(server, jobId, stageId, sendingServer), BlockExchange::getExchange, jobId, stageId);
  }

  @VisibleForTesting
  MailboxSendOperator(MailboxService<TransferableBlock> mailboxService,
      MultiStageOperator dataTableBlockBaseOperator, List<VirtualServer> receivingStageInstances,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> keySelector,
      MailboxIdGenerator mailboxIdGenerator, BlockExchangeFactory blockExchangeFactory, long jobId, int stageId) {
    _dataTableBlockBaseOperator = dataTableBlockBaseOperator;

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
    _exchange = blockExchangeFactory.build(mailboxService, receivingMailboxes, exchangeType, keySelector, splitter);

    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPE.contains(exchangeType),
        String.format("Exchange type '%s' is not supported yet", exchangeType));
    _operatorStats = new OperatorStats(jobId, stageId, EXPLAIN_NAME);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_dataTableBlockBaseOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    _dataTableBlockBaseOperator.toExplainString();
    LOGGER.debug(_operatorStats.toString());
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    _operatorStats.startTimer();
    TransferableBlock transferableBlock;
    try {
      _operatorStats.endTimer();
      transferableBlock = _dataTableBlockBaseOperator.nextBlock();
      _operatorStats.startTimer();
      while (!transferableBlock.isNoOpBlock()) {
        _exchange.send(transferableBlock);
        _operatorStats.recordInput(1, transferableBlock.getNumRows());
        // The # of output block is not accurate because we may do a split in exchange send.
        _operatorStats.recordOutput(1, transferableBlock.getNumRows());
        if (transferableBlock.isEndOfStreamBlock()) {
          return transferableBlock;
        }
        _operatorStats.endTimer();
        transferableBlock = _dataTableBlockBaseOperator.nextBlock();
        _operatorStats.startTimer();
      }
    } catch (final Exception e) {
      // ideally, MailboxSendOperator doesn't ever throw an exception because
      // it will just get swallowed, in this scenario at least we can forward
      // any upstream exceptions as an error  block
      transferableBlock = TransferableBlockUtils.getErrorTransferableBlock(e);
      try {
        _exchange.send(transferableBlock);
      } catch (Exception e2) {
        LOGGER.error("Exception while sending block to mailbox.", e2);
      }
    } finally {
      _operatorStats.endTimer();
    }
    return transferableBlock;
  }

  private static JsonMailboxIdentifier toMailboxId(
      VirtualServer destination, long jobId, int stageId, VirtualServerAddress sender) {
    return new JsonMailboxIdentifier(
        String.format("%s_%s", jobId, stageId),
        sender,
        new VirtualServerAddress(destination));
  }
}
