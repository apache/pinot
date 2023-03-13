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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()}()} API.
 *
 *  MailboxReceiveOperator receives mailbox from mailboxService from sendingStageInstances.
 *  We use sendingStageInstance to deduce mailboxId and fetch the content from mailboxService.
 *  When exchangeType is Singleton, we find the mapping mailbox for the mailboxService. If not found, use empty list.
 *  When exchangeType is non-Singleton, we pull from each instance in round-robin way to get matched mailbox content.
 */
public class MailboxReceiveOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  // TODO: Unify SUPPORTED_EXCHANGE_TYPES with MailboxSendOperator.
  private static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPES =
      ImmutableSet.of(RelDistribution.Type.BROADCAST_DISTRIBUTED, RelDistribution.Type.HASH_DISTRIBUTED,
          RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED);

  private final MailboxService<TransferableBlock> _mailboxService;
  private final RelDistribution.Type _exchangeType;
  private final List<MailboxIdentifier> _sendingMailbox;
  private final long _deadlineTimestampNano;
  private int _serverIdx;
  private TransferableBlock _upstreamErrorBlock;

  private static MailboxIdentifier toMailboxId(VirtualServer sender, long jobId, int senderStageId,
      int receiverStageId, VirtualServerAddress receiver) {
    return new JsonMailboxIdentifier(
        String.format("%s_%s", jobId, senderStageId),
        new VirtualServerAddress(sender),
        receiver,
        senderStageId,
        receiverStageId);
  }

  // TODO: Move deadlineInNanoSeconds to OperatorContext.
  public MailboxReceiveOperator(MailboxService<TransferableBlock> mailboxService,
      List<VirtualServer> sendingStageInstances, RelDistribution.Type exchangeType, VirtualServerAddress receiver,
      long jobId, int senderStageId, int receiverStageId, Long timeoutMs) {
    super(jobId, senderStageId, receiver);
    _mailboxService = mailboxService;
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPES.contains(exchangeType),
        "Exchange/Distribution type: " + exchangeType + " is not supported!");
    long timeoutNano = (timeoutMs != null ? timeoutMs : QueryConfig.DEFAULT_MAILBOX_TIMEOUT_MS) * 1_000_000L;
    _deadlineTimestampNano = timeoutNano + System.nanoTime();

    _exchangeType = exchangeType;
    if (_exchangeType == RelDistribution.Type.SINGLETON) {
      VirtualServer singletonInstance = null;
      for (VirtualServer serverInstance : sendingStageInstances) {
        if (serverInstance.getHostname().equals(_mailboxService.getHostname())
            && serverInstance.getQueryMailboxPort() == _mailboxService.getMailboxPort()) {
          Preconditions.checkState(singletonInstance == null, "multiple instance found for singleton exchange type!");
          singletonInstance = serverInstance;
        }
      }

      if (singletonInstance == null) {
        // TODO: fix WorkerManager assignment, this should not happen if we properly assign workers.
        // see: https://github.com/apache/pinot/issues/9611
        _sendingMailbox = Collections.emptyList();
      } else {
        _sendingMailbox =
            Collections.singletonList(toMailboxId(singletonInstance, jobId, senderStageId, receiverStageId, receiver));
      }
    } else {
      _sendingMailbox = new ArrayList<>(sendingStageInstances.size());
      for (VirtualServer instance : sendingStageInstances) {
        _sendingMailbox.add(toMailboxId(instance, jobId, senderStageId, receiverStageId, receiver));
      }
    }
    _upstreamErrorBlock = null;
    _serverIdx = 0;
  }

  public List<MailboxIdentifier> getSendingMailbox() {
    return _sendingMailbox;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of();
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    } else if (System.nanoTime() >= _deadlineTimestampNano) {
      return TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
    }

    int startingIdx = _serverIdx;
    int openMailboxCount = 0;
    int eosMailboxCount = 0;
    // For all non-singleton distribution, we poll from every instance to check mailbox content.
    // TODO: Fix wasted CPU cycles on waiting for servers that are not supposed to give content.
    for (int i = 0; i < _sendingMailbox.size(); i++) {
      // this implements a round-robin mailbox iterator, so we don't starve any mailboxes
      _serverIdx = (startingIdx + i) % _sendingMailbox.size();
      MailboxIdentifier mailboxId = _sendingMailbox.get(_serverIdx);
      try {
        ReceivingMailbox<TransferableBlock> mailbox = _mailboxService.getReceivingMailbox(mailboxId);
        if (!mailbox.isClosed()) {
          openMailboxCount++;
          TransferableBlock block = mailbox.receive();
          // Get null block when pulling times out from mailbox.
          if (block != null) {
            if (block.isErrorBlock()) {
              _upstreamErrorBlock =
                  TransferableBlockUtils.getErrorTransferableBlock(block.getDataBlock().getExceptions());
              return _upstreamErrorBlock;
            }
            if (!block.isEndOfStreamBlock()) {
              return block;
            } else {
              if (!block.getResultMetadata().isEmpty()) {
                _operatorStatsMap.putAll(block.getResultMetadata());
              }
              eosMailboxCount++;
            }
          }
        }
      } catch (Exception e) {
        return TransferableBlockUtils.getErrorTransferableBlock(
            new RuntimeException(String.format("Error polling mailbox=%s", mailboxId), e));
      }
    }

    // there are two conditions in which we should return EOS: (1) there were
    // no mailboxes to open (this shouldn't happen because the second condition
    // should be hit first, but is defensive) (2) every mailbox that was opened
    // returned an EOS block. in every other scenario, there are mailboxes that
    // are not yet exhausted and we should wait for more data to be available
    TransferableBlock block =
        openMailboxCount > 0 && openMailboxCount > eosMailboxCount ? TransferableBlockUtils.getNoOpTransferableBlock()
            : TransferableBlockUtils.getEndOfStreamTransferableBlock();
    return block;
  }

  @Override
  public void close() {
    super.close();
    for (MailboxIdentifier sendingMailbox : _sendingMailbox) {
      _mailboxService.releaseReceivingMailbox(sendingMailbox);
    }
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    for (MailboxIdentifier sendingMailbox : _sendingMailbox) {
      _mailboxService.releaseReceivingMailbox(sendingMailbox);
    }
  }
}
