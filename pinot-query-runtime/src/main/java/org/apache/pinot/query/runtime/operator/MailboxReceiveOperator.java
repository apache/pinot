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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()}()} API.
 */
public class MailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  public MailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType, int senderStageId,
      int receiverStageId) {
    this(context, context.getMetadataMap().get(senderStageId).getServerInstances(), exchangeType, senderStageId,
        receiverStageId, context.getTimeoutMs());
  }

  // TODO: Move deadlineInNanoSeconds to OperatorContext.
  // TODO: Remove boxed timeoutMs value from here and use long deadlineMs from context.
  public MailboxReceiveOperator(OpChainExecutionContext context, List<VirtualServer> sendingStageInstances,
      RelDistribution.Type exchangeType, int senderStageId, int receiverStageId, Long timeoutMs) {
    super(context, sendingStageInstances, exchangeType, senderStageId, receiverStageId, timeoutMs);
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
              if (_opChainStats != null && !block.getResultMetadata().isEmpty()) {
                for (Map.Entry<String, OperatorStats> entry : block.getResultMetadata().entrySet()) {
                  _opChainStats.getOperatorStatsMap().compute(entry.getKey(), (_key, _value) -> entry.getValue());
                }
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
}
