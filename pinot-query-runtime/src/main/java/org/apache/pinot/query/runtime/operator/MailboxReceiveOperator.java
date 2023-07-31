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

import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()} API.
 */
public class MailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";

  private TransferableBlock _errorBlock;

  public MailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType, int senderStageId) {
    super(context, exchangeType, senderStageId);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    while (!_mailboxes.isEmpty()) {
      if (_errorBlock != null) {
        return _errorBlock;
      }
      if (System.currentTimeMillis() > _context.getDeadlineMs()) {
        _errorBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
        return _errorBlock;
      }

      // Poll from every mailbox in round-robin fashion:
      // - Return the first content block
      // - If no content block found but there are mailboxes not finished, return no-op block
      // - If all content blocks are already returned, return end-of-stream block
      LOGGER.trace("==[RECEIVE]== Enter getNextBlock from: " + _context.getId() + " mailboxSize: " + _mailboxes.size());
      int numMailboxes = _mailboxes.size();
      for (int i = 0; i < numMailboxes; i++) {
        ReceivingMailbox mailbox = _mailboxes.remove();
        TransferableBlock block = mailbox.poll();

        // Release the mailbox when the block is end-of-stream
        if (block != null && block.isSuccessfulEndOfStreamBlock()) {
          LOGGER.debug("==[RECEIVE]== EOS received : " + _context.getId() + " in mailbox: " + mailbox.getId());
          _mailboxService.releaseReceivingMailbox(mailbox);
          _opChainStats.getOperatorStatsMap().putAll(block.getResultMetadata());
          continue;
        }

        // Add the mailbox back to the queue if the block is not end-of-stream
        _mailboxes.add(mailbox);
        if (block != null) {
          if (block.isErrorBlock()) {
            _errorBlock = block;
          }
          LOGGER.trace("==[RECEIVE]== Returned block from : " + _context.getId() + " in mailbox: " + mailbox.getId());
          return block;
        }
      }
      if (_mailboxes.isEmpty()) {
        LOGGER.debug("==[RECEIVE]== Finished : " + _context.getId());
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      } else {
        LOGGER.debug("==[RECEIVE]== Yield : " + _context.getId());
        _context.yield();
      }
    }
    if (System.currentTimeMillis() > _context.getDeadlineMs()) {
      _errorBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
      return _errorBlock;
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
  }
}
