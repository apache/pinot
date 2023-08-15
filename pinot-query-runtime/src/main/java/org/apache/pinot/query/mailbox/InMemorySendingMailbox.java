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
package org.apache.pinot.query.mailbox;

import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InMemorySendingMailbox implements SendingMailbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcSendingMailbox.class);

  private final String _id;
  private final MailboxService _mailboxService;
  private final long _deadlineMs;

  private ReceivingMailbox _receivingMailbox;

  private boolean _isEarlyTerminated = false;

  public InMemorySendingMailbox(String id, MailboxService mailboxService, long deadlineMs) {
    _id = id;
    _mailboxService = mailboxService;
    _deadlineMs = deadlineMs;
  }

  @Override
  public void send(TransferableBlock block) {
    if (_isEarlyTerminated) {
      return;
    }
    if (_receivingMailbox == null) {
      _receivingMailbox = _mailboxService.getReceivingMailbox(_id);
    }
    long timeoutMs = _deadlineMs - System.currentTimeMillis();
    ReceivingMailbox.ReceivingMailboxStatus receivingMailboxStatus = _receivingMailbox.offer(block, timeoutMs);
    switch (receivingMailboxStatus) {
      case EARLY_TERMINATED:
        _isEarlyTerminated = true;
        break;
      case TIMEOUT:
      case ERROR:
        throw new RuntimeException(
            String.format("Failed to offer block into mailbox: %s within: %dms", _id, timeoutMs));
      case SUCCESS:
        break;
      default:
        throw new IllegalStateException("Unsupported mailbox status: " + receivingMailboxStatus);
    }
  }

  @Override
  public void complete() {
  }

  @Override
  public void cancel(Throwable t) {
    LOGGER.debug("Cancelling mailbox: {}", _id);
    if (_receivingMailbox == null) {
      _receivingMailbox = _mailboxService.getReceivingMailbox(_id);
    }
    if (_isEarlyTerminated) {
      _receivingMailbox.cancel();
    } else {
      _receivingMailbox.setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(
          new RuntimeException("Cancelled by sender with exception: " + t.getMessage(), t)));
    }
  }
}
