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
  private volatile boolean _isTerminated;

  public InMemorySendingMailbox(String id, MailboxService mailboxService, long deadlineMs) {
    _id = id;
    _mailboxService = mailboxService;
    _deadlineMs = deadlineMs;
  }

  @Override
  public void send(TransferableBlock block) {
    if (_isTerminated) {
      return;
    }
    if (_receivingMailbox == null) {
      _receivingMailbox = _mailboxService.getReceivingMailbox(_id);
    }
    long timeoutMs = _deadlineMs - System.currentTimeMillis();
    ReceivingMailbox.ReceivingMailboxStatus status = _receivingMailbox.offer(block, timeoutMs);
    switch (status) {
      case SUCCESS:
        break;
      case ERROR:
        throw new RuntimeException(String.format("Mailbox: %s already errored out (received error block before)", _id));
      case TIMEOUT:
        throw new RuntimeException(
            String.format("Timed out adding block into mailbox: %s with timeout: %dms", _id, timeoutMs));
      case EARLY_TERMINATED:
        _isTerminated = true;
        break;
      default:
        throw new IllegalStateException("Unsupported mailbox status: " + status);
    }
  }

  @Override
  public void complete() {
  }

  @Override
  public void cancel(Throwable t) {
    if (_isTerminated) {
      return;
    }
    LOGGER.debug("Cancelling mailbox: {}", _id);
    if (_receivingMailbox == null) {
      _receivingMailbox = _mailboxService.getReceivingMailbox(_id);
    }
    _receivingMailbox.setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(
        new RuntimeException("Cancelled by sender with exception: " + t.getMessage(), t)));
  }

  @Override
  public boolean isTerminated() {
    return _isTerminated;
  }
}
