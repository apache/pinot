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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InMemorySendingMailbox implements SendingMailbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemorySendingMailbox.class);

  private final String _id;
  private final MailboxService _mailboxService;
  private final long _deadlineMs;

  private ReceivingMailbox _receivingMailbox;
  private volatile boolean _isTerminated;
  private volatile boolean _isEarlyTerminated;
  private final StatMap<MailboxSendOperator.StatKey> _statMap;

  public InMemorySendingMailbox(String id, MailboxService mailboxService, long deadlineMs,
      StatMap<MailboxSendOperator.StatKey> statMap) {
    _id = id;
    _mailboxService = mailboxService;
    _deadlineMs = deadlineMs;
    _statMap = statMap;
  }

  @Override
  public boolean isLocal() {
    return true;
  }

  @Override
  public void send(MseBlock.Data data)
      throws IOException, TimeoutException {
    sendPrivate(data, Collections.emptyList());
  }

  @Override
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats)
      throws IOException, TimeoutException {
    sendPrivate(block, serializedStats);
  }

  private void sendPrivate(MseBlock block, List<DataBuffer> serializedStats)
      throws TimeoutException {
    if (isTerminated() || (isEarlyTerminated() && block.isData())) {
      return;
    }
    if (_receivingMailbox == null) {
      _receivingMailbox = _mailboxService.getReceivingMailbox(_id);
    }
    _statMap.merge(MailboxSendOperator.StatKey.IN_MEMORY_MESSAGES, 1);
    long timeoutMs = _deadlineMs - System.currentTimeMillis();
    ReceivingMailbox.ReceivingMailboxStatus status = _receivingMailbox.offer(block, serializedStats, timeoutMs);

    switch (status) {
      case SUCCESS:
        break;
      case CANCELLED:
        throw new QueryCancelledException(String.format("Mailbox: %s already cancelled from upstream", _id));
      case ERROR:
        throw new QueryException(QueryErrorCode.ERRORED_OUT, String.format(
            "Mailbox: %s already errored out (received error block before)", _id));
      case TIMEOUT:
        throw new QueryException(QueryErrorCode.EXECUTION_TIMEOUT,
            String.format("Timed out adding block into mailbox: %s with timeout: %dms", _id, timeoutMs));
      case EARLY_TERMINATED:
        _isEarlyTerminated = true;
        break;
      default:
        throw new IllegalStateException("Unsupported mailbox status: " + status);
    }
  }

  @Override
  public void complete() {
    _isTerminated = true;
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
    _receivingMailbox.setErrorBlock(
        ErrorMseBlock.fromException(new QueryCancelledException(
            "Cancelled by sender with exception: " + t.getMessage())), Collections.emptyList());
  }

  @Override
  public boolean isEarlyTerminated() {
    return _isEarlyTerminated;
  }

  @Override
  public boolean isTerminated() {
    return _isTerminated;
  }

  @Override
  public String toString() {
    return "m" + _id;
  }
}
