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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
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

  /// Set to true when the send operation completes calling [#complete()]
  private volatile boolean _isTerminated;

  /// Set to true when the receiver waits for EOS but discards any further data blocks.
  /// This can happen when the receiver has already early terminated, for example,
  /// when the [org.apache.pinot.query.runtime.operator.SortOperator] limit has been reached.
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
  public void send(MseBlock.Data data) {
    sendPrivate(data, Collections.emptyList());
  }

  @Override
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats) {
    sendPrivate(block, serializedStats);
    _isTerminated = true;
  }

  private void sendPrivate(MseBlock block, List<DataBuffer> serializedStats) {
    if (isTerminated() || (isEarlyTerminated() && block.isData())) {
      LOGGER.debug("Mailbox {} already terminated, ignoring block {}", _id, block);
      return;
    }
    if (_receivingMailbox == null) {
      _receivingMailbox = _mailboxService.getReceivingMailbox(_id);
    }
    _statMap.merge(MailboxSendOperator.StatKey.IN_MEMORY_MESSAGES, 1);
    long timeoutMs = _deadlineMs - System.currentTimeMillis();
    ReceivingMailbox.ReceivingMailboxStatus status;
    try {
      status = _receivingMailbox.offer(block, serializedStats, timeoutMs);
    } catch (InterruptedException e) {
      // We are not restoring the interrupt status because we are already throwing an exception
      // Code that catches this exception must finish the work fast enough to comply the interrupt contract
      // See https://github.com/apache/pinot/pull/16903#discussion_r2409003423
      throw new QueryException(QueryErrorCode.INTERNAL, "Interrupted while sending data to mailbox: " + _id, e);
    } catch (TimeoutException e) {
      throw new QueryException(QueryErrorCode.EXECUTION_TIMEOUT, "Timed out adding block into mailbox: " + _id
          + " with timeout: " + Duration.of(timeoutMs, ChronoUnit.MILLIS), e);
    }
    switch (status) {
      case SUCCESS:
        break;
      case WAITING_EOS:
        _isEarlyTerminated = true;
        break;
      case LAST_BLOCK:
        _isTerminated = true;
        break;
      case ALREADY_TERMINATED:
        LOGGER.error("Trying to offer blocks to the already closed mailbox {}. This should not happen", _id);
        break;
      default:
        throw new IllegalStateException("Unsupported mailbox status: " + status);
    }
  }

  @Override
  public void cancel(Throwable t) {
    if (_isTerminated) {
      return;
    }
    _isTerminated = true;
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

  @Override
  public void close() {
    if (!isTerminated()) {
      String msg = "Closing in-memory mailbox without proper EOS message";
      LOGGER.warn(msg);
      cancel(new RuntimeException(msg).fillInStackTrace());
    }
  }
}
