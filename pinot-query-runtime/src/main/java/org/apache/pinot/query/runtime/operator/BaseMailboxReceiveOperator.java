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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class to be used by the various MailboxReceiveOperators such as the sorted and non-sorted versions. This
 * class contains the common logic needed for MailboxReceive
 *
 * BaseMailboxReceiveOperator receives mailbox from mailboxService from sendingStageInstances.
 * We use sendingStageInstance to deduce mailboxId and fetch the content from mailboxService.
 * When exchangeType is Singleton, we find the mapping mailbox for the mailboxService. If not found, use empty list.
 * When exchangeType is non-Singleton, we pull from each instance in round-robin way to get matched mailbox content.
 */
public abstract class BaseMailboxReceiveOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMailboxReceiveOperator.class);
  protected final MailboxService _mailboxService;
  protected final RelDistribution.Type _exchangeType;
  protected final List<String> _mailboxIds;
  protected final List<ReceivingMailbox> _mailboxes;
  protected final ReentrantLock _lock = new ReentrantLock(false);
  protected final Condition _notEmpty = _lock.newCondition();
  protected int _lastRead;
  private TransferableBlock _errorBlock = null;

  public BaseMailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      int senderStageId) {
    super(context);
    _mailboxService = context.getMailboxService();
    Preconditions.checkState(MailboxSendOperator.SUPPORTED_EXCHANGE_TYPES.contains(exchangeType),
        "Unsupported exchange type: %s", exchangeType);
    _exchangeType = exchangeType;

    long requestId = context.getRequestId();
    int workerId = context.getServer().workerId();
    MailboxMetadata senderMailBoxMetadatas =
        context.getStageMetadata().getWorkerMetadataList().get(workerId).getMailBoxInfosMap().get(senderStageId);
    Preconditions.checkState(senderMailBoxMetadatas != null && !senderMailBoxMetadatas.getMailBoxIdList().isEmpty(),
        "Failed to find mailbox for stage: %s",
        senderStageId);
    _mailboxIds = MailboxIdUtils.toMailboxIds(requestId, senderMailBoxMetadatas);
    _mailboxes = new ArrayList<>(_mailboxIds.size());
    for (String mailboxId : _mailboxIds) {
      _mailboxes.add(_mailboxService.getReceivingMailbox(mailboxId, this));
    }
    _lastRead = _mailboxes.size() - 1;
  }

  public List<String> getMailboxIds() {
    return _mailboxIds;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public void close() {
    super.close();
    cancelRemainingMailboxes();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    cancelRemainingMailboxes();
  }

  protected void cancelRemainingMailboxes() {
    for (ReceivingMailbox mailbox : _mailboxes) {
      mailbox.cancel();
    }
  }

  public void onData() {
    _lock.lock();
    try {
      _notEmpty.signal();
    } finally {
      _lock.unlock();
    }
  }

  protected TransferableBlock readBlockBlocking() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("==[RECEIVE]== Enter getNextBlock from: " + _context.getId() + " mailboxSize: " + _mailboxes.size());
    }
    TransferableBlock block = readDroppingSuccessEos();
    if (block == null) {
      boolean timeout = false;
      _lock.lock();
      try {
        block = readDroppingSuccessEos();
        while (block == null && !timeout) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("==[RECEIVE]== Yield : " + _context.getId());
          }
          timeout = !_notEmpty.await(_context.getDeadlineMs() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          block = readDroppingSuccessEos();
        }
        if (timeout) {
          _errorBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
          return _errorBlock;
        }
      } catch (InterruptedException ex) {
        return TransferableBlockUtils.getErrorTransferableBlock(ex);
      } finally {
        _lock.unlock();
      }
    }
    return block;
  }

  @Nullable
  private TransferableBlock readDroppingSuccessEos() {
    if (System.currentTimeMillis() > _context.getDeadlineMs()) {
      _errorBlock = TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
      return _errorBlock;
    }

    TransferableBlock block = readBlockOrNull();
    while (block != null && block.isSuccessfulEndOfStreamBlock() && !_mailboxes.isEmpty()) {
      ReceivingMailbox removed = _mailboxes.remove(_lastRead);
      _lastRead--;
      _mailboxService.releaseReceivingMailbox(removed);
      _opChainStats.getOperatorStatsMap().putAll(block.getResultMetadata());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[RECEIVE]== EOS received : " + _context.getId() + " in mailbox: " + removed.getId());
      }

      block = readBlockOrNull();
    }
    if (_mailboxes.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[RECEIVE]== Finished : " + _context.getId());
      }
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    if (block != null) {
      if (LOGGER.isTraceEnabled()) {
        ReceivingMailbox mailbox = _mailboxes.get(_lastRead);
        LOGGER.trace("==[RECEIVE]== Returned block from : " + _context.getId() + " in mailbox: " + mailbox.getId());
      }
      if (block.isErrorBlock()) {
        _errorBlock = block;
      }
    }
    return block;
  }

  @Nullable
  private TransferableBlock readBlockOrNull() {
    for (int i = _lastRead + 1; i < _mailboxes.size(); i++) {
      ReceivingMailbox mailbox = _mailboxes.get(i);
      TransferableBlock block = mailbox.poll();
      if (block != null) {
        _lastRead = i;
        return block;
      }
    }
    for (int i = 0; i <= _lastRead; i++) {
      ReceivingMailbox mailbox = _mailboxes.get(i);
      TransferableBlock block = mailbox.poll();
      if (block != null) {
        _lastRead = i;
        return block;
      }
    }
    return null;
  }
}
