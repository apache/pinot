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
package org.apache.pinot.query.mailbox.channel;

import com.google.common.base.Preconditions;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nullable;
import org.apache.pinot.query.mailbox.InMemoryMailboxService;
import org.apache.pinot.query.mailbox.InMemoryReceivingMailbox;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


public class InMemoryTransferStream {

  private MailboxIdentifier _mailboxId;
  private BlockingQueue<TransferableBlock> _queue;
  private InMemoryMailboxService _mailboxService;
  private final long _deadlineMs;
  private boolean _receivingMailboxInitialized = false;
  private boolean _isCancelled = false;
  private boolean _isCompleted = false;

  public InMemoryTransferStream(MailboxIdentifier mailboxId, InMemoryMailboxService mailboxService, long deadlineMs) {
    _mailboxId = mailboxId;
    _queue = new LinkedBlockingQueue<>();
    _mailboxService = mailboxService;
    _deadlineMs = deadlineMs;
  }

  public void send(TransferableBlock block) {
    Preconditions.checkState(!isCancelled(), "Tried to send on a cancelled InMemory stream");
    // TODO: Deadline check can be more efficient.
    // While, in most cases the receiver would have anyways called cancel, for expensive queries it is possible that
    // the receiver may have hung-up before it could get a reference to the stream. This can happen if the sending
    // OpChain was running an expensive operation (like a large hash-join).
    long currentTime = System.currentTimeMillis();
    Preconditions.checkState(currentTime < _deadlineMs,
        String.format("Deadline exceeded by %s ms", currentTime - _deadlineMs));
    if (!_receivingMailboxInitialized) {
      InMemoryReceivingMailbox receivingMailbox =
          (InMemoryReceivingMailbox) _mailboxService.getReceivingMailbox(_mailboxId);
      receivingMailbox.init(this);
      _receivingMailboxInitialized = true;
    }
    _queue.offer(block);
  }

  @Nullable
  public TransferableBlock poll()
      throws InterruptedException {
    if (_isCancelled) {
      return TransferableBlockUtils.getErrorTransferableBlock(
          new RuntimeException("InMemoryTransferStream is cancelled"));
    } else if (System.currentTimeMillis() > _deadlineMs) {
      return TransferableBlockUtils.getErrorTransferableBlock(
          new RuntimeException("Deadline reached for in-memory transfer stream"));
    }
    return _queue.poll();
  }

  public void complete() {
    _isCompleted = true;
  }

  public int size() {
    return _queue.size();
  }

  public void cancel() {
    _isCancelled = true;
    // Eagerly lose references to the underlying data.
    _queue.clear();
  }

  public boolean isCompleted() {
    return _isCompleted;
  }

  public boolean isCancelled() {
    return _isCancelled;
  }
}
