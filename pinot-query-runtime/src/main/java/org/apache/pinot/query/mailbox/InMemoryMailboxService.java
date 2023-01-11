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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryMailboxService implements MailboxService<TransferableBlock> {
  private static final Duration MAILBOX_CACHE_EXPIRY = Duration.ofMinutes(5);
  // channel manager
  private final String _hostname;
  private final int _mailboxPort;
  private final Consumer<MailboxIdentifier> _receivedMailContentCallback;

  private final Cache<String, InMemoryMailboxState> _mailboxStateCache =
      CacheBuilder.newBuilder().expireAfterAccess(MAILBOX_CACHE_EXPIRY.toMinutes(), TimeUnit.MINUTES).build();

  public InMemoryMailboxService(String hostname, int mailboxPort,
      Consumer<MailboxIdentifier> receivedMailContentCallback) {
    _hostname = hostname;
    _mailboxPort = mailboxPort;
    _receivedMailContentCallback = receivedMailContentCallback;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public String getHostname() {
    return _hostname;
  }

  @Override
  public int getMailboxPort() {
    return _mailboxPort;
  }

  public SendingMailbox<TransferableBlock> getSendingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    return getMailboxState(mailboxId.toString())._sendingMailbox;
  }

  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    return getMailboxState(mailboxId.toString())._receivingMailbox;
  }

  private InMemoryMailboxState getMailboxState(String mailboxId) {
    try {
      return _mailboxStateCache.get(mailboxId, () -> {
        BlockingQueue<TransferableBlock> queue = createDefaultChannel();
        InMemoryMailboxState mailboxState = new InMemoryMailboxState(
            new InMemorySendingMailbox(mailboxId, queue, _receivedMailContentCallback),
            new InMemoryReceivingMailbox(mailboxId, queue),
            queue);
        return mailboxState;
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private BlockingQueue<TransferableBlock> createDefaultChannel() {
    // for now, we use an unbounded blocking queue as the means of communication between
    // in memory mailboxes - the reason for this is that unless we implement flow control,
    // blocks will sit in memory either way (blocking the sender from sending doesn't prevent
    // more blocks from being generated from upstream). on the other hand, having a capacity
    // for the queue causes the sending thread to occupy a task pool thread and prevents other
    // threads (most importantly, the receiving thread) from running - which can cause unnecessary
    // failure situations
    // TODO: when we implement flow control, we should swap this out with a bounded abstraction
    return new LinkedBlockingQueue<>();
  }

  static class InMemoryMailboxState {
    ReceivingMailbox<TransferableBlock> _receivingMailbox;
    SendingMailbox<TransferableBlock> _sendingMailbox;
    BlockingQueue<TransferableBlock> _queue;

    InMemoryMailboxState(SendingMailbox<TransferableBlock> sendingMailbox,
        ReceivingMailbox<TransferableBlock> receivingMailbox, BlockingQueue<TransferableBlock> queue) {
      _receivingMailbox = receivingMailbox;
      _sendingMailbox = sendingMailbox;
      _queue = queue;
    }
  }
}
