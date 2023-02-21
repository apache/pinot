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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pinot.query.mailbox.channel.InMemoryTransferStream;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InMemoryMailboxService implements MailboxService<TransferableBlock> {
  // channel manager
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMailboxService.class);
  private static final Duration DANGLING_RECEIVING_MAILBOX_EXPIRY = Duration.ofMinutes(5);
  private final String _hostname;
  private final int _mailboxPort;
  private final Consumer<MailboxIdentifier> _receivedMailContentCallback;

  // maintaining a list of registered mailboxes.
  private final Cache<String, InMemoryReceivingMailbox> _receivingMailboxCache =
      CacheBuilder.newBuilder().expireAfterAccess(DANGLING_RECEIVING_MAILBOX_EXPIRY.toMinutes(), TimeUnit.MINUTES)
          .removalListener(new RemovalListener<String, InMemoryReceivingMailbox>() {
            @Override
            public void onRemoval(RemovalNotification<String, InMemoryReceivingMailbox> notification) {
              if (notification.wasEvicted()) {
                if (!notification.getValue().isClaimed()) {
                  String reason = String.format("Receiving mailbox=%s was never claimed", notification.getKey());
                  notification.getValue().cancel(new RuntimeException(reason));
                }
              }
            }
          })
          .build();
  private final ConcurrentHashMap<String, InMemoryTransferStream> _transferStreamMap = new ConcurrentHashMap<>();

  public InMemoryMailboxService(String hostname, int mailboxPort,
      Consumer<MailboxIdentifier> receivedMailContentCallback) {
    _hostname = hostname;
    _mailboxPort = mailboxPort;
    _receivedMailContentCallback = receivedMailContentCallback;
  }

  public Consumer<MailboxIdentifier> getReceivedMailContentCallback() {
    return _receivedMailContentCallback;
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
    String mId = mailboxId.toString();
    // for now, we use an unbounded blocking queue as the means of communication between
    // in memory mailboxes - the reason for this is that unless we implement flow control,
    // blocks will sit in memory either way (blocking the sender from sending doesn't prevent
    // more blocks from being generated from upstream). on the other hand, having a capacity
    // for the queue causes the sending thread to occupy a task pool thread and prevents other
    // threads (most importantly, the receiving thread) from running - which can cause unnecessary
    // failure situations
    // TODO: when we implement flow control, we should swap this out with a bounded abstraction
    return new InMemorySendingMailbox(mailboxId.toString(),
        _transferStreamMap.computeIfAbsent(mId, id -> new InMemoryTransferStream(mailboxId, this)),
        getReceivedMailContentCallback());
  }

  @Override
  public void releaseReceivingMailbox(MailboxIdentifier mailboxId) {
    _receivingMailboxCache.invalidate(mailboxId.toString());
  }

  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    String mId = mailboxId.toString();
    InMemoryTransferStream transferStream = _transferStreamMap.computeIfAbsent(mId,
        id -> new InMemoryTransferStream(mailboxId, this));
    try {
      return _receivingMailboxCache.get(mId, () -> new InMemoryReceivingMailbox(mId, transferStream));
    } catch (ExecutionException e) {
      LOGGER.error(String.format("Error getting in-memory receiving mailbox=%s", mailboxId), e);
      throw new RuntimeException(e);
    }
  }
}
