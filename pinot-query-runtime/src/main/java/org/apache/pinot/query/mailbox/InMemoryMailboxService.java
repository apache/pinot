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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryMailboxService implements MailboxService<TransferableBlock> {
  // channel manager
  private final String _hostname;
  private final int _mailboxPort;
  static final int DEFAULT_CHANNEL_CAPACITY = 5;
  // TODO: This should come from a config and should be consistent with the timeout for GrpcMailboxService
  static final int DEFAULT_CHANNEL_TIMEOUT_SECONDS = 1;

  private final ConcurrentHashMap<String, InMemoryMailboxState> _mailboxStateMap = new ConcurrentHashMap<>();

  public InMemoryMailboxService(String hostname, int mailboxPort) {
    _hostname = hostname;
    _mailboxPort = mailboxPort;
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
    return _mailboxStateMap.computeIfAbsent(mId, this::newMailboxState)._sendingMailbox;
  }

  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    String mId = mailboxId.toString();
    return _mailboxStateMap.computeIfAbsent(mId, this::newMailboxState)._receivingMailbox;
  }

  InMemoryMailboxState newMailboxState(String mailboxId) {
    BlockingQueue<TransferableBlock> queue = createDefaultChannel();
    return new InMemoryMailboxState(new InMemorySendingMailbox(mailboxId, queue),
        new InMemoryReceivingMailbox(mailboxId, queue), queue);
  }

  private ArrayBlockingQueue<TransferableBlock> createDefaultChannel() {
    return new ArrayBlockingQueue<>(DEFAULT_CHANNEL_CAPACITY);
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
