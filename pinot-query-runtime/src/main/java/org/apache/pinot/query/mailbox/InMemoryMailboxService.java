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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryMailboxService implements MailboxService<TransferableBlock> {
  // channel manager
  private final String _hostname;
  private final int _mailboxPort;
  private final Consumer<MailboxIdentifier> _receivedMailContentCallback;
  // TODO: This should come from a config and should be consistent with the timeout for GrpcMailboxService
  static final int DEFAULT_CHANNEL_TIMEOUT_SECONDS = 1;

  static public final int DEFAULT_CHANNEL_CAPACITY = 5;

  private final ConcurrentHashMap<String, InMemoryReceivingMailbox> _mailboxStateMap = new ConcurrentHashMap<>();

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
  public void close(MailboxIdentifier mid) {
    // Notify the sender for mailbox closing.
    _mailboxStateMap.remove(mid);
  }

  @Override
  public String getHostname() {
    return _hostname;
  }

  @Override
  public int getMailboxPort() {
    return _mailboxPort;
  }

  @Override
  public SendingMailbox<TransferableBlock> createSendingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    String mId = mailboxId.toString();
    InMemoryReceivingMailbox receivingMailbox =
        _mailboxStateMap.computeIfAbsent(mId, mid -> new InMemoryReceivingMailbox(mId));
    return new InMemorySendingMailbox(mId, receivingMailbox, _receivedMailContentCallback);
  }

  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    String mId = mailboxId.toString();
    ReceivingMailbox<TransferableBlock> mailbox =
        _mailboxStateMap.computeIfAbsent(mId, mid -> new InMemoryReceivingMailbox(mId));
    if (mailbox.isInitialized()) {
      _mailboxStateMap.remove(mId);
    }
    return mailbox;
  }
}
