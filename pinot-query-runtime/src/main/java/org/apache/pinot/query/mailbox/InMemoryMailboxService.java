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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.query.mailbox.channel.InMemoryChannel;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class InMemoryMailboxService implements MailboxService<TransferableBlock> {
  // channel manager
  private final String _hostname;
  private final int _mailboxPort;
  static final int DEFAULT_CHANNEL_CAPACITY = 5;
  // TODO: This should come from a config and should be consistent with the timeout for GrpcMailboxService
  static final int DEFAULT_CHANNEL_TIMEOUT_SECONDS = 120;

  // maintaining a list of registered mailboxes.
  private final ConcurrentHashMap<String, ReceivingMailbox<TransferableBlock>> _receivingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, SendingMailbox<TransferableBlock>> _sendingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, InMemoryChannel<TransferableBlock>> _channelMap = new ConcurrentHashMap<>();

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
    _channelMap.computeIfAbsent(mId, (x) -> createDefaultChannel(_hostname, _mailboxPort));
    return _sendingMailboxMap.computeIfAbsent(mId, (x) -> new InMemorySendingMailbox(x,
        _channelMap.get(x)));
  }

  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    Preconditions.checkState(mailboxId.isLocal(), "Cannot use in-memory mailbox service for non-local transport");
    String mId = mailboxId.toString();
    _channelMap.computeIfAbsent(mId, (x) -> createDefaultChannel(_hostname, _mailboxPort));
    return _receivingMailboxMap.computeIfAbsent(mId, (x) -> new InMemoryReceivingMailbox(mId,
        _channelMap.get(x)));
  }

  private InMemoryChannel<TransferableBlock> createDefaultChannel(String hostname, int port) {
    return new InMemoryChannel<>(new ArrayBlockingQueue<>(DEFAULT_CHANNEL_CAPACITY), hostname, port);
  }
}
