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

import io.grpc.ManagedChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GRPC-based implementation of {@link MailboxService}.
 *
 * <p>It maintains a collection of connected mailbox servers and clients to remote hosts. All indexed by the
 * mailboxID in the format of: <code>"jobId:partitionKey:senderHost:senderPort:receiverHost:receiverPort"</code>
 *
 * <p>Connections are established/initiated from the sender side and only tier-down from the sender side as well.
 * In the event of exception or timed out, the connection is cloased based on a mutually agreed upon timeout period
 * after the last successful message sent/received.
 *
 * <p>Noted that:
 * <ul>
 *   <li>the latter part of the mailboxID consist of the channelID.</li>
 *   <li>the job_id should be uniquely identifying a send/receving pair, for example if one bundle job requires
 *   to open 2 mailboxes, they should use {job_id}_1 and {job_id}_2 to distinguish the 2 different mailbox.</li>
 * </ul>
 */
public class GrpcMailboxService implements MailboxService<TransferableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemorySendingMailbox.class);
  // channel manager
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _mailboxPort;

  // maintaining a list of registered mailboxes.
  private final ConcurrentHashMap<String, ReceivingMailbox<TransferableBlock>> _receivingMailboxMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, SendingMailbox<TransferableBlock>> _sendingMailboxMap =
      new ConcurrentHashMap<>();
  private final Consumer<MailboxIdentifier> _gotMailCallback;

  public GrpcMailboxService(String hostname, int mailboxPort, PinotConfiguration extraConfig,
      Consumer<MailboxIdentifier> gotMailCallback) {
    _hostname = hostname;
    _mailboxPort = mailboxPort;
    _channelManager = new ChannelManager(this, extraConfig);
    _gotMailCallback = gotMailCallback;
  }

  @Override
  public void start() {
    _channelManager.init();
  }

  @Override
  public void shutdown() {
    _channelManager.shutdown();
  }

  @Override
  public String getHostname() {
    return _hostname;
  }

  @Override
  public int getMailboxPort() {
    return _mailboxPort;
  }

  /**
   * Register a mailbox, mailbox needs to be registered before use.
   * @param mailboxId the id of the mailbox.
   */
  public SendingMailbox<TransferableBlock> getSendingMailbox(MailboxIdentifier mailboxId) {
    SendingMailbox<TransferableBlock> mailbox =
        _sendingMailboxMap.computeIfAbsent(mailboxId.toString(), (mId) -> new GrpcSendingMailbox(mId, this));
    LOGGER.trace("GrpcMailboxService sendingMailBox size:" + _sendingMailboxMap.size());
    return mailbox;
  }

  /**
   * Register a mailbox, mailbox needs to be registered before use.
   * @param mailboxId the id of the mailbox.
   */
  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    ReceivingMailbox<TransferableBlock> mailbox = _receivingMailboxMap.computeIfAbsent(
        mailboxId.toString(), (mId) -> new GrpcReceivingMailbox(mId, this, _gotMailCallback));
    LOGGER.trace("GrpcMailboxService receiving size:" + _receivingMailboxMap.size());
    return mailbox;
  }

  public ManagedChannel getChannel(String mailboxId) {
    ManagedChannel channel = _channelManager.getChannel(Utils.constructChannelId(mailboxId));
    LOGGER.trace("GrpcMailboxService channel manager size:" + _channelManager.size());
    return channel;
  }

  @Override
  public String toString() {
    return "GrpcMailboxService{" + "_hostname='" + _hostname + '\'' + ", _mailboxPort=" + _mailboxPort + '}';
  }
}
