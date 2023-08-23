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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.GrpcMailboxServer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mailbox service that handles data transfer.
 */
public class MailboxService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxService.class);
  private static final int DANGLING_RECEIVING_MAILBOX_EXPIRY_SECONDS = 300;

  /**
   * Cached receiving mailboxes that contains the received blocks queue.
   *
   * We use a cache to ensure the receiving mailbox are not leaked in the cases where the corresponding OpChain is
   * either never registered or died before the sender finished sending data.
   */
  private final Cache<String, ReceivingMailbox> _receivingMailboxCache =
      CacheBuilder.newBuilder().expireAfterAccess(DANGLING_RECEIVING_MAILBOX_EXPIRY_SECONDS, TimeUnit.SECONDS)
          .removalListener((RemovalListener<String, ReceivingMailbox>) notification -> {
            if (notification.wasEvicted()) {
              int numPendingBlocks = notification.getValue().getNumPendingBlocks();
              if (numPendingBlocks > 0) {
                LOGGER.warn("Evicting dangling receiving mailbox: {} with {} pending blocks", notification.getKey(),
                    numPendingBlocks);
              }
            }
          }).build();

  private final String _hostname;
  private final int _port;
  private final PinotConfiguration _config;
  private final ChannelManager _channelManager = new ChannelManager();

  private GrpcMailboxServer _grpcMailboxServer;

  public MailboxService(String hostname, int port, PinotConfiguration config) {
    _hostname = hostname;
    _port = port;
    _config = config;
    LOGGER.info("Initialized MailboxService with hostname: {}, port: {}", hostname, port);
  }

  /**
   * Starts the mailbox service.
   */
  public void start() {
    LOGGER.info("Starting GrpcMailboxServer");
    _grpcMailboxServer = new GrpcMailboxServer(this, _config);
    _grpcMailboxServer.start();
  }

  /**
   * Shuts down the mailbox service.
   */
  public void shutdown() {
    LOGGER.info("Shutting down GrpcMailboxServer");
    _grpcMailboxServer.shutdown();
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  /**
   * Returns a sending mailbox for the given mailbox id. The returned sending mailbox is uninitialized, i.e. it will
   * not open the underlying channel or acquire any additional resources. Instead, it will initialize lazily when the
   * data is sent for the first time.
   */
  public SendingMailbox getSendingMailbox(String hostname, int port, String mailboxId, long deadlineMs) {
    if (_hostname.equals(hostname) && _port == port) {
      return new InMemorySendingMailbox(mailboxId, this, deadlineMs);
    } else {
      return new GrpcSendingMailbox(mailboxId, _channelManager, hostname, port, deadlineMs);
    }
  }

  /**
   * Returns the receiving mailbox for the given mailbox id.
   */
  public ReceivingMailbox getReceivingMailbox(String mailboxId) {
    try {
      return _receivingMailboxCache.get(mailboxId, () -> new ReceivingMailbox(mailboxId));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Releases the receiving mailbox from the cache.
   *
   * The receiving mailbox for a given OpChain may be created before the OpChain is even registered. Reason being that
   * the sender starts sending data, and the receiver starts receiving the same without waiting for the OpChain to be
   * registered. The ownership for the ReceivingMailbox hence lies with the MailboxService and not the OpChain.
   *
   * We can safely release a receiving mailbox when all the data are received and processed by the OpChain. If there
   * might be data not received yet, we should not release the receiving mailbox to prevent a new receiving mailbox
   * being created.
   */
  public void releaseReceivingMailbox(ReceivingMailbox mailbox) {
    _receivingMailboxCache.invalidate(mailbox.getId());
  }
}
