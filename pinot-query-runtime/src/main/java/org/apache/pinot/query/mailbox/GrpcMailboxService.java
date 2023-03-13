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
import com.google.common.cache.RemovalNotification;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.MailboxStatusStreamObserver;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GRPC-based implementation of {@link MailboxService}. Note that there can be cases where the ReceivingMailbox
 * and/or the underlying connection can be leaked:
 *
 * <ol>
 *   <li>When the OpChain corresponding to the receiver was never registered.</li>
 *   <li>When the receiving OpChain exited before data was sent for the first time by the sender.</li>
 * </ol>
 *
 * To handle these cases, we store the {@link ReceivingMailbox} entries in a time-expiring cache. If there was a
 * leak, the entry would be evicted, and in that case we also issue a cancel to ensure the underlying stream is also
 * released.
 */
public class GrpcMailboxService implements MailboxService<TransferableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMailboxService.class);
  // channel manager
  private static final Duration DANGLING_RECEIVING_MAILBOX_EXPIRY = Duration.ofMinutes(5);
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _mailboxPort;

  // We use a cache to ensure that the receiving mailbox and the underlying gRPC stream are not leaked in the cases
  // where the corresponding OpChain is either never registered or died before the sender sent data for the first time.
  private final Cache<String, GrpcReceivingMailbox> _receivingMailboxCache =
      CacheBuilder.newBuilder().expireAfterAccess(DANGLING_RECEIVING_MAILBOX_EXPIRY.toMinutes(), TimeUnit.MINUTES)
          .removalListener(new RemovalListener<String, GrpcReceivingMailbox>() {
            @Override
            public void onRemoval(RemovalNotification<String, GrpcReceivingMailbox> notification) {
              if (notification.wasEvicted()) {
                // TODO: This should be tied with query deadline, but for that we need to know the query deadline
                //  when the GrpcReceivingMailbox is initialized in MailboxContentStreamObserver.
                LOGGER.warn("Removing dangling GrpcReceivingMailbox: {}", notification.getKey());
                notification.getValue().cancel();
              }
            }
          })
          .build();
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
   * {@inheritDoc}
   */
  @Override
  public SendingMailbox<TransferableBlock> getSendingMailbox(MailboxIdentifier mailboxId, long deadlineMs) {
    MailboxStatusStreamObserver statusStreamObserver = new MailboxStatusStreamObserver();

    GrpcSendingMailbox mailbox = new GrpcSendingMailbox(mailboxId.toString(), statusStreamObserver, (deadline) -> {
      ManagedChannel channel = getChannel(mailboxId.toString());
      PinotMailboxGrpc.PinotMailboxStub stub =
          PinotMailboxGrpc.newStub(channel)
              .withDeadlineAfter(Math.max(0L, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
      return stub.open(statusStreamObserver);
    }, deadlineMs);
    return mailbox;
  }

  /**
   * {@inheritDoc} See {@link GrpcMailboxService} for details on the design.
   */
  @Override
  public ReceivingMailbox<TransferableBlock> getReceivingMailbox(MailboxIdentifier mailboxId) {
    try {
      return _receivingMailboxCache.get(mailboxId.toString(),
          () -> new GrpcReceivingMailbox(mailboxId.toString(), _gotMailCallback));
    } catch (ExecutionException e) {
      LOGGER.error(String.format("Error getting receiving mailbox: %s", mailboxId), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * If there's a cached receiving mailbox and it isn't closed (i.e. query didn't finish successfully), then this
   * calls a cancel to ensure that the underlying gRPC stream is closed. After that the receiving mailbox is removed
   * from the cache.
   * <p>
   *   Also refer to the definition in the interface:
   * </p>
   * <p>
   *   {@inheritDoc}
   * </p>
   */
  @Override
  public void releaseReceivingMailbox(MailboxIdentifier mailboxId) {
    GrpcReceivingMailbox receivingMailbox = _receivingMailboxCache.getIfPresent(mailboxId.toString());
    if (receivingMailbox != null && !receivingMailbox.isClosed()) {
      receivingMailbox.cancel();
    }
    _receivingMailboxCache.invalidate(mailboxId.toString());
  }

  private ManagedChannel getChannel(String mailboxId) {
    return _channelManager.getChannel(Utils.constructChannelId(mailboxId));
  }

  @Override
  public String toString() {
    return "GrpcMailboxService{" + "_hostname='" + _hostname + '\'' + ", _mailboxPort=" + _mailboxPort + '}';
  }
}
