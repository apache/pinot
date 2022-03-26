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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.MailboxContentStreamObserver;


/**
 * GRPC implementation of the {@link ReceivingMailbox}.
 */
public class GrpcReceivingMailbox implements ReceivingMailbox<MailboxContent> {
  private static final long DEFAULT_MAILBOX_INIT_TIMEOUT = 100L;
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final CountDownLatch _initializationLatch;
  private final AtomicInteger _totalMsgReceived = new AtomicInteger(0);

  private MailboxContentStreamObserver _contentStreamObserver;

  public GrpcReceivingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
    _initializationLatch = new CountDownLatch(1);
  }

  public void init(MailboxContentStreamObserver streamObserver) {
    if (_initializationLatch.getCount() > 0) {
      _contentStreamObserver = streamObserver;
      _initializationLatch.countDown();
    }
  }

  @Override
  public MailboxContent receive()
      throws Exception {
    MailboxContent mailboxContent = null;
    if (waitForInitialize()) {
      mailboxContent = _contentStreamObserver.poll();
      _totalMsgReceived.incrementAndGet();
    }
    return mailboxContent;
  }

  @Override
  public boolean isInitialized() {
    return _initializationLatch.getCount() <= 0;
  }

  @Override
  public boolean isClosed() {
    return isInitialized() && _contentStreamObserver.isCompleted();
  }

  // TODO: fix busy wait. This should be guarded by timeout.
  private boolean waitForInitialize()
      throws Exception {
    if (_initializationLatch.getCount() > 0) {
      return _initializationLatch.await(DEFAULT_MAILBOX_INIT_TIMEOUT, TimeUnit.MILLISECONDS);
    } else {
      return true;
    }
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
