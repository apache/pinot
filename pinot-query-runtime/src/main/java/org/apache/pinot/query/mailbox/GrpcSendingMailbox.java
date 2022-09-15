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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelUtils;
import org.apache.pinot.query.mailbox.channel.MailboxStatusStreamObserver;

/**
 * GRPC implementation of the {@link SendingMailbox}.
 */
public class GrpcSendingMailbox implements SendingMailbox<MailboxContent> {
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final AtomicBoolean _initialized = new AtomicBoolean(false);
  private final AtomicInteger _totalMsgSent = new AtomicInteger(0);

  private MailboxStatusStreamObserver _statusStreamObserver;

  public GrpcSendingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
    _initialized.set(false);
  }

  public void init()
      throws UnsupportedOperationException {
    ManagedChannel channel = _mailboxService.getChannel(_mailboxId);
    PinotMailboxGrpc.PinotMailboxStub stub = PinotMailboxGrpc.newStub(channel);
    _statusStreamObserver = new MailboxStatusStreamObserver();
    _statusStreamObserver.init(stub.open(_statusStreamObserver));
    // send a begin-of-stream message.
    _statusStreamObserver.send(MailboxContent.newBuilder()
        .setMailboxId(_mailboxId)
        .putMetadata(ChannelUtils.MAILBOX_METADATA_BEGIN_OF_STREAM_KEY, "true")
        .build());
    _initialized.set(true);
  }

  @Override
  public void send(MailboxContent data)
      throws UnsupportedOperationException {
    if (!_initialized.get()) {
      // initialization is special
      init();
    }
    _statusStreamObserver.send(data);
    _totalMsgSent.incrementAndGet();
  }

  @Override
  public void complete() {
    _statusStreamObserver.complete();
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }
}
