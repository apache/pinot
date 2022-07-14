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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code MailboxStatusStreamObserver} is used by the SendingMailbox to send data over the wire.
 *
 * <p>Once {@link org.apache.pinot.query.mailbox.GrpcSendingMailbox#init()} is called, one instances of this class is
 * created based on the opened GRPC connection returned {@link StreamObserver}. From this point, the sending mailbox
 * can use the {@link MailboxStatusStreamObserver#send(Mailbox.MailboxContent)} API to send data packet to the receiving
 * end.
 */
public class MailboxStatusStreamObserver implements StreamObserver<Mailbox.MailboxStatus> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxStatusStreamObserver.class);
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private static final long DEFAULT_MAILBOX_POLL_TIMEOUT_MS = 1000L;
  private final AtomicInteger _bufferSize = new AtomicInteger(5);
  private final AtomicBoolean _isCompleted = new AtomicBoolean(false);

  private StreamObserver<Mailbox.MailboxContent> _mailboxContentStreamObserver;

  public MailboxStatusStreamObserver() {
  }

  public void init(StreamObserver<Mailbox.MailboxContent> mailboxContentStreamObserver) {
    _mailboxContentStreamObserver = mailboxContentStreamObserver;
  }

  public void send(Mailbox.MailboxContent mailboxContent) {
    _mailboxContentStreamObserver.onNext(mailboxContent);
  }

  public void complete() {
    _mailboxContentStreamObserver.onCompleted();
  }

  @Override
  public void onNext(Mailbox.MailboxStatus mailboxStatus) {
    // when received a mailbox status from the receiving end, sending end update the known buffer size available
    // so we can make better throughput send judgement. here is a simple example.
    // TODO: this feedback info is not used to throttle the send speed. it is currently being discarded.
    if (mailboxStatus.getMetadataMap().containsKey(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY)) {
      _bufferSize.set(Integer.parseInt(
          mailboxStatus.getMetadataMap().get(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY)));
    } else {
      _bufferSize.set(DEFAULT_MAILBOX_QUEUE_CAPACITY); // DEFAULT_AVAILABILITY;
    }
  }

  @Override
  public void onError(Throwable e) {
    _isCompleted.set(true);
    shutdown();
    throw new RuntimeException(e);
  }

  private void shutdown() {
  }

  @Override
  public void onCompleted() {
    _isCompleted.set(true);
    shutdown();
  }
}
