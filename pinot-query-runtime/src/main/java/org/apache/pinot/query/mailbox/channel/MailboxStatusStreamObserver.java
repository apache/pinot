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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

  private final CountDownLatch _finishLatch = new CountDownLatch(1);

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
  }

  @Override
  public void onError(Throwable e) {
    _finishLatch.countDown();
    shutdown();
    throw new RuntimeException(e);
  }

  private void shutdown() {
  }

  @Override
  public void onCompleted() {
    _finishLatch.countDown();
    shutdown();
  }

  public void waitForComplete(long durationNanos)
      throws InterruptedException {
    if (!_finishLatch.await(durationNanos, TimeUnit.NANOSECONDS)) {
      LOGGER.error("MailboxSend cannot finish within " + durationNanos + " nanoseconds");
    }
  }
}
