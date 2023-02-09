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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.proto.Mailbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code MailboxStatusStreamObserver} is used by the SendingMailbox to send data over the wire.
 *
 * <p>Once {@link org.apache.pinot.query.mailbox.GrpcSendingMailbox#init()} is called, one instances of this class is
 * created based on the opened GRPC connection returned {@link StreamObserver}.
 * end.
 */
public class MailboxStatusStreamObserver implements StreamObserver<Mailbox.MailboxStatus> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxStatusStreamObserver.class);
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private final AtomicInteger _bufferSize = new AtomicInteger(5);

  private CountDownLatch _finishLatch;

  public MailboxStatusStreamObserver(CountDownLatch finishLatch) {
    _finishLatch = finishLatch;
  }

  @Override
  public void onNext(Mailbox.MailboxStatus mailboxStatus) {
    // when received a mailbox status from the receiving end, sending end update the known buffer size available
    // so we can make better throughput send judgement. here is a simple example.
    // TODO: this feedback info is not used to throttle the send speed. it is currently being discarded.
    if (mailboxStatus.getMetadataMap().containsKey(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY)) {
      _bufferSize.set(
          Integer.parseInt(mailboxStatus.getMetadataMap().get(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY)));
    } else {
      _bufferSize.set(DEFAULT_MAILBOX_QUEUE_CAPACITY); // DEFAULT_AVAILABILITY;
    }
  }

  @Override
  public void onError(Throwable e) {
    _finishLatch.countDown();
    LOGGER.error("Receiving error msg from grpc mailbox status stream:", e);
    throw new RuntimeException(e);
  }

  @Override
  public void onCompleted() {
    _finishLatch.countDown();
  }
}
