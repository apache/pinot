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
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code MailboxStatusStreamObserver} is the status streaming observer used to track the status by the sender.
 */
public class MailboxStatusObserver implements StreamObserver<MailboxStatus> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxStatusObserver.class);
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;

  private final AtomicInteger _bufferSize = new AtomicInteger(DEFAULT_MAILBOX_QUEUE_CAPACITY);
  private final AtomicBoolean _finished = new AtomicBoolean();
  private volatile boolean _isEarlyTerminated;

  @Override
  public void onNext(MailboxStatus mailboxStatus) {
    // when receiving mailbox receives a data block it will return an updated info of the receiving end status including
    //   1. the buffer size available, for back-pressure handling
    //   2. status whether there's no need to send any additional data block b/c it considered itself finished.
    // -- handle early-terminate EOS request.
    if (Boolean.parseBoolean(
        mailboxStatus.getMetadataMap().get(ChannelUtils.MAILBOX_METADATA_REQUEST_EARLY_TERMINATE))) {
      _isEarlyTerminated = true;
    }
    // -- handling buffer size back-pressure
    // TODO: this feedback info is not used to throttle the send speed. it is currently being discarded.
    if (mailboxStatus.getMetadataMap().containsKey(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY)) {
      _bufferSize.set(
          Integer.parseInt(mailboxStatus.getMetadataMap().get(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY)));
    } else {
      _bufferSize.set(DEFAULT_MAILBOX_QUEUE_CAPACITY); // DEFAULT_AVAILABILITY;
    }
  }

  public boolean isEarlyTerminated() {
    return _isEarlyTerminated;
  }

  public int getBufferSize() {
    return _bufferSize.get();
  }

  @Override
  public void onError(Throwable t) {
    LOGGER.warn("Error on sender side", t);
    _finished.set(true);
  }

  @Override
  public void onCompleted() {
    _finished.set(true);
  }

  public boolean isFinished() {
    return _finished.get();
  }
}
