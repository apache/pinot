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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.mailbox.GrpcReceivingMailbox;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code MailboxContentStreamObserver} is the content streaming observer used to receive mailbox content.
 *
 * <p>When the observer onNext() is called (e.g. when data packet has arrived at the receiving end), it puts the
 * mailbox content to the receiving mailbox buffer; response with the remaining buffer size of the receiving mailbox
 * to the sender side.
 */
public class MailboxContentStreamObserver implements StreamObserver<Mailbox.MailboxContent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxContentStreamObserver.class);
  private static final int DEFAULT_MAILBOX_QUEUE_CAPACITY = 5;
  private final GrpcMailboxService _mailboxService;
  private final StreamObserver<Mailbox.MailboxStatus> _responseObserver;
  private final boolean _isEnabledFeedback;

  private final AtomicBoolean _isCompleted = new AtomicBoolean(false);
  private final ArrayBlockingQueue<Mailbox.MailboxContent> _receivingBuffer;
  private StringMailboxIdentifier _mailboxId;
  private Consumer<MailboxIdentifier> _gotMailCallback;

  public MailboxContentStreamObserver(GrpcMailboxService mailboxService,
      StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    this(mailboxService, responseObserver, false);
  }

  public MailboxContentStreamObserver(GrpcMailboxService mailboxService,
      StreamObserver<Mailbox.MailboxStatus> responseObserver, boolean isEnabledFeedback) {
    _mailboxService = mailboxService;
    _responseObserver = responseObserver;
    _receivingBuffer = new ArrayBlockingQueue<>(DEFAULT_MAILBOX_QUEUE_CAPACITY);
    _isEnabledFeedback = isEnabledFeedback;
  }

  /**
   * This method will return {@code null} if there has not been any data since the
   * last poll. Callers should be careful not to call this in a tight loop, and
   * instead use the {@code gotMailCallback} passed into this observer's constructor
   * to indicate when to call this method.
   */
  public Mailbox.MailboxContent poll() {
    if (isCompleted()) {
      return null;
    }

    return _receivingBuffer.poll();
  }

  public boolean isCompleted() {
    return _isCompleted.get() && _receivingBuffer.isEmpty();
  }

  @Override
  public void onNext(Mailbox.MailboxContent mailboxContent) {
    _mailboxId = new StringMailboxIdentifier(mailboxContent.getMailboxId());

    GrpcReceivingMailbox receivingMailbox = (GrpcReceivingMailbox) _mailboxService.getReceivingMailbox(_mailboxId);
    _gotMailCallback = receivingMailbox.init(this);

    if (!mailboxContent.getMetadataMap().containsKey(ChannelUtils.MAILBOX_METADATA_BEGIN_OF_STREAM_KEY)) {
      // when the receiving end receives a message put it in the mailbox queue.
      _receivingBuffer.offer(mailboxContent);
      _gotMailCallback.accept(_mailboxId);

      if (_isEnabledFeedback) {
        // TODO: this has race conditions with onCompleted() because sender blindly closes connection channels once
        // it has finished sending all the data packets.
        int remainingCapacity = _receivingBuffer.remainingCapacity() - 1;
        Mailbox.MailboxStatus.Builder builder =
            Mailbox.MailboxStatus.newBuilder().setMailboxId(mailboxContent.getMailboxId())
                .putMetadata(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY, String.valueOf(remainingCapacity));
        if (mailboxContent.getMetadataMap().get(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY) != null) {
          builder.putMetadata(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY, "true");
        }
        Mailbox.MailboxStatus status = builder.build();
        // returns the buffer available size to sender for rate controller / throttling.
        _responseObserver.onNext(status);
      }
    }
  }

  @Override
  public void onError(Throwable e) {
    try {
      _receivingBuffer.offer(Mailbox.MailboxContent.newBuilder()
          .setPayload(ByteString.copyFrom(
              TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(e)).getDataBlock().toBytes()))
          .putMetadata(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY, "true").build());
      _gotMailCallback.accept(_mailboxId);
      throw new RuntimeException(e);
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to encode exception for cascade reporting: " + e, ioe);
    }
  }

  @Override
  public void onCompleted() {
    _isCompleted.set(true);
    _responseObserver.onCompleted();
  }
}
