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
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.mailbox.GrpcReceivingMailbox;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
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
  public static final int DEFAULT_MAX_PENDING_MAILBOX_CONTENT = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxContentStreamObserver.class);
  private static final Mailbox.MailboxContent DEFAULT_ERROR_MAILBOX_CONTENT;
  // This delta is added to the buffer offer on top of the query timeout to avoid a race between client cancellation
  // due to deadline and server-side cancellation due to the receiving buffer being full.
  private static final long BUFFER_OFFER_TIMEOUT_DELTA_MS = 1_000;

  static {
    try {
      RuntimeException exception = new RuntimeException(
          "Error creating error-content.. please file a bug in the apache/pinot repo");
      DEFAULT_ERROR_MAILBOX_CONTENT = Mailbox.MailboxContent.newBuilder().setPayload(ByteString.copyFrom(
          TransferableBlockUtils.getErrorTransferableBlock(exception).getDataBlock().toBytes()))
          .putMetadata(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY, "true").build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final GrpcMailboxService _mailboxService;
  private final StreamObserver<Mailbox.MailboxStatus> _responseObserver;

  private final AtomicBoolean _isCompleted = new AtomicBoolean(false);
  private final BlockingQueue<Mailbox.MailboxContent> _receivingBuffer;

  private Mailbox.MailboxContent _errorContent = null;
  private JsonMailboxIdentifier _mailboxId;
  private Consumer<MailboxIdentifier> _gotMailCallback;

  public MailboxContentStreamObserver(GrpcMailboxService mailboxService,
      StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    _mailboxService = mailboxService;
    _responseObserver = responseObserver;
    _receivingBuffer = new ArrayBlockingQueue<>(DEFAULT_MAX_PENDING_MAILBOX_CONTENT);
  }

  /**
   * This method will return {@code null} if there has not been any data since the
   * last poll. Callers should be careful not to call this in a tight loop, and
   * instead use the {@code gotMailCallback} passed into this observer's constructor
   * to indicate when to call this method.
   */
  public Mailbox.MailboxContent poll() {
    if (_errorContent != null) {
      return _errorContent;
    }
    if (hasConsumedAllData()) {
      return null;
    }

    return _receivingBuffer.poll();
  }

  @Override
  public void onNext(Mailbox.MailboxContent mailboxContent) {
    _mailboxId = JsonMailboxIdentifier.parse(mailboxContent.getMailboxId());

    long remainingTimeMs = Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    GrpcReceivingMailbox receivingMailbox = (GrpcReceivingMailbox) _mailboxService.getReceivingMailbox(_mailboxId);
    _gotMailCallback = receivingMailbox.init(this, _responseObserver);
    if (_errorContent != null) {
      // This should never happen because gRPC calls StreamObserver in a single-threaded fashion, and if error-content
      // is not null then we would have already issued a onError which means gRPC will not call onNext again.
      LOGGER.warn("onNext called even though already errored out");
      return;
    }

    if (!mailboxContent.getMetadataMap().containsKey(ChannelUtils.MAILBOX_METADATA_BEGIN_OF_STREAM_KEY)) {
      // when the receiving end receives a message put it in the mailbox queue.
      try {
        final long offerTimeoutMs = remainingTimeMs + BUFFER_OFFER_TIMEOUT_DELTA_MS;
        if (!_receivingBuffer.offer(mailboxContent, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
          RuntimeException e = new RuntimeException("Timed out offering to the receivingBuffer: " + _mailboxId);
          LOGGER.error(e.getMessage());
          _errorContent = createErrorContent(e);
          try {
            _responseObserver.onError(Status.CANCELLED.asRuntimeException());
          } catch (Exception ignored) {
            // Exception can be thrown if the stream deadline has already been reached, so we simply ignore it.
          }
        }
      } catch (InterruptedException e) {
        _errorContent = createErrorContent(e);
        LOGGER.error("Interrupted while polling receivingBuffer", e);
        _responseObserver.onError(Status.CANCELLED.asRuntimeException());
      }
      _gotMailCallback.accept(_mailboxId);
    }
  }

  @Override
  public void onError(Throwable e) {
    if (_errorContent == null) {
      _errorContent = createErrorContent(e);
    }
    _gotMailCallback.accept(_mailboxId);
  }

  @Override
  public void onCompleted() {
    _isCompleted.set(true);
    _gotMailCallback.accept(_mailboxId);
    _responseObserver.onCompleted();
  }

  /**
   * @return true if all data has been received via {@link #poll()}.
   */
  public boolean hasConsumedAllData() {
    return _isCompleted.get() && _receivingBuffer.isEmpty();
  }

  private static Mailbox.MailboxContent createErrorContent(Throwable e) {
    try {
      return Mailbox.MailboxContent.newBuilder().setPayload(ByteString.copyFrom(
          TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(e)).getDataBlock().toBytes()))
          .putMetadata(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY, "true").build();
    } catch (IOException ioException) {
      LOGGER.error("Error creating error MailboxContent", ioException);
      return DEFAULT_ERROR_MAILBOX_CONTENT;
    }
  }
}
