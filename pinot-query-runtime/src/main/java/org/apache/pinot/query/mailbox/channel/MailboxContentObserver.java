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

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code MailboxContentObserver} is the content streaming observer used to receive mailbox content.
 *
 * <p>When the observer onNext() is called (e.g. when data packet has arrived at the receiving end), it puts the
 * mailbox content to the receiving mailbox buffer; response with the remaining buffer size of the receiving mailbox
 * to the sender side.
 */
public class MailboxContentObserver implements StreamObserver<MailboxContent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxContentObserver.class);

  private final MailboxService _mailboxService;
  private final StreamObserver<MailboxStatus> _responseObserver;
  private final List<ByteBuffer> _mailboxBuffers = Collections.synchronizedList(new ArrayList<>());
  private boolean _closedStream = false;

  private volatile ReceivingMailbox _mailbox;

  public MailboxContentObserver(MailboxService mailboxService, String mailboxId,
      StreamObserver<MailboxStatus> responseObserver) {
    _mailboxService = mailboxService;
    _responseObserver = responseObserver;
    _mailbox = StringUtils.isNotBlank(mailboxId) ? _mailboxService.getReceivingMailbox(mailboxId) : null;
  }

  @Override
  public void onNext(MailboxContent mailboxContent) {
    if (_closedStream) {
      LOGGER.debug("Received a late message once the stream was closed. Ignoring it.");
      return;
    }
    String mailboxId = mailboxContent.getMailboxId();
    if (_mailbox == null) {
      _mailbox = _mailboxService.getReceivingMailbox(mailboxId);
    }
    _mailboxBuffers.add(mailboxContent.getPayload().asReadOnlyByteBuffer());
    if (mailboxContent.getWaitForMore()) {
      return;
    }
    try {
      long timeoutMs = Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
      ReceivingMailbox.ReceivingMailboxStatus status = null;
      try {
        status = _mailbox.offerRaw(_mailboxBuffers, timeoutMs);
      } catch (TimeoutException e) {
        LOGGER.debug("Timed out adding block into mailbox: {} with timeout: {}ms", mailboxId, timeoutMs);
        closeStream();
        return;
      } catch (InterruptedException e) {
        // We are not restoring the interrupt status because we are already throwing an exception
        // Code that catches this exception must finish the work fast enough to comply the interrupt contract
        // See https://github.com/apache/pinot/pull/16903#discussion_r2409003423
        LOGGER.debug("Interrupted while processing blocks for mailbox: {}", mailboxId, e);
        closeStream();
        return;
      }
      switch (status) {
        case SUCCESS:
          _responseObserver.onNext(MailboxStatus.newBuilder().setMailboxId(mailboxId)
              .putMetadata(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY,
                  Integer.toString(_mailbox.getNumPendingBlocks())).build());
          break;
        case WAITING_EOS:
          // The receiving mailbox is early terminated, inform the sender to stop sending more data. Only EOS block is
          // expected to be sent afterward.
          _responseObserver.onNext(MailboxStatus.newBuilder().setMailboxId(mailboxId)
              .putMetadata(ChannelUtils.MAILBOX_METADATA_REQUEST_EARLY_TERMINATE, "true").build());
          break;
        case LAST_BLOCK:
          LOGGER.debug("Mailbox: {} has received the last block, closing the stream", mailboxId);
          closeStream();
          break;
        case ALREADY_TERMINATED:
          LOGGER.warn("Trying to offer blocks to the already closed mailbox {}. This should not happen", mailboxId);
          closeStream();
          break;
        default:
          throw new IllegalStateException("Unsupported mailbox status: " + status);
      }
    } catch (Exception e) {
      String errorMessage = "Caught exception while processing blocks for mailbox: " + mailboxId;
      LOGGER.error(errorMessage, e);
      closeStream();
      _mailbox.setErrorBlock(
          ErrorMseBlock.fromException(new RuntimeException(errorMessage, e)), Collections.emptyList());
    } finally {
      _mailboxBuffers.clear();
    }
  }

  private void closeStream() {
    try {
      // NOTE: DO NOT use onError() because it will terminate the stream, and sender might not get the callback
      _responseObserver.onCompleted();
      _closedStream = true;
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception cancelling mailbox: {}", _mailbox != null ? _mailbox.getId() : "unknown", e);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOGGER.warn("Error on receiver side", t);
    _mailboxBuffers.clear();
    if (_mailbox != null) {
      String msg = t != null ? t.getMessage() : "Unknown";
      _mailbox.setErrorBlock(ErrorMseBlock.fromError(
          QueryErrorCode.QUERY_CANCELLATION, "Cancelled by sender with exception: " + msg), List.of());
    } else {
      LOGGER.error("Got error before mailbox is set up", t);
    }
    if (!_closedStream) {
      _closedStream = true;
      _responseObserver.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    _mailboxBuffers.clear();
    if (_closedStream) {
      return;
    }
    _closedStream = true;
    try {
      _responseObserver.onCompleted();
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception sending complete to mailbox: {}", _mailbox != null ? _mailbox.getId() : "unknown",
          e);
    }
  }
}
