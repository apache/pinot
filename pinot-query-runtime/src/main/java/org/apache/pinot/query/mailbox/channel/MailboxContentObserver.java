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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
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

  private transient ReceivingMailbox _mailbox;

  public MailboxContentObserver(MailboxService mailboxService, StreamObserver<MailboxStatus> responseObserver) {
    _mailboxService = mailboxService;
    _responseObserver = responseObserver;
  }

  @Override
  public void onNext(MailboxContent mailboxContent) {
    String mailboxId = mailboxContent.getMailboxId();
    if (_mailbox == null) {
      _mailbox = _mailboxService.getReceivingMailbox(mailboxId);
    }
    try {
      TransferableBlock block;
      DataBlock dataBlock = DataBlockUtils.getDataBlock(mailboxContent.getPayload().asReadOnlyByteBuffer());
      if (dataBlock instanceof MetadataBlock) {
        Map<Integer, String> exceptions = dataBlock.getExceptions();
        if (exceptions.isEmpty()) {
          block = TransferableBlockUtils.getEndOfStreamTransferableBlock(((MetadataBlock) dataBlock).getStats());
        } else {
          _mailbox.setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(exceptions));
          return;
        }
      } else {
        block = new TransferableBlock(dataBlock);
      }

      long timeoutMs = Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
      ReceivingMailbox.ReceivingMailboxStatus status = _mailbox.offer(block, timeoutMs);
      switch (status) {
        case SUCCESS:
          _responseObserver.onNext(MailboxStatus.newBuilder().setMailboxId(mailboxId)
              .putMetadata(ChannelUtils.MAILBOX_METADATA_BUFFER_SIZE_KEY,
                  Integer.toString(_mailbox.getNumPendingBlocks())).build());
          break;
        case ERROR:
          LOGGER.warn("Mailbox: {} already errored out (received error block before)", mailboxId);
          cancelStream();
          break;
        case TIMEOUT:
          LOGGER.warn("Timed out adding block into mailbox: {} with timeout: {}ms", mailboxId, timeoutMs);
          cancelStream();
          break;
        case EARLY_TERMINATED:
          LOGGER.debug("Mailbox: {} has been early terminated", mailboxId);
          onCompleted();
          break;
        default:
          throw new IllegalStateException("Unsupported mailbox status: " + status);
      }
    } catch (Exception e) {
      String errorMessage = "Caught exception while processing blocks for mailbox: " + mailboxId;
      LOGGER.error(errorMessage, e);
      _mailbox.setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException(errorMessage, e)));
      cancelStream();
    }
  }

  private void cancelStream() {
    try {
      // NOTE: DO NOT use onError() because it will terminate the stream, and sender might not get the callback
      _responseObserver.onCompleted();
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception cancelling mailbox: {}", _mailbox != null ? _mailbox.getId() : "unknown", e);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOGGER.warn("Error on receiver side", t);
    if (_mailbox != null) {
      _mailbox.setErrorBlock(
          TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException("Cancelled by sender", t)));
    } else {
      LOGGER.error("Got error before mailbox is set up", t);
    }
  }

  @Override
  public void onCompleted() {
    try {
      _responseObserver.onCompleted();
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception sending complete to mailbox: {}", _mailbox != null ? _mailbox.getId() : "unknown",
          e);
    }
  }
}
