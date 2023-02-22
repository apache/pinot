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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.ChannelUtils;
import org.apache.pinot.query.mailbox.channel.MailboxContentStreamObserver;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GRPC implementation of the {@link ReceivingMailbox}.
 */
public class GrpcReceivingMailbox implements ReceivingMailbox<TransferableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcReceivingMailbox.class);
  private static final long DEFAULT_MAILBOX_INIT_TIMEOUT = 100L;
  private final String _mailboxId;
  private Consumer<MailboxIdentifier> _gotMailCallback;
  private final CountDownLatch _initializationLatch;
  private final AtomicInteger _totalMsgReceived = new AtomicInteger(0);

  private MailboxContentStreamObserver _contentStreamObserver;
  private StreamObserver<Mailbox.MailboxStatus> _statusStreamObserver;

  public GrpcReceivingMailbox(String mailboxId, Consumer<MailboxIdentifier> gotMailCallback) {
    _mailboxId = mailboxId;
    _gotMailCallback = gotMailCallback;
    _initializationLatch = new CountDownLatch(1);
  }

  public Consumer<MailboxIdentifier> init(MailboxContentStreamObserver streamObserver,
      StreamObserver<Mailbox.MailboxStatus> statusStreamObserver) {
    if (_initializationLatch.getCount() > 0) {
      _contentStreamObserver = streamObserver;
      _statusStreamObserver = statusStreamObserver;
      _initializationLatch.countDown();
    }
    return _gotMailCallback;
  }

  /**
   * Polls the underlying channel and converts the received data into a TransferableBlock. This may return null in the
   * following cases:
   *
   * <p>
   *  1. If the mailbox hasn't initialized yet. This means we haven't received any data yet.
   *  2. If the received block from the sender is a data-block with 0 rows.
   * </p>
   */
  @Override
  public TransferableBlock receive() throws Exception {
    if (!waitForInitialize()) {
      return null;
    }
    MailboxContent mailboxContent = _contentStreamObserver.poll();
    _totalMsgReceived.incrementAndGet();
    return mailboxContent == null ? null : fromMailboxContent(mailboxContent);
  }

  @Override
  public boolean isInitialized() {
    return _initializationLatch.getCount() == 0;
  }

  @Override
  public boolean isClosed() {
    return isInitialized() && _contentStreamObserver.isCompleted();
  }

  @Override
  public void cancel() {
    if (isInitialized()) {
      try {
        _statusStreamObserver.onError(Status.CANCELLED.asRuntimeException());
      } catch (Exception e) {
        LOGGER.error("Error cancelling to sender", e);
      }
    }
  }

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

  /**
   * Converts the data sent by a {@link GrpcSendingMailbox} to a {@link TransferableBlock}.
   *
   * @param mailboxContent data sent by a GrpcSendingMailbox.
   * @return null if the received MailboxContent is a data-block with 0 rows.
   * @throws IOException if the MailboxContent cannot be converted to a TransferableBlock.
   */
  @Nullable
  private TransferableBlock fromMailboxContent(MailboxContent mailboxContent)
      throws IOException {
    ByteBuffer byteBuffer = mailboxContent.getPayload().asReadOnlyByteBuffer();
    DataBlock dataBlock = null;
    if (byteBuffer.hasRemaining()) {
      dataBlock = DataBlockUtils.getDataBlock(byteBuffer);
      if (dataBlock instanceof MetadataBlock && !dataBlock.getExceptions().isEmpty()) {
        return TransferableBlockUtils.getErrorTransferableBlock(dataBlock.getExceptions());
      }
      if (dataBlock.getNumberOfRows() > 0) {
        return new TransferableBlock(dataBlock);
      }
    }

    if (mailboxContent.getMetadataOrDefault(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY, "false").equals("true")) {
      if (dataBlock instanceof MetadataBlock) {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock(((MetadataBlock) dataBlock).getStats());
      }
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }

    return null;
  }
}
