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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.MailboxContentStreamObserver;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


/**
 * GRPC implementation of the {@link ReceivingMailbox}.
 */
public class GrpcReceivingMailbox implements ReceivingMailbox<TransferableBlock> {
  private static final long DEFAULT_MAILBOX_INIT_TIMEOUT = 100L;
  private final GrpcMailboxService _mailboxService;
  private final String _mailboxId;
  private final CountDownLatch _initializationLatch;
  private final AtomicInteger _totalMsgReceived = new AtomicInteger(0);

  private MailboxContentStreamObserver _contentStreamObserver;

  public GrpcReceivingMailbox(String mailboxId, GrpcMailboxService mailboxService) {
    _mailboxService = mailboxService;
    _mailboxId = mailboxId;
    _initializationLatch = new CountDownLatch(1);
  }

  public void init(MailboxContentStreamObserver streamObserver) {
    if (_initializationLatch.getCount() > 0) {
      _contentStreamObserver = streamObserver;
      _initializationLatch.countDown();
    }
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
  public TransferableBlock receive()
      throws Exception {
    if (!waitForInitialize()) {
      return null;
    }
    MailboxContent mailboxContent = _contentStreamObserver.poll();
    _totalMsgReceived.incrementAndGet();
    return mailboxContent == null ? null : fromMailboxContent(mailboxContent);
  }

  @Override
  public boolean isInitialized() {
    return _initializationLatch.getCount() <= 0;
  }

  @Override
  public boolean isClosed() {
    return isInitialized() && _contentStreamObserver.isCompleted();
  }

  // TODO: fix busy wait. This should be guarded by timeout.
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
    if (byteBuffer.hasRemaining()) {
      BaseDataBlock dataBlock = DataBlockUtils.getDataBlock(byteBuffer);
      if (dataBlock instanceof MetadataBlock && !dataBlock.getExceptions().isEmpty()) {
        return TransferableBlockUtils.getErrorTransferableBlock(dataBlock.getExceptions());
      }
      if (dataBlock.getNumberOfRows() > 0) {
        return new TransferableBlock(dataBlock);
      }
    }
    return null;
  }
}
