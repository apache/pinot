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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.ChannelUtils;
import org.apache.pinot.query.mailbox.channel.MailboxStatusStreamObserver;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * gRPC implementation of the {@link SendingMailbox}. The gRPC stream is created on the first call to {@link #send}.
 */
public class GrpcSendingMailbox implements SendingMailbox<TransferableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcSendingMailbox.class);
  private final String _mailboxId;
  private final AtomicBoolean _initialized = new AtomicBoolean(false);

  private StreamObserver<MailboxContent> _mailboxContentStreamObserver;
  private final Function<Long, StreamObserver<MailboxContent>> _mailboxContentStreamObserverSupplier;
  private final MailboxStatusStreamObserver _statusObserver;
  private final long _deadlineMs;

  public GrpcSendingMailbox(String mailboxId, MailboxStatusStreamObserver statusObserver,
      Function<Long, StreamObserver<MailboxContent>> contentStreamObserverSupplier, long deadlineMs) {
    _mailboxId = mailboxId;
    _mailboxContentStreamObserverSupplier = contentStreamObserverSupplier;
    _statusObserver = statusObserver;
    _deadlineMs = deadlineMs;
  }

  @Override
  public void send(TransferableBlock block)
      throws Exception {
    if (!_initialized.get()) {
      open();
    }
    Preconditions.checkState(!_statusObserver.isFinished(),
        "Called send when stream is already closed for mailbox=" + _mailboxId);
    MailboxContent data = toMailboxContent(block.getDataBlock());
    _mailboxContentStreamObserver.onNext(data);
  }

  @Override
  public void complete()
      throws Exception {
    _mailboxContentStreamObserver.onCompleted();
  }

  @Override
  public boolean isInitialized() {
    return _initialized.get();
  }

  @Override
  public void cancel(Throwable t) {
    if (_initialized.get() && !_statusObserver.isFinished()) {
      LOGGER.warn("GrpcSendingMailbox={} cancelling stream", _mailboxId);
      try {
        _mailboxContentStreamObserver.onError(Status.fromThrowable(
            new RuntimeException("Cancelled by the sender")).asRuntimeException());
      } catch (Exception e) {
        // TODO: We don't necessarily need to log this since this is relatively quite likely to happen. Logging this
        //  anyways as info for now so we can see how frequently this happens.
        LOGGER.info("Unexpected error issuing onError to MailboxContentStreamObserver: {}", e.getMessage());
      }
    }
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  private void open() {
    _mailboxContentStreamObserver = _mailboxContentStreamObserverSupplier.apply(_deadlineMs);
    _initialized.set(true);
    // send a begin-of-stream message.
    _mailboxContentStreamObserver.onNext(MailboxContent.newBuilder().setMailboxId(_mailboxId)
        .putMetadata(ChannelUtils.MAILBOX_METADATA_BEGIN_OF_STREAM_KEY, "true").build());
  }

  private MailboxContent toMailboxContent(DataBlock dataBlock) {
    try {
      Mailbox.MailboxContent.Builder builder = Mailbox.MailboxContent.newBuilder().setMailboxId(_mailboxId)
          .setPayload(ByteString.copyFrom(dataBlock.toBytes()));
      if (dataBlock instanceof MetadataBlock) {
        builder.putMetadata(ChannelUtils.MAILBOX_METADATA_END_OF_STREAM_KEY, "true");
      }
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException("Error converting to mailbox content", e);
    }
  }
}
