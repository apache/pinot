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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.query.mailbox.channel.ChannelUtils;
import org.apache.pinot.query.mailbox.channel.MailboxStatusStreamObserver;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * GRPC implementation of the {@link SendingMailbox}.
 */
public class GrpcSendingMailbox implements SendingMailbox<TransferableBlock> {
  private final String _mailboxId;
  private final AtomicBoolean _initialized = new AtomicBoolean(false);

  private StreamObserver<MailboxContent> _mailboxContentStreamObserver;
  private final Supplier<StreamObserver<MailboxContent>> _mailboxContentStreamObserverSupplier;
  private final MailboxStatusStreamObserver _statusObserver;

  public GrpcSendingMailbox(String mailboxId, MailboxStatusStreamObserver statusObserver,
      Supplier<StreamObserver<MailboxContent>> contentStreamObserverSupplier) {
    _mailboxId = mailboxId;
    _mailboxContentStreamObserverSupplier = contentStreamObserverSupplier;
    _statusObserver = statusObserver;
    _initialized.set(false);
  }

  @Override
  public void send(TransferableBlock block)
      throws UnsupportedOperationException {
    if (!_initialized.get()) {
      // initialization is special
      open();
    }
    if (_statusObserver.isFinished()) {
      // If server has already finished, the stream is already closed
      // TODO: Stop processing if receiver has already hung-up.
      return;
    }
    MailboxContent data = toMailboxContent(block.getDataBlock());
    _mailboxContentStreamObserver.onNext(data);
  }

  @Override
  public void complete() {
    _mailboxContentStreamObserver.onCompleted();
  }

  @Override
  public void open() {
    _mailboxContentStreamObserver = _mailboxContentStreamObserverSupplier.get();
    // send a begin-of-stream message.
    _mailboxContentStreamObserver.onNext(MailboxContent.newBuilder().setMailboxId(_mailboxId)
        .putMetadata(ChannelUtils.MAILBOX_METADATA_BEGIN_OF_STREAM_KEY, "true").build());
    _initialized.set(true);
  }

  @Override
  public String getMailboxId() {
    return _mailboxId;
  }

  @Override
  public void cancel(Throwable t) {
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
