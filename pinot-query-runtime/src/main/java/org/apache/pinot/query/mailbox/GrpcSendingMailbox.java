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
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.MailboxStatusObserver;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * gRPC implementation of the {@link SendingMailbox}. The gRPC stream is created on the first call to {@link #send}.
 */
public class GrpcSendingMailbox implements SendingMailbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcSendingMailbox.class);

  private final String _id;
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _port;
  private final long _deadlineMs;
  private final MailboxStatusObserver _statusObserver = new MailboxStatusObserver();

  private StreamObserver<MailboxContent> _contentObserver;

  public GrpcSendingMailbox(String id, ChannelManager channelManager, String hostname, int port, long deadlineMs) {
    _id = id;
    _channelManager = channelManager;
    _hostname = hostname;
    _port = port;
    _deadlineMs = deadlineMs;
  }

  @Override
  public void send(TransferableBlock block)
      throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[GRPC SEND]== sending data to: " + _id);
    }
    if (_contentObserver == null) {
      _contentObserver = getContentObserver();
    }
    Preconditions.checkState(!_statusObserver.isFinished(), "Mailbox: %s is already closed", _id);
    _contentObserver.onNext(toMailboxContent(block));
  }

  @Override
  public void complete() {
    _contentObserver.onCompleted();
  }

  @Override
  public void cancel(Throwable t) {
    if (!_statusObserver.isFinished()) {
      LOGGER.debug("Cancelling mailbox: {}", _id);
      if (_contentObserver == null) {
        _contentObserver = getContentObserver();
      }
      try {
        String msg = t != null ? t.getMessage() : "Unknown";
        // NOTE: DO NOT use onError() because it will terminate the stream, and receiver might not get the callback
        _contentObserver.onNext(toMailboxContent(TransferableBlockUtils.getErrorTransferableBlock(
            new RuntimeException("Cancelled by sender with exception: " + msg, t))));
        _contentObserver.onCompleted();
      } catch (Exception e) {
        // Exception can be thrown if the stream is already closed, so we simply ignore it
        LOGGER.debug("Caught exception cancelling mailbox: {}", _id, e);
      }
    }
  }

  private StreamObserver<MailboxContent> getContentObserver() {
    return PinotMailboxGrpc.newStub(_channelManager.getChannel(_hostname, _port))
        .withDeadlineAfter(_deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS).open(_statusObserver);
  }

  private MailboxContent toMailboxContent(TransferableBlock block)
      throws IOException {
    DataBlock dataBlock = block.getDataBlock();
    byte[] bytes = dataBlock.toBytes();
    ByteString byteString = UnsafeByteOperations.unsafeWrap(bytes);
    return MailboxContent.newBuilder().setMailboxId(_id).setPayload(byteString)
        .build();
  }
}
