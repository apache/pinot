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

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.MailboxStatusObserver;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * gRPC implementation of the {@link SendingMailbox}. The gRPC stream is created on the first call to {@link #send}.
 */
public class GrpcSendingMailbox implements SendingMailbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcSendingMailbox.class);

  private final PinotConfiguration _config;
  private final String _id;
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _port;
  private final long _deadlineMs;
  private final StatMap<MailboxSendOperator.StatKey> _statMap;
  private final MailboxStatusObserver _statusObserver = new MailboxStatusObserver();

  private StreamObserver<MailboxContent> _contentObserver;

  public GrpcSendingMailbox(
      PinotConfiguration config, String id, ChannelManager channelManager, String hostname, int port, long deadlineMs,
      StatMap<MailboxSendOperator.StatKey> statMap) {
    _config = config;
    _id = id;
    _channelManager = channelManager;
    _hostname = hostname;
    _port = port;
    _deadlineMs = deadlineMs;
    _statMap = statMap;
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public void send(MseBlock.Data data)
      throws IOException, TimeoutException {
    sendInternal(data, List.of());
  }

  @Override
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats)
      throws IOException, TimeoutException {
    sendInternal(block, serializedStats);
  }

  private void sendInternal(MseBlock block, List<DataBuffer> serializedStats)
      throws IOException {
    if (isTerminated() || (isEarlyTerminated() && block.isData())) {
      LOGGER.debug("==[GRPC SEND]== terminated or early terminated mailbox. Skipping sending message {} to: {}",
          block, _id);
      return;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[GRPC SEND]== sending message " + block + " to: " + _id);
    }
    if (_contentObserver == null) {
      _contentObserver = getContentObserver();
    }
    for (MailboxContent content: toMailboxContents(block, serializedStats)) {
      _contentObserver.onNext(content);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[GRPC SEND]== message " + block + " sent to: " + _id);
    }
  }

  @Override
  public void complete() {
    if (isTerminated()) {
      LOGGER.debug("Already terminated mailbox: {}", _id);
      return;
    }
    LOGGER.debug("Completing mailbox: {}", _id);
    _contentObserver.onCompleted();
  }

  @Override
  public void cancel(Throwable t) {
    if (isTerminated()) {
      LOGGER.debug("Already terminated mailbox: {}", _id);
      return;
    }
    LOGGER.debug("Cancelling mailbox: {}", _id);
    if (_contentObserver == null) {
      _contentObserver = getContentObserver();
    }
    try {
      String msg = t != null ? t.getMessage() : "Unknown";
      // NOTE: DO NOT use onError() because it will terminate the stream, and receiver might not get the callback
      MseBlock errorBlock = ErrorMseBlock.fromError(
          QueryErrorCode.QUERY_CANCELLATION, "Cancelled by sender with exception: " + msg);
      for (MailboxContent content: toMailboxContents(errorBlock, List.of())) {
        _contentObserver.onNext(content);
      }
      _contentObserver.onCompleted();
    } catch (Exception e) {
      // Exception can be thrown if the stream is already closed, so we simply ignore it
      LOGGER.debug("Caught exception cancelling mailbox: {}", _id, e);
    }
  }

  @Override
  public boolean isEarlyTerminated() {
    return _statusObserver.isEarlyTerminated();
  }

  @Override
  public boolean isTerminated() {
    return _statusObserver.isFinished();
  }

  private StreamObserver<MailboxContent> getContentObserver() {
    return PinotMailboxGrpc.newStub(_channelManager.getChannel(_hostname, _port))
        .withDeadlineAfter(_deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .open(_statusObserver);
  }

  private List<MailboxContent> toMailboxContents(MseBlock block, List<DataBuffer> serializedStats)
      throws IOException {
    _statMap.merge(MailboxSendOperator.StatKey.RAW_MESSAGES, 1);
    long start = System.currentTimeMillis();
    try {
      DataBlock dataBlock = MseBlockSerializer.toDataBlock(block, serializedStats);
      // so far we ensure payload is not bigger than maxBlockSize/2, we can fine tune this later
      List<ByteString> byteStrings = DataBlockUtils.toByteStrings(dataBlock, getMaxBlockSize() / 2);
      int sizeInBytes = byteStrings.stream().map(ByteString::size).reduce(0, Integer::sum);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Serialized block: {} to {} bytes", block, sizeInBytes);
      }
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZED_BYTES, sizeInBytes);
      List<MailboxContent> contents = new ArrayList<>();
      for (int i = 0; i < byteStrings.size(); i++) {
        contents.add(MailboxContent.newBuilder()
            .setMailboxId(_id)
            .setPayload(byteStrings.get(i))
            .setWaitForMore(i < byteStrings.size() - 1)
            .build());
      }
      return contents;
    } catch (Throwable t) {
      LOGGER.warn("Caught exception while serializing block: {}", block, t);
      throw t;
    } finally {
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZATION_TIME_MS, System.currentTimeMillis() - start);
    }
  }

  private int getMaxBlockSize() {
    return _config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES
    );
  }

  @Override
  public String toString() {
    return "g" + _id;
  }

  private static class MseBlockSerializer implements MseBlock.Visitor<DataBlock, List<DataBuffer>> {
    private static final MseBlockSerializer INSTANCE = new MseBlockSerializer();

    public static DataBlock toDataBlock(MseBlock block, List<DataBuffer> serializedStats)
        throws IOException {
      return block.accept(INSTANCE, serializedStats);
    }

    @Override
    public DataBlock visit(RowHeapDataBlock block, List<DataBuffer> serializedStats) {
      // this is already guaranteed by the SendingMailbox.send(MseBlock.Data) signature, but just to be sure...
      if (serializedStats != null && !serializedStats.isEmpty()) {
        throw new UnsupportedOperationException("Cannot serialize stats with RowHeapDataBlock");
      }
      return block.asSerialized().getDataBlock();
    }

    @Override
    public DataBlock visit(SerializedDataBlock block, List<DataBuffer> serializedStats) {
      // this is already guaranteed by the SendingMailbox.send(MseBlock.Data) signature, but just to be sure...
      if (serializedStats != null && !serializedStats.isEmpty()) {
        throw new UnsupportedOperationException("Cannot serialize stats with SerializedDataBlock");
      }
      return block.getDataBlock();
    }

    @Override
    public DataBlock visit(SuccessMseBlock block, List<DataBuffer> serializedStats) {
      if (serializedStats != null && !serializedStats.isEmpty()) {
        return MetadataBlock.newEosWithStats(serializedStats);
      } else {
        return MetadataBlock.newEos();
      }
    }

    @Override
    public DataBlock visit(ErrorMseBlock block, List<DataBuffer> serializedStats) {
      Map<QueryErrorCode, String> errorMessagesByCode = block.getErrorMessages();
      Map<Integer, String> errorMessagesByInt = Maps.newHashMapWithExpectedSize(errorMessagesByCode.size());
      errorMessagesByCode.forEach((code, message) -> errorMessagesByInt.put(code.getId(), message));
      if (serializedStats != null && !serializedStats.isEmpty()) {
        return MetadataBlock.newErrorWithStats(errorMessagesByInt, serializedStats);
      } else {
        return MetadataBlock.newError(errorMessagesByInt);
      }
    }
  }
}
