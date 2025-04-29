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
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.datablock.DataBlock;
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

  private static final List<ByteString> EMPTY_BYTEBUFFER_LIST = Collections.emptyList();
  private final String _id;
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _port;
  private final long _deadlineMs;
  private final StatMap<MailboxSendOperator.StatKey> _statMap;
  private final MailboxStatusObserver _statusObserver = new MailboxStatusObserver();
  private final int _maxByteStringSize;

  private StreamObserver<MailboxContent> _contentObserver;

  public GrpcSendingMailbox(
      PinotConfiguration config, String id, ChannelManager channelManager, String hostname, int port, long deadlineMs,
      StatMap<MailboxSendOperator.StatKey> statMap) {
    _id = id;
    _channelManager = channelManager;
    _hostname = hostname;
    _port = port;
    _deadlineMs = deadlineMs;
    _statMap = statMap;
    // so far we ensure payload is not bigger than maxBlockSize/2, we can fine tune this later
    _maxByteStringSize = Math.max(config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES
    ) / 2, 1);
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
    splitAndSend(block, serializedStats);
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
      splitAndSend(errorBlock, List.of());
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

  private void splitAndSend(MseBlock block, List<DataBuffer> serializedStats)
      throws IOException {
    _statMap.merge(MailboxSendOperator.StatKey.RAW_MESSAGES, 1);
    long start = System.currentTimeMillis();
    try {
      DataBlock dataBlock = MseBlockSerializer.toDataBlock(block, serializedStats);
      List<ByteString> byteStrings = toByteStrings(dataBlock, _maxByteStringSize);
      int sizeInBytes = byteStrings.stream().mapToInt(ByteString::size).reduce(0, Integer::sum);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Serialized block: {} to {} bytes", block, sizeInBytes);
      }
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZED_BYTES, sizeInBytes);

      Iterator<ByteString> byteStringIt = byteStrings.iterator();
      while (byteStringIt.hasNext()) {
        ByteString byteString = byteStringIt.next();
        boolean waitForMore = byteStringIt.hasNext();
        sendContent(byteString, waitForMore);
      }
    } catch (Throwable t) {
      LOGGER.warn("Caught exception while serializing block: {}", block, t);
      throw t;
    } finally {
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZATION_TIME_MS, System.currentTimeMillis() - start);
    }
  }

  private void sendContent(ByteString byteString, boolean waitForMore) {
    MailboxContent content = MailboxContent.newBuilder()
        .setMailboxId(_id)
        .setPayload(byteString)
        .setWaitForMore(waitForMore)
        .build();
    _contentObserver.onNext(content);
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

  /// Transforms a DataBlock into a list of ByteStrings of maxByteStringSize (except for the last one).
  /// This method will consume the dataBlock.serialize() output.
  static List<ByteString> toByteStrings(DataBlock dataBlock, int maxByteStringSize)
      throws IOException {
    return toByteStrings(dataBlock.serialize(), maxByteStringSize);
  }

  /// Transforms a list of ByteBuffers into a list of ByteStrings of maxByteStringSize (except for the last one).
  /// This method will consume the original ByteBuffers.
  static List<ByteString> toByteStrings(List<ByteBuffer> bytes, int maxByteStringSize) {
    if (bytes.isEmpty()) {
      return EMPTY_BYTEBUFFER_LIST;
    }

    int totalBytes = 0;
    for (ByteBuffer bb : bytes) {
      totalBytes += bb.remaining();
    }
    int initialCapacity = (totalBytes / maxByteStringSize) + 1;
    List<ByteString> result = new ArrayList<>(initialCapacity);

    ByteString acc = ByteString.EMPTY;
    int available = maxByteStringSize;

    for (ByteBuffer bb: bytes) {
      while (bb.hasRemaining()) {
        if (bb.remaining() < available) {
          available -= bb.remaining();
          acc = acc.concat(UnsafeByteOperations.unsafeWrap(bb));
          bb.position(bb.limit()); // just exhaust it
        } else {
          int oldLimit = bb.limit();
          bb.limit(bb.position() + available);
          acc = acc.concat(UnsafeByteOperations.unsafeWrap(bb));
          bb.position(bb.limit()); // consume the copied chunk
          bb.limit(oldLimit);
          result.add(acc);
          acc = ByteString.EMPTY;
          available = maxByteStringSize;
        }
      }
    }

    if (!acc.isEmpty()) {
      result.add(acc);
    }

    return result;
  }
}
