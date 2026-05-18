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
import io.grpc.Metadata;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.ChannelUtils;
import org.apache.pinot.query.mailbox.channel.MailboxStatusObserver;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * gRPC implementation of the {@link SendingMailbox}. The gRPC stream is created on the first call to {@link #send}.
 */
public class GrpcSendingMailbox implements SendingMailbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcSendingMailbox.class);
  private static final String SEND_SCOPE = "GrpcSendingMailbox";

  private static final List<ByteString> EMPTY_BYTEBUFFER_LIST = Collections.emptyList();
  private final String _id;
  private final ChannelManager _channelManager;
  private final String _hostname;
  private final int _port;
  private final long _deadlineMs;
  private final StatMap<MailboxSendOperator.StatKey> _statMap;
  private final MailboxStatusObserver _statusObserver = new MailboxStatusObserver();
  private final int _maxByteStringSize;
  /// Indicates whether the sending side has attempted to close the mailbox (either via complete() or cancel()).
  private volatile boolean _senderSideClosed;

  /// Guards [#_readyCond]. [#_contentObserver] is mutated only by the sending thread before any waiter exists, so it
  /// does not need to be protected by this lock.
  private final ReentrantLock _readyLock = new ReentrantLock();
  /// Signalled whenever any of the predicates `awaitReady()` waits on may have changed: the gRPC outbound becomes
  /// ready, the receiver acknowledges a chunk, the receiver-side stream closes (success or error), or the sender
  /// itself is cancelled. Multiple producers fire the signal; the waiter always re-checks the predicates after
  /// waking up.
  private final Condition _readyCond = _readyLock.newCondition();

  private ClientCallStreamObserver<MailboxContent> _contentObserver;

  public GrpcSendingMailbox(String id, ChannelManager channelManager, String hostname, int port, long deadlineMs,
      StatMap<MailboxSendOperator.StatKey> statMap, int maxInboundMessageSize) {
    _id = id;
    _channelManager = channelManager;
    _hostname = hostname;
    _port = port;
    _deadlineMs = deadlineMs;
    _statMap = statMap;
    // TODO: tune the maxByteStringSize based on experiments. We know the maxInboundMessageSize on the receiver side,
    //  but we want to leave some room for extra stuff for other fields like metadata, mailbox id, etc, whose size
    //  we don't know at the time of writing into the stream as it is serialized by protobuf.
    _maxByteStringSize = Math.max(maxInboundMessageSize / 2, 1);
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public void send(MseBlock.Data data) {
    QueryThreadContext.checkTerminationAndSampleUsage(SEND_SCOPE);
    sendInternal(data, List.of());
  }

  @Override
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats) {
    if (sendInternal(block, serializedStats)) {
      LOGGER.debug("Completing mailbox: {}", _id);
      _contentObserver.onCompleted();
      _senderSideClosed = true;
    } else {
      LOGGER.warn("Trying to send EOS to the already terminated mailbox: {}", _id);
    }
  }

  /// Tries to send the block to the receiver. Returns true if the block is sent, false otherwise.
  private boolean sendInternal(MseBlock block, List<DataBuffer> serializedStats) {
    if (isTerminated() || (isEarlyTerminated() && block.isData())) {
      LOGGER.debug("==[GRPC SEND]== terminated or early terminated mailbox. Skipping sending message {} to: {}",
          block, _id);
      return false;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[GRPC SEND]== sending message " + block + " to: " + _id);
    }
    if (_contentObserver == null) {
      _contentObserver = getContentObserver();
    }
    try {
      processAndSend(block, serializedStats);
    } catch (IOException e) {
      throw new QueryException(QueryErrorCode.INTERNAL, "Failed to split and send mailbox", e);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[GRPC SEND]== message " + block + " sent to: " + _id);
    }
    return true;
  }

  private void processAndSend(MseBlock block, List<DataBuffer> serializedStats)
      throws IOException {
    processAndSend(block, serializedStats, false);
  }

  /// Same as [#processAndSend(MseBlock, List)] but with a flag to bypass the [#awaitReady] gate. Used by the
  /// cancel / close paths to push the error EOS through without waiting on back-pressure relief — without this,
  /// a cancel issued while the receiver is congested would itself block until the receiver drains.
  private void processAndSend(MseBlock block, List<DataBuffer> serializedStats, boolean bypassReady)
      throws IOException {
    _statMap.merge(MailboxSendOperator.StatKey.RAW_MESSAGES, 1);
    long start = System.currentTimeMillis();
    try {
      DataBlock dataBlock = MseBlockSerializer.toDataBlock(block, serializedStats);
      int sizeInBytes = processAndSend(dataBlock, bypassReady);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Serialized block: {} to {} bytes", block, sizeInBytes);
      }
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZED_BYTES, sizeInBytes);
    } finally {
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZATION_TIME_MS, System.currentTimeMillis() - start);
    }
  }

  /**
   * Process the data block to split it into multiple ByteStrings that fit into the maxByteStringSize, and send them
   * one by one.
   */
  protected int processAndSend(DataBlock dataBlock)
      throws IOException {
    return processAndSend(dataBlock, false);
  }

  protected int processAndSend(DataBlock dataBlock, boolean bypassReady)
      throws IOException {
    List<ByteString> byteStrings = toByteStrings(dataBlock, _maxByteStringSize);
    int sizeInBytes = 0;
    for (ByteString byteString : byteStrings) {
      sizeInBytes += byteString.size();
    }
    Iterator<ByteString> byteStringIt = byteStrings.iterator();
    while (byteStringIt.hasNext()) {
      ByteString byteString = byteStringIt.next();
      boolean waitForMore = byteStringIt.hasNext();
      sendContent(byteString, waitForMore, bypassReady);
    }
    return sizeInBytes;
  }

  @Override
  public void cancel(Throwable t) {
    if (isTerminated()) {
      LOGGER.debug("Already terminated mailbox: {}", _id);
      return;
    }
    _senderSideClosed = true;
    // Wake any sender thread blocked in awaitReady() so it observes the termination and exits without racing this
    // cancel path on the same observer.
    wakeWaiters();
    LOGGER.debug("Cancelling mailbox: {}", _id);
    if (_contentObserver == null) {
      _contentObserver = getContentObserver();
    }
    try {
      String msg = t != null ? t.getMessage() : "Unknown";
      // NOTE: DO NOT use onError() because it will terminate the stream, and receiver might not get the callback
      MseBlock errorBlock = ErrorMseBlock.fromError(
          QueryErrorCode.QUERY_CANCELLATION, "Cancelled by sender with exception: " + msg);
      processAndSend(errorBlock, List.of(), /* bypassReady */ true);
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
    // _senderSideClosed is set when the sending side has attempted to close the mailbox (either via complete() or
    // cancel()). But we also need to return true the gRPC status observer has observed that the connection is closed
    // (ie due to timeout)
    return _senderSideClosed || _statusObserver.isFinished();
  }

  private ClientCallStreamObserver<MailboxContent> getContentObserver() {
    Metadata metadata = new Metadata();
    metadata.put(ChannelUtils.MAILBOX_ID_METADATA_KEY, _id);

    // We wrap `_statusObserver` in a ClientResponseObserver so we can register the on-ready handler through
    // `beforeStart` — gRPC rejects setOnReadyHandler() if it is called after open() returns. Wrapping (rather than
    // making MailboxStatusObserver itself a ClientResponseObserver) keeps the back-pressure plumbing local to this
    // class. The wrapper delegates the data callbacks unchanged, and signals our `_readyCond` on stream close so a
    // blocked sender wakes up to observe `_statusObserver.isFinished()` becoming true.
    ClientResponseObserver<MailboxContent, MailboxStatus> responseObserver =
        new ClientResponseObserver<MailboxContent, MailboxStatus>() {
          @Override
          public void beforeStart(ClientCallStreamObserver<MailboxContent> requestStream) {
            // Fires on a gRPC channel/Netty thread whenever isReady() transitions false -> true. Just signal; the
            // sender re-checks the predicate after waking.
            requestStream.setOnReadyHandler(GrpcSendingMailbox.this::wakeWaiters);
          }

          @Override
          public void onNext(MailboxStatus value) {
            _statusObserver.onNext(value);
          }

          @Override
          public void onError(Throwable t) {
            try {
              _statusObserver.onError(t);
            } finally {
              wakeWaiters();
            }
          }

          @Override
          public void onCompleted() {
            try {
              _statusObserver.onCompleted();
            } finally {
              wakeWaiters();
            }
          }
        };

    return (ClientCallStreamObserver<MailboxContent>) PinotMailboxGrpc.newStub(
            _channelManager.getChannel(_hostname, _port))
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
        .withDeadlineAfter(_deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
        .open(responseObserver);
  }

  protected void sendContent(ByteString byteString, boolean waitForMore) {
    sendContent(byteString, waitForMore, false);
  }

  protected void sendContent(ByteString byteString, boolean waitForMore, boolean bypassReady) {
    if (!awaitReady(bypassReady)) {
      // Either the mailbox was cancelled while we were waiting (normal path) or the gRPC stream is already dead
      // (bypass path). Either way, skip the send.
      return;
    }
    MailboxContent content = MailboxContent.newBuilder()
        .setMailboxId(_id)
        .setPayload(byteString)
        .setWaitForMore(waitForMore)
        .build();
    _contentObserver.onNext(content);
  }

  /// Blocks the calling (query-runner) thread until the gRPC client outbound is ready to accept another chunk, the
  /// mailbox terminates, or the query deadline is exceeded. Returns `true` if the caller should proceed with the
  /// `onNext` call, `false` if the send should be skipped.
  ///
  /// Two modes:
  ///  * `bypassReady = false` (normal user sends): waits for [#_contentObserver]`.isReady()` to flip, but exits
  ///    early if the mailbox has been [#isTerminated terminated] in the meantime. This makes [#cancel] able to
  ///    unblock a blocked sender promptly.
  ///  * `bypassReady = true` (the cancel / close paths): never waits, never short-circuits on `isTerminated()`.
  ///    The only check is whether the underlying gRPC stream is already dead ([MailboxStatusObserver#isFinished]),
  ///    in which case there is nothing to send. This is what lets a cancel issued while the receiver is congested
  ///    push its error EOS through without blocking behind back-pressure of its own.
  ///
  /// Spool note: when a stage spools to multiple destination mailboxes via [BlockExchange.BlockExchangeSendingMailbox],
  /// one slow downstream worker will throttle the whole spool — every wrapped mailbox is awaited in turn from the same
  /// OpChain thread. This is intentional: the alternative (forking each destination onto its own thread) would
  /// re-introduce the unbounded outbound queue we are fixing here. Spool throughput is gated by the slowest consumer.
  private boolean awaitReady(boolean bypassReady) {
    if (bypassReady) {
      return !_statusObserver.isFinished();
    }
    // Fast path: don't take the lock if the observer is already ready. This is the common case in the steady state.
    if (_contentObserver.isReady()) {
      return true;
    }
    if (isTerminated()) {
      return false;
    }
    _readyLock.lock();
    try {
      while (!_contentObserver.isReady()) {
        if (isTerminated()) {
          return false;
        }
        // Cooperative termination poll so query cancellation can unblock the wait through the same mechanism we use
        // elsewhere in the MSE.
        QueryThreadContext.checkTerminationAndSampleUsage(SEND_SCOPE);
        long remainingMs = _deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          throw new QueryException(QueryErrorCode.EXECUTION_TIMEOUT,
              "Deadline exceeded while waiting for gRPC outbound to become ready on mailbox: " + _id);
        }
        try {
          _readyCond.await(remainingMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new QueryException(QueryErrorCode.INTERNAL,
              "Interrupted while waiting for gRPC outbound to become ready on mailbox: " + _id, e);
        }
      }
      return true;
    } finally {
      _readyLock.unlock();
    }
  }

  /// Wakes every waiter in [#awaitReady]. Called from the gRPC ready handler, from [MailboxStatusObserver] events,
  /// and from [#cancel] / [#close]. Must be cheap because it can fire from Netty event-loop threads.
  private void wakeWaiters() {
    _readyLock.lock();
    try {
      _readyCond.signalAll();
    } finally {
      _readyLock.unlock();
    }
  }

  @Override
  public String toString() {
    return "g" + _id;
  }

  private static class MseBlockSerializer implements MseBlock.Visitor<DataBlock, List<DataBuffer>> {
    private static final MseBlockSerializer INSTANCE = new MseBlockSerializer();

    public static DataBlock toDataBlock(MseBlock block, List<DataBuffer> serializedStats) {
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
        return MetadataBlock.newErrorWithStats(block.getStageId(), block.getWorkerId(), block.getServerId(),
            errorMessagesByInt, serializedStats);
      } else {
        return MetadataBlock.newError(block.getStageId(), block.getWorkerId(), block.getServerId(), errorMessagesByInt);
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

  @Override
  public void close()
      throws Exception {
    if (!isTerminated()) {
      String errorMsg = "Closing gPRC mailbox without proper EOS message";
      RuntimeException ex = new RuntimeException(errorMsg);
      ex.fillInStackTrace();
      LOGGER.error(errorMsg, ex);
      _senderSideClosed = true;
      wakeWaiters();

      MseBlock errorBlock = ErrorMseBlock.fromError(QueryErrorCode.INTERNAL, errorMsg);
      if (_contentObserver != null) {
        processAndSend(errorBlock, List.of(), /* bypassReady */ true);
      }
    }
  }
}
