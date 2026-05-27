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
package org.apache.pinot.query.mailbox.flight;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.GrpcSendingMailbox;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link SendingMailbox} that sends {@link ArrowBlock} data over Arrow Flight and delegates EOS blocks to gRPC.
 *
 * <p>The gRPC mailbox is kept for EOS / error signalling so that the receiving side (which still uses gRPC for
 * control messages) can detect stream termination. Data blocks are sent via Flight's {@code acceptPut} endpoint
 * in configurable-sized record-batch chunks.
 *
 * <p>The Flight stream is opened lazily on the first {@link #send(MseBlock.Data)} call.
 */
public class FlightSendingMailbox implements SendingMailbox {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightSendingMailbox.class);
  /** Maximum rows per Arrow record batch sent over the wire. */
  private static final int MAX_BATCH_ROWS = 16_192;

  private final String _id;
  private final FlightChannelManager _channelManager;
  private final String _hostname;
  private final int _port;
  private final long _deadlineMs;
  private final StatMap<MailboxSendOperator.StatKey> _statMap;
  private final GrpcSendingMailbox _grpcMailbox;
  private final BufferAllocator _allocator;

  private FlightClient _client;
  // Kept open across send(Data) calls; completed/closed on EOS or cancel.
  private FlightClient.ClientStreamListener _listener;
  private VectorSchemaRoot _sendRoot;

  public FlightSendingMailbox(String id, FlightChannelManager channelManager, String hostname, int port,
      long deadlineMs, StatMap<MailboxSendOperator.StatKey> statMap, GrpcSendingMailbox grpcMailbox,
      BufferAllocator allocator) {
    _id = id;
    _channelManager = channelManager;
    _hostname = hostname;
    _port = port;
    _deadlineMs = deadlineMs;
    _statMap = statMap;
    _grpcMailbox = grpcMailbox;
    _allocator = allocator;
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public void send(MseBlock.Data data) {
    if (isTerminated() || isEarlyTerminated()) {
      LOGGER.info("Mailbox {} is terminated, skipping data send", _id);
      if (data instanceof ArrowBlock) {
        ((ArrowBlock) data).release();
      }
      return;
    }
    _statMap.merge(MailboxSendOperator.StatKey.RAW_MESSAGES, 1);
    long start = System.currentTimeMillis();
    try {
      sendDataBlock(data);
    } finally {
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZATION_TIME_MS, System.currentTimeMillis() - start);
    }
  }

  @Override
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats) {
    // Complete the Flight stream before signalling EOS over gRPC, so the receiver has all data
    // before it sees the termination signal.
    completeFlightStream();
    _grpcMailbox.send(block, serializedStats);
  }

  private void sendDataBlock(MseBlock.Data data) {
    ArrowBlock arrowBlock = data.asArrow();
    VectorSchemaRoot root = arrowBlock.getDataBlock().getRoot();
    DictionaryProvider dictionaryProvider = arrowBlock.getDataBlock().getDictionaryProvider();

    // Open the stream lazily on the first data block.
    if (_listener == null) {
      FlightDescriptor descriptor = FlightDescriptor.path(_id, Long.toString(_deadlineMs));
      long remainingMs = _deadlineMs - System.currentTimeMillis();
      _sendRoot = VectorSchemaRoot.create(root.getSchema(), _allocator);
      _listener = getClient().startPut(descriptor, new AsyncPutListener(),
          CallOptions.timeout(remainingMs, TimeUnit.MILLISECONDS), null);
      _listener.start(_sendRoot, dictionaryProvider);
    }

    int totalRows = arrowBlock.getNumRows();
    List<FieldVector> sendVectors = _sendRoot.getFieldVectors();
    if (totalRows <= MAX_BATCH_ROWS) {
      // Common case: entire block fits in one batch — transfer vectors directly (zero-copy)
      for (int i = 0; i < sendVectors.size(); i++) {
        root.getVector(i).makeTransferPair(sendVectors.get(i)).transfer();
      }
      _sendRoot.setRowCount(totalRows);
      _listener.putNext();
    } else {
      // Multi-batch: use splitAndTransfer per range (avoids the intermediate slice root)
      int offset = 0;
      while (offset < totalRows) {
        int batchSize = Math.min(MAX_BATCH_ROWS, totalRows - offset);
        for (int i = 0; i < sendVectors.size(); i++) {
          root.getVector(i).makeTransferPair(sendVectors.get(i))
              .splitAndTransfer(offset, batchSize);
        }
        _sendRoot.setRowCount(batchSize);
        _listener.putNext();
        offset += batchSize;
      }
    }
  }

  private void completeFlightStream() {
    if (_listener != null) {
      try {
        _listener.completed();
      } catch (Exception e) {
        LOGGER.warn("Exception completing Flight stream for mailbox: {}", _id, e);
      } finally {
        _listener = null;
        if (_sendRoot != null) {
          _sendRoot.close();
          _sendRoot = null;
        }
      }
    }
  }

  @Override
  public void cancel(Throwable t) {
    completeFlightStream();
    if (!isTerminated()) {
      try {
        _grpcMailbox.cancel(t);
      } catch (Exception e) {
        LOGGER.debug("Exception cancelling gRPC mailbox: {}", _id, e);
      }
    }
  }

  @Override
  public void close() {
    completeFlightStream();
  }

  @Override
  public boolean isEarlyTerminated() {
    return _grpcMailbox.isEarlyTerminated();
  }

  @Override
  public boolean isTerminated() {
    return _grpcMailbox.isTerminated();
  }

  private FlightClient getClient() {
    if (_client == null) {
      _client = _channelManager.getClient(_hostname, _port);
    }
    return _client;
  }

  @Override
  public String toString() {
    return "FlightSendingMailbox[" + _id + "]";
  }
}
