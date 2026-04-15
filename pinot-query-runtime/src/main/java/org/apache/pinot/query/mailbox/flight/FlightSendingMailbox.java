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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.GrpcSendingMailbox;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
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

  private FlightClient _client;

  public FlightSendingMailbox(String id, FlightChannelManager channelManager, String hostname, int port,
      long deadlineMs, StatMap<MailboxSendOperator.StatKey> statMap, GrpcSendingMailbox grpcMailbox) {
    _id = id;
    _channelManager = channelManager;
    _hostname = hostname;
    _port = port;
    _deadlineMs = deadlineMs;
    _statMap = statMap;
    _grpcMailbox = grpcMailbox;
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  @Override
  public void send(MseBlock.Data data)
      throws IOException, TimeoutException {
    if (isTerminated() || isEarlyTerminated()) {
      LOGGER.debug("Mailbox {} is terminated, skipping data send", _id);
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
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats)
      throws IOException, TimeoutException {
    // EOS / error signalling goes through gRPC so the receiver detects stream termination
    _grpcMailbox.send(block, serializedStats);
  }

  private void sendDataBlock(MseBlock.Data data) {
    ArrowBlock arrowBlock = data.asArrow();
    VectorSchemaRoot root = arrowBlock.getDataBlock().getRoot();
    DictionaryProvider dictionaryProvider = arrowBlock.getDataBlock().getDictionaryProvider();

    FlightDescriptor descriptor = FlightDescriptor.path(_id);
    long remainingMs = _deadlineMs - System.currentTimeMillis();
    FlightClient.ClientStreamListener listener = getClient().startPut(descriptor, new AsyncPutListener(),
        CallOptions.timeout(remainingMs, TimeUnit.MILLISECONDS), null);

    try (VectorSchemaRoot sendRoot = VectorSchemaRoot.create(root.getSchema(), ArrowBuffers.getLocalAllocator())) {
      listener.start(sendRoot, dictionaryProvider);
      int offset = 0;
      int totalRows = arrowBlock.getNumRows();
      List<FieldVector> sendVectors = sendRoot.getFieldVectors();
      while (offset < totalRows) {
        int batchSize = Math.min(MAX_BATCH_ROWS, totalRows - offset);
        try (VectorSchemaRoot slice = root.slice(offset, batchSize)) {
          List<FieldVector> sliceVectors = slice.getFieldVectors();
          for (int i = 0; i < sliceVectors.size(); i++) {
            sliceVectors.get(i).makeTransferPair(sendVectors.get(i)).transfer();
          }
          sendRoot.setRowCount(batchSize);
          listener.putNext();
        }
        offset += batchSize;
      }
    } finally {
      listener.completed();
    }
  }

  @Override
  public void complete() {
    if (!isTerminated()) {
      _grpcMailbox.complete();
    }
  }

  @Override
  public void cancel(Throwable t) {
    if (!isTerminated()) {
      try {
        _grpcMailbox.cancel(t);
      } catch (Exception e) {
        LOGGER.debug("Exception cancelling gRPC mailbox: {}", _id, e);
      }
    }
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
