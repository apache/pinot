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
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A [PartitionGroupConsumer] implementation for the Pulsar stream
public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);
  // A reader disconnected this long is given up on, so that the consume loop recreates it: the client retries the
  // reconnect forever and only logs why the broker refuses it, while the fresh handshake fails with the refusal. 30 s
  // is the client's default operation timeout, after which a caught-up reader in the same state already fails with a
  // timeout. A broker restart or a topic unload reconnects well within it.
  @VisibleForTesting
  static final long MAX_DISCONNECTED_MS = 30_000L;

  private final Reader<byte[]> _reader;
  private MessageId _nextMessageId = null;
  private long _disconnectedSinceMs = -1;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig);
    String topicName = _config.getPulsarTopicName();
    try {
      String partitionName = topicName + "-partition-" + partition;
      _reader = _pulsarClient.newReader().topic(partitionName).startMessageId(MessageId.earliest)
          .startMessageIdInclusive().create();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while creating Pulsar reader for topic: %s, partition: %d", topicName,
              partition), e);
    }
    LOGGER.info("Created Pulsar reader for topic: {}, partition: {}", topicName, partition);
  }

  @Override
  public synchronized PulsarMessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
    MessageId startMessageId = ((MessageIdStreamOffset) startOffset).getMessageId();
    long nowMs = System.currentTimeMillis();
    checkConnected(nowMs);
    long endTimeMs = nowMs + timeoutMs;
    List<BytesStreamMessage> messages = new ArrayList<>();

    // Seek to the start message id if necessary
    // NOTE: Use Objects.equals() to check reference first for performance.
    if (!Objects.equals(startMessageId, _nextMessageId)) {
      try {
        _reader.seek(startMessageId);
      } catch (PulsarClientException e) {
        throw new RuntimeException("Caught exception while seeking to message id: " + startMessageId, e);
      }
    }

    long batchSizeInBytes = 0;
    // Read messages until all available messages are read, or we run out of time
    try {
      while (_reader.hasMessageAvailable()) {
        long remainingMs = endTimeMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          break;
        }
        // Bounded by the deadline: a lagging reader answers hasMessageAvailable() from its cache, and once its
        // connection drops an unbounded read waits forever
        Message<byte[]> message = _reader.readNext((int) remainingMs, TimeUnit.MILLISECONDS);
        if (message == null) {
          break;
        }
        BytesStreamMessage bytesStreamMessage = PulsarUtils.buildPulsarStreamMessage(message, _config);
        batchSizeInBytes += bytesStreamMessage.getMetadata().getRecordSerializedSize();
        messages.add(bytesStreamMessage);
      }
    } catch (PulsarClientException e) {
      throw new RuntimeException("Caught exception while fetching messages from Pulsar", e);
    }

    MessageIdStreamOffset offsetOfNextBatch;
    if (messages.isEmpty()) {
      offsetOfNextBatch = (MessageIdStreamOffset) startOffset;
    } else {
      offsetOfNextBatch = (MessageIdStreamOffset) messages.get(messages.size() - 1).getMetadata().getNextOffset();
    }
    _nextMessageId = offsetOfNextBatch.getMessageId();
    return new PulsarMessageBatch(messages, offsetOfNextBatch, _reader.hasReachedEndOfTopic(), batchSizeInBytes);
  }

  /// Throws a transient exception once the reader has stayed disconnected for [#MAX_DISCONNECTED_MS], so that the
  /// consume loop recreates it whether or not idle recreation is enabled.
  private void checkConnected(long nowMs) {
    if (_reader.isConnected()) {
      _disconnectedSinceMs = -1;
    } else if (_disconnectedSinceMs < 0) {
      _disconnectedSinceMs = nowMs;
    } else if (nowMs - _disconnectedSinceMs >= MAX_DISCONNECTED_MS) {
      throw new TransientConsumerException(new TimeoutException(
          String.format("Pulsar reader for topic: %s has been disconnected from the broker for %d ms",
              _reader.getTopic(), nowMs - _disconnectedSinceMs)));
    }
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
    super.close();
  }
}
