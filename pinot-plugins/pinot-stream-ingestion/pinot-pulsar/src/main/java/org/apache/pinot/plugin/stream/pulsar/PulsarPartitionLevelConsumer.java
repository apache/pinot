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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PartitionGroupConsumer} implementation for the Pulsar stream
 */
public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);
  private final Reader<byte[]> _reader;
  private MessageId _nextMessageId = null;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig);
    String topicName = _config.getPulsarTopicName();
    try {
      List<String> partitions = _pulsarClient.getPartitionsForTopic(topicName).get();
      _reader = _pulsarClient.newReader().topic(partitions.get(partition)).startMessageId(MessageId.earliest)
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
    long endTimeMs = System.currentTimeMillis() + timeoutMs;
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

    // Read messages until all available messages are read, or we run out of time
    try {
      while (_reader.hasMessageAvailable() && System.currentTimeMillis() < endTimeMs) {
        messages.add(PulsarUtils.buildPulsarStreamMessage(_reader.readNext(), _config));
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
    return new PulsarMessageBatch(messages, offsetOfNextBatch, _reader.hasReachedEndOfTopic());
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
    super.close();
  }
}
