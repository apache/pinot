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
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
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

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig);
    try {
      _reader = createReaderForPartition(partition);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating Pulsar reader", e);
    }
    LOGGER.info("Created Pulsar reader with topic: {}, partition: {}, initial message id: {}",
        _config.getPulsarTopicName(), partition, _config.getInitialMessageId());
  }

  /**
   * Fetch records from the Pulsar stream between the start and end StreamPartitionMsgOffset
   * Used {@link Reader} to read the messaged from pulsar partitioned topic
   * The reader seeks to the startMsgOffset and starts reading records in a loop until endMsgOffset or timeout is
   * reached.
   */
  @Override
  public PulsarMessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
    long endTimeMs = System.currentTimeMillis() + timeoutMs;
    MessageId startMessageId = ((MessageIdStreamOffset) startOffset).getMessageId();
    List<BytesStreamMessage> messages = new ArrayList<>();
    try {
      _reader.seek(startMessageId);
      while (_reader.hasMessageAvailable() && System.currentTimeMillis() < endTimeMs) {
        Message<byte[]> message = _reader.readNext();
        MessageId messageId = message.getMessageId();
        if (messageId.compareTo(startMessageId) < 0) {
          continue;
        }
        messages.add(PulsarUtils.buildPulsarStreamMessage(message, _config));
      }
    } catch (PulsarClientException e) {
      throw new RuntimeException("Caught exception while fetching messages from Pulsar", e);
    }
    MessageIdStreamOffset offsetOfNextBatch;
    if (messages.isEmpty()) {
      offsetOfNextBatch = (MessageIdStreamOffset) startOffset;
    } else {
      StreamMessageMetadata lastMessageMetadata = messages.get(messages.size() - 1).getMetadata();
      assert lastMessageMetadata != null;
      offsetOfNextBatch = (MessageIdStreamOffset) lastMessageMetadata.getNextOffset();
    }
    return new PulsarMessageBatch(messages, offsetOfNextBatch, _reader.hasReachedEndOfTopic());
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
    super.close();
  }
}
