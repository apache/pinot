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
package org.apache.pinot.plugin.stream.kafka20;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPartitionLevelConsumer extends KafkaPartitionLevelConnectionHandler
    implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumer.class);

  private long _lastFetchedOffset = -1;

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  @Override
  public MessageBatch<StreamMessage<byte[]>> fetchMessages(StreamPartitionMsgOffset startMsgOffset,
      StreamPartitionMsgOffset endMsgOffset, int timeoutMillis) {
    final long startOffset = ((LongMsgOffset) startMsgOffset).getOffset();
    final long endOffset = endMsgOffset == null ? Long.MAX_VALUE : ((LongMsgOffset) endMsgOffset).getOffset();
    return fetchMessages(startOffset, endOffset, timeoutMillis);
  }

  public synchronized MessageBatch<StreamMessage<byte[]>> fetchMessages(long startOffset, long endOffset,
      int timeoutMillis) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Polling partition: {}, startOffset: {}, endOffset: {} timeout: {}ms", _topicPartition, startOffset,
          endOffset, timeoutMillis);
    }
    if (_lastFetchedOffset < 0 || _lastFetchedOffset != startOffset - 1) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Seeking to offset: {}", startOffset);
      }
      _consumer.seek(_topicPartition, startOffset);
    }
    ConsumerRecords<String, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMillis));
    List<ConsumerRecord<String, Bytes>> messageAndOffsets = consumerRecords.records(_topicPartition);
    List<StreamMessage<byte[]>> filtered = new ArrayList<>(messageAndOffsets.size());
    long firstOffset = startOffset;
    long lastOffset = startOffset;
    StreamMessageMetadata rowMetadata = null;
    if (!consumerRecords.isEmpty()) {
      firstOffset = consumerRecords.iterator().next().offset();
    }
    for (ConsumerRecord<String, Bytes> messageAndOffset : messageAndOffsets) {
      long offset = messageAndOffset.offset();
      _lastFetchedOffset = offset;
      if (offset >= startOffset && (endOffset > offset || endOffset < 0)) {
        Bytes message = messageAndOffset.value();
        rowMetadata = (StreamMessageMetadata) _kafkaMetadataExtractor.extract(messageAndOffset);
        if (message != null) {
          String key = messageAndOffset.key();
          byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
          filtered.add(new KafkaStreamMessage(keyBytes, message.get(), rowMetadata));
        } else if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Tombstone message at offset: {}", offset);
        }
        lastOffset = offset;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Ignoring message at offset: {} (outside of offset range [{}, {}))", offset, startOffset,
            endOffset);
      }
    }
    return new KafkaMessageBatch(messageAndOffsets.size(), firstOffset, lastOffset, filtered, rowMetadata);
  }
}
