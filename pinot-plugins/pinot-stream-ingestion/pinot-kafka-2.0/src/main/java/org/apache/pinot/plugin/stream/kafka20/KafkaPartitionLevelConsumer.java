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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.MessageAndOffset;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPartitionLevelConsumer extends KafkaPartitionLevelConnectionHandler
    implements PartitionLevelConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumer.class);

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  @Override
  public MessageBatch<byte[]> fetchMessages(StreamPartitionMsgOffset startMsgOffset,
      StreamPartitionMsgOffset endMsgOffset, int timeoutMillis) {
    final long startOffset = ((LongMsgOffset) startMsgOffset).getOffset();
    final long endOffset = endMsgOffset == null ? Long.MAX_VALUE : ((LongMsgOffset) endMsgOffset).getOffset();
    return fetchMessages(startOffset, endOffset, timeoutMillis);
  }

  public MessageBatch<byte[]> fetchMessages(long startOffset, long endOffset, int timeoutMillis) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("poll consumer: {}, startOffset: {}, endOffset:{} timeout: {}ms", _topicPartition, startOffset,
          endOffset, timeoutMillis);
    }
    _consumer.seek(_topicPartition, startOffset);
    ConsumerRecords<String, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMillis));
    List<ConsumerRecord<String, Bytes>> messageAndOffsets = consumerRecords.records(_topicPartition);
    List<MessageAndOffset> filtered = new ArrayList<>(messageAndOffsets.size());
    long lastOffset = startOffset;
    for (ConsumerRecord<String, Bytes> messageAndOffset : messageAndOffsets) {
      Bytes message = messageAndOffset.value();
      long offset = messageAndOffset.offset();
      if (offset >= startOffset & (endOffset > offset | endOffset == -1)) {
        if (message != null) {
          filtered.add(new MessageAndOffset(message.get(), offset));
        } else if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("tombstone message at offset {}", offset);
        }
        lastOffset = offset;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("filter message at offset {} (outside of offset range {} {})", offset, startOffset, endOffset);
      }
    }
    return new KafkaMessageBatch(messageAndOffsets.size(), lastOffset, filtered);
  }
}
