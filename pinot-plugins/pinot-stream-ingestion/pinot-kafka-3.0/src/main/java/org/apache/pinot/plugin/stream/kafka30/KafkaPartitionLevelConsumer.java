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
package org.apache.pinot.plugin.stream.kafka30;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamMessageMetadata;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPartitionLevelConsumer extends KafkaPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumer.class);

  private long _lastFetchedOffset = -1;

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  @Override
  public synchronized KafkaMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    long startOffset = ((LongMsgOffset) startMsgOffset).getOffset();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Polling partition: {}, startOffset: {}, timeout: {}ms", _topicPartition, startOffset, timeoutMs);
    }
    if (_lastFetchedOffset < 0 || _lastFetchedOffset != startOffset - 1) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Seeking to offset: {}", startOffset);
      }
      _consumer.seek(_topicPartition, startOffset);
    }

    ConsumerRecords<String, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMs));
    List<ConsumerRecord<String, Bytes>> records = consumerRecords.records(_topicPartition);
    List<BytesStreamMessage> filteredRecords = new ArrayList<>(records.size());
    long firstOffset = -1;
    long offsetOfNextBatch = startOffset;
    StreamMessageMetadata lastMessageMetadata = null;
    if (!records.isEmpty()) {
      firstOffset = records.get(0).offset();
      _lastFetchedOffset = records.get(records.size() - 1).offset();
      offsetOfNextBatch = _lastFetchedOffset + 1;
      for (ConsumerRecord<String, Bytes> record : records) {
        StreamMessageMetadata messageMetadata = extractMessageMetadata(record);
        Bytes message = record.value();
        if (message != null) {
          String key = record.key();
          byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
          filteredRecords.add(new BytesStreamMessage(keyBytes, message.get(), messageMetadata));
        } else if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Tombstone message at offset: {}", record.offset());
        }
        lastMessageMetadata = messageMetadata;
      }
    }

    return new KafkaMessageBatch(filteredRecords, records.size(), offsetOfNextBatch, firstOffset, lastMessageMetadata,
        firstOffset > startOffset);
  }

  private StreamMessageMetadata extractMessageMetadata(ConsumerRecord<String, Bytes> record) {
    long timestamp = record.timestamp();
    long offset = record.offset();

    StreamMessageMetadata.Builder builder = new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(timestamp)
        .setOffset(new LongMsgOffset(offset), new LongMsgOffset(offset + 1))
        .setSerializedValueSize(record.serializedValueSize());
    if (_config.isPopulateMetadata()) {
      Headers headers = record.headers();
      if (headers != null) {
        GenericRow headerGenericRow = new GenericRow();
        for (Header header : headers.toArray()) {
          headerGenericRow.putValue(header.key(), header.value());
        }
        builder.setHeaders(headerGenericRow);
      }
      builder.setMetadata(Map.of(KafkaStreamMessageMetadata.RECORD_TIMESTAMP_KEY, String.valueOf(timestamp),
          KafkaStreamMessageMetadata.METADATA_OFFSET_KEY, String.valueOf(offset),
          KafkaStreamMessageMetadata.METADATA_PARTITION_KEY, String.valueOf(record.partition())));
    }
    return builder.build();
  }
}
