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
package org.apache.pinot.plugin.stream.kafka40;

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
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamMessageMetadata;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaPartitionLevelConsumer extends KafkaPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumer.class);

  private long _lastFetchedOffset = -1;
  // True once the consumer has been positioned (via seek or assign) for the current
  // startOffset and has had at least one poll attempt. Tracked separately from
  // _lastFetchedOffset because, with read_committed isolation, the very first poll can
  // legitimately return zero records (all records were aborted) and we must NOT re-seek
  // on the next call -- doing so would undo the consumer's internal advance through the
  // aborted region and wedge us forever at startOffset = 0.
  private long _lastSeekedStartOffset = Long.MIN_VALUE;
  // Diagnostic counter: consecutive fetch calls that returned no records.
  private int _consecutiveEmptyPolls = 0;

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
  }

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition,
      RetryPolicy retryPolicy) {
    super(clientId, streamConfig, partition, retryPolicy);
  }

  @Override
  public synchronized KafkaMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    long startOffset = ((LongMsgOffset) startMsgOffset).getOffset();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Polling partition: {}, startOffset: {}, timeout: {}ms", _topicPartition, startOffset, timeoutMs);
    }
    // Seek if (a) we've never positioned the consumer for this startOffset, OR (b) the
    // caller's startOffset moved off the position we last tracked. _lastFetchedOffset < 0
    // alone is NOT a sufficient seek trigger because, with read_committed, an empty
    // first poll keeps _lastFetchedOffset at its initial -1 even though the consumer's
    // internal position has moved past aborted records; re-seeking on every empty poll
    // would undo that progress and wedge consumption forever at startOffset.
    boolean firstSeekForThisOffset = _lastSeekedStartOffset != startOffset;
    if (firstSeekForThisOffset || _lastFetchedOffset != startOffset - 1) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Seeking to offset: {}", startOffset);
      }
      _consumer.seek(_topicPartition, startOffset);
      _lastSeekedStartOffset = startOffset;
    }

    ConsumerRecords<Bytes, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMs));
    List<ConsumerRecord<Bytes, Bytes>> records = consumerRecords.records(_topicPartition);
    List<BytesStreamMessage> filteredRecords = new ArrayList<>(records.size());
    long firstOffset = -1;
    long offsetOfNextBatch = startOffset;
    StreamMessageMetadata lastMessageMetadata = null;
    long batchSizeInBytes = 0;
    if (!records.isEmpty()) {
      if (_consecutiveEmptyPolls > 0) {
        // ERROR (not WARN) so this passes the test BurstFilter that DENIES below-ERROR.
        LOGGER.error("[kafka-consumer-diag] {} records received on {} after {} consecutive empty polls",
            records.size(), _topicPartition, _consecutiveEmptyPolls);
        _consecutiveEmptyPolls = 0;
      }
      firstOffset = records.get(0).offset();
      _lastFetchedOffset = records.get(records.size() - 1).offset();
      offsetOfNextBatch = _lastFetchedOffset + 1;
      for (ConsumerRecord<Bytes, Bytes> record : records) {
        StreamMessageMetadata messageMetadata = extractMessageMetadata(record);
        Bytes message = record.value();
        if (message != null) {
          Bytes key = record.key();
          byte[] keyBytes = key != null ? key.get() : null;
          filteredRecords.add(new BytesStreamMessage(keyBytes, message.get(), messageMetadata));
          if (messageMetadata.getRecordSerializedSize() > 0) {
            batchSizeInBytes += messageMetadata.getRecordSerializedSize();
          }
        } else if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Tombstone message at offset: {}", record.offset());
        }
        lastMessageMetadata = messageMetadata;
      }
    } else {
      // No records were returned to the application by this poll, but the underlying
      // KafkaConsumer's internal position may still have advanced past offsets that were
      // filtered out -- most commonly when read_committed isolation skips an aborted
      // transactional batch. Track the consumer's actual position so the next call
      // resumes from there. If position has not advanced either, still mark
      // _lastFetchedOffset so the seek-check at the top of the next call skips the
      // redundant re-seek.
      long currentPosition;
      Exception positionEx = null;
      try {
        currentPosition = _consumer.position(_topicPartition);
      } catch (Exception e) {
        positionEx = e;
        currentPosition = startOffset;
      }
      _consecutiveEmptyPolls++;
      if (currentPosition > startOffset) {
        _lastFetchedOffset = currentPosition - 1;
        offsetOfNextBatch = currentPosition;
      } else {
        _lastFetchedOffset = startOffset - 1;
      }
      if (_consecutiveEmptyPolls == 1 || _consecutiveEmptyPolls % 50 == 0) {
        // ERROR (not WARN) so this passes the test BurstFilter that DENIES below-ERROR.
        LOGGER.error(
            "[kafka-consumer-diag] empty poll #{} on {} startOffset={} consumerPosition={} lastFetchedOffset={}{}",
            _consecutiveEmptyPolls, _topicPartition, startOffset, currentPosition, _lastFetchedOffset,
            positionEx == null ? "" : (" positionError=" + positionEx));
      }
    }

    // In case read_committed is enabled, the messages consumed are not guaranteed to have consecutive offsets.
    // TODO: A better solution would be to fetch earliest offset from topic and see if it is greater than startOffset.
    // However, this would require and additional call to Kafka which we want to avoid.
    boolean hasDataLoss = false;
    if (_config.getKafkaIsolationLevel() == null || _config.getKafkaIsolationLevel()
        .equals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED)) {
      hasDataLoss = firstOffset > startOffset;
    }
    return new KafkaMessageBatch(filteredRecords, records.size(), offsetOfNextBatch, firstOffset, lastMessageMetadata,
        hasDataLoss, batchSizeInBytes);
  }

  private StreamMessageMetadata extractMessageMetadata(ConsumerRecord<Bytes, Bytes> record) {
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
