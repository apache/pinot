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
import org.apache.pinot.plugin.stream.kafka.KafkaPartitionLevelStreamConfig;
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

  // Offset the consumer is positioned to read NEXT. -1 means the consumer has not been
  // positioned for any caller-requested startOffset yet (we have not issued a seek).
  // After a successful fetch, this is lastRecord.offset + 1. After an empty fetch under
  // read_committed (where the batch may have been filtered as aborted), this is the
  // consumer's actual KafkaConsumer.position(), which may have advanced past the caller's
  // startOffset even though zero records were returned.
  private long _nextReadOffset = -1;
  // The caller's startOffset on the previous fetchMessages() call. Used to distinguish
  // "caller is passing the same startOffset back because RealtimeSegmentDataManager's
  // _currentOffset didn't advance on our last empty batch" (should NOT re-seek and undo
  // _nextReadOffset's progress) from "caller is requesting a new startOffset" (should
  // re-seek). The latter does not occur in production -- RealtimeSegmentDataManager
  // consumes monotonically forward -- but the public PartitionGroupConsumer contract
  // (and KafkaPartitionLevelConsumerTest.testConsumer) does exercise arbitrary
  // re-seeks, so we preserve that behaviour.
  private long _lastFetchStartOffset = Long.MIN_VALUE;
  // Cached true iff stream.kafka.isolation.level == read_committed. Computed once at
  // construction (the value is final per consumer) and reused on every fetch instead of
  // re-resolving the StreamConfig each time.
  private final boolean _isReadCommitted = isReadCommitted(_config);

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
    // Seek when:
    //   (a) we have not yet positioned the consumer (initial state), OR
    //   (b) the caller passed a startOffset different from the previous call AND
    //       different from the consumer's current _nextReadOffset (i.e. a fresh
    //       caller-requested seek that doesn't already match where we are).
    // We do NOT seek when the caller passed the SAME startOffset as the previous call --
    // that is the read_committed empty-poll case, where RealtimeSegmentDataManager.
    // _currentOffset didn't advance on our last empty batch and so calls us with the same
    // startOffset again. Seeking back to that startOffset would undo the consumer's
    // progress through the filtered (aborted) region and wedge consumption forever; this
    // was the dominant CI flake mode for ExactlyOnceKafkaRealtimeClusterIntegrationTest.
    boolean explicitNewStartOffset = startOffset != _lastFetchStartOffset && startOffset != _nextReadOffset;
    if (_nextReadOffset < 0 || explicitNewStartOffset) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Seeking to offset: {}", startOffset);
      }
      _consumer.seek(_topicPartition, startOffset);
      _nextReadOffset = startOffset;
    }
    _lastFetchStartOffset = startOffset;

    ConsumerRecords<Bytes, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMs));
    List<ConsumerRecord<Bytes, Bytes>> records = consumerRecords.records(_topicPartition);
    List<BytesStreamMessage> filteredRecords = new ArrayList<>(records.size());
    long firstOffset = -1;
    StreamMessageMetadata lastMessageMetadata = null;
    long batchSizeInBytes = 0;
    if (!records.isEmpty()) {
      firstOffset = records.get(0).offset();
      _nextReadOffset = records.get(records.size() - 1).offset() + 1;
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
    } else if (_isReadCommitted) {
      // No records returned. With read_committed isolation, the underlying KafkaConsumer's
      // internal position may still have advanced past offsets filtered out as aborted
      // transactional records, so snap _nextReadOffset to the consumer's actual position.
      // RealtimeSegmentDataManager doesn't advance _currentOffset on empty batches and will
      // call us again with the same startOffset; without this update the seek-check would
      // re-seek to startOffset and undo the consumer's progress through the aborted region.
      //
      // For read_uncommitted (the default), empty polls can never advance the internal
      // position past records that didn't exist, so we skip the position() call in the
      // hot path. The call is bounded by the same timeout as poll() so a sick broker
      // can't stall this loop unbounded.
      long currentPosition;
      try {
        currentPosition = _consumer.position(_topicPartition, Duration.ofMillis(timeoutMs));
      } catch (Exception e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Failed to read consumer position after empty poll on {}", _topicPartition, e);
        }
        currentPosition = _nextReadOffset;
      }
      if (currentPosition > _nextReadOffset) {
        _nextReadOffset = currentPosition;
      }
    }
    long offsetOfNextBatch = _nextReadOffset;

    // For read_uncommitted (the default), a non-contiguous returned batch implies data
    // loss (records dropped before being read). For read_committed the offset gap is
    // expected because the broker filters aborted transactional records, so we don't flag
    // it as data loss.
    boolean hasDataLoss = !_isReadCommitted && firstOffset > startOffset;
    return new KafkaMessageBatch(filteredRecords, records.size(), offsetOfNextBatch, firstOffset, lastMessageMetadata,
        hasDataLoss, batchSizeInBytes);
  }

  private static boolean isReadCommitted(KafkaPartitionLevelStreamConfig config) {
    String level = config.getKafkaIsolationLevel();
    return level != null
        && !level.equals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED);
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
