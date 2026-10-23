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
import java.util.concurrent.TimeUnit;
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
  // Minimum interval between WARN lines for a failed log-start-offset lookup. The lookup runs on every poll
  // whose first record is past the requested offset (every transaction on a low-traffic transactional topic),
  // so an unhealthy broker would otherwise emit one WARN per poll.
  private static final long LOG_START_OFFSET_LOOKUP_FAILURE_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

  // Offset the consumer is positioned to read NEXT. -1 means the consumer has not been
  // positioned for any caller-requested startOffset yet (we have not issued a seek).
  // After a successful fetch, this is lastRecord.offset + 1. After an empty fetch, this is the
  // consumer's actual KafkaConsumer.position(), which may have advanced past the caller's
  // startOffset even though zero records were returned (skipped transaction control records,
  // or aborted records under read_committed).
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
  // Whether the previous successful poll returned any records from Kafka. When the caller
  // repeats the same startOffset after a non-empty fetch, the records may have been fetched
  // but not processed by RealtimeSegmentDataManager because an end criterion fired before
  // index 0. In that case we must re-seek and replay the batch instead of continuing from
  // _nextReadOffset.
  private boolean _lastFetchReturnedRecords;
  // Cached true iff stream.kafka.isolation.level == read_committed. Computed once at
  // construction (the value is final per consumer) and reused on every fetch instead of
  // re-resolving the StreamConfig each time.
  private final boolean _isReadCommitted = isReadCommitted(_config);
  // Rate limiting for the WARN in getLogStartOffset(). Only touched from fetchMessages(), which is synchronized.
  private long _lastLogStartOffsetLookupFailureLogMs = 0;
  private int _suppressedLogStartOffsetLookupFailures = 0;

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
    //   (b) the caller-requested startOffset differs from _nextReadOffset, except for
    //       the repeated-startOffset-after-empty-fetch case described below.
    //
    // We only suppress a re-seek when the caller passed the same startOffset after the
    // previous successful fetch returned zero records. That is the read_committed
    // empty-poll case: RealtimeSegmentDataManager._currentOffset did not advance, but
    // KafkaConsumer.position() may have advanced through aborted records. Seeking back to
    // startOffset would undo that progress and wedge consumption forever.
    //
    // If the previous poll returned records, a repeated startOffset means the caller did
    // not process them (for example, the segment time limit fired before index 0). We must
    // seek back and replay those records; otherwise CATCH_UP can skip an already-fetched
    // batch and falsely report data loss.
    boolean repeatedStartOffsetAfterEmptyFetch = startOffset == _lastFetchStartOffset && !_lastFetchReturnedRecords;
    if (_nextReadOffset < 0 || (startOffset != _nextReadOffset && !repeatedStartOffsetAfterEmptyFetch)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Seeking to offset: {}", startOffset);
      }
      _consumer.seek(_topicPartition, startOffset);
      _nextReadOffset = startOffset;
    }

    ConsumerRecords<Bytes, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMs));
    List<ConsumerRecord<Bytes, Bytes>> records = consumerRecords.records(_topicPartition);
    _lastFetchStartOffset = startOffset;
    _lastFetchReturnedRecords = !records.isEmpty();
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
    } else {
      // No records returned, but the underlying KafkaConsumer may still have moved past offsets it will never
      // deliver: transaction control records (commit/abort markers) under either isolation level, and records of
      // aborted transactions under read_committed. Snap _nextReadOffset to the consumer's actual position so the
      // returned batch reports where consumption really resumes. RealtimeSegmentDataManager doesn't advance its
      // offset on an empty batch and calls us again with the same startOffset; the no-re-seek rule above keeps
      // the consumer's progress in that case. position() is answered from the client's local state once a
      // position is known, and is bounded by the same timeout as poll() otherwise, so a sick broker can't stall
      // this loop.
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
    // A gap between the requested startOffset and the first returned offset does NOT by itself imply data loss.
    // Transactional producers write commit/abort control records that occupy offsets but are never delivered to
    // the consumer (under either isolation level), so a contiguous stream of user records legitimately has offset
    // gaps. On a low-traffic transactional topic every transaction produces one: after record N the caller is
    // parked at the marker offset N + 1 and the next record lands at N + 2. The Kafka client gives no way to tell
    // a skipped control record from an out-of-range reset (both just move the position), so the only reliable
    // signal is the broker's log start offset: real data loss means records at/after startOffset were deleted
    // (retention or truncation), i.e. the log start offset is now above startOffset. That lookup is one round
    // trip per gap, never on the contiguous hot path. For read_committed we never flag loss because aborted-record
    // gaps are always expected, and the caller's startOffset can lag far behind the consumer's position through
    // an aborted region.
    boolean hasDataLoss = false;
    if (!_isReadCommitted && firstOffset > startOffset) {
      hasDataLoss = getLogStartOffset(timeoutMs) > startOffset;
    }
    return new KafkaMessageBatch(filteredRecords, records.size(), offsetOfNextBatch, firstOffset, lastMessageMetadata,
        hasDataLoss, batchSizeInBytes);
  }

  /// Returns the log start (earliest available) offset for the partition, bounded by the same
  /// timeout as [#poll]. Returns [Long#MIN_VALUE] when it cannot be determined so the caller
  /// treats an offset gap as expected (no data loss) rather than raising a false positive.
  private long getLogStartOffset(int timeoutMs) {
    try {
      return _consumer.beginningOffsets(List.of(_topicPartition), Duration.ofMillis(timeoutMs))
          .getOrDefault(_topicPartition, Long.MIN_VALUE);
    } catch (Exception e) {
      logLogStartOffsetLookupFailure(e);
      return Long.MIN_VALUE;
    }
  }

  /// Logs a failed log-start-offset lookup at WARN at most once per
  /// [#LOG_START_OFFSET_LOOKUP_FAILURE_LOG_INTERVAL_MS], reporting how many failures were suppressed since the
  /// previous line. Suppressed failures are still logged at DEBUG.
  private void logLogStartOffsetLookupFailure(Exception e) {
    long nowMs = System.currentTimeMillis();
    if (nowMs - _lastLogStartOffsetLookupFailureLogMs < LOG_START_OFFSET_LOOKUP_FAILURE_LOG_INTERVAL_MS) {
      _suppressedLogStartOffsetLookupFailures++;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Failed to read log start offset for {} (WARN suppressed)", _topicPartition, e);
      }
      return;
    }
    LOGGER.warn("Failed to read log start offset for {} ({} similar failures suppressed since the last report); "
        + "treating the offset gap as no data loss (this can mask genuine data loss if it persists)",
        _topicPartition, _suppressedLogStartOffsetLookupFailures, e);
    _lastLogStartOffsetLookupFailureLogMs = nowMs;
    _suppressedLogStartOffsetLookupFailures = 0;
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
