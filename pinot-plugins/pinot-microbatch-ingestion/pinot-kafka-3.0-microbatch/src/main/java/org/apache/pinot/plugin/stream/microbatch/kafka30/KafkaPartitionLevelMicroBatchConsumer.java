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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamMessageMetadata;
import org.apache.pinot.plugin.stream.kafka30.KafkaPartitionLevelConnectionHandler;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kafka partition-level consumer for MicroBatch ingestion.
 *
 * <p>This consumer reads Kafka messages containing MicroBatch protocol payloads (references to
 * batch files in PinotFS or inline data) and returns decoded {@link GenericRow} records. Unlike
 * traditional Kafka consumers that process individual records, this consumer:
 *
 * <ol>
 *   <li>Polls Kafka for protocol messages containing file URIs or inline data</li>
 *   <li>Downloads batch files from PinotFS (S3, HDFS, GCS, etc.) via {@link MicroBatchQueueManager}</li>
 *   <li>Parses files using Pinot's {@link org.apache.pinot.spi.data.readers.RecordReader}</li>
 *   <li>Returns records as {@link MessageBatch} with composite offsets for mid-batch resume</li>
 * </ol>
 *
 * <p>The consumer maintains state to support mid-batch resume after segment commits. When
 * restarting from a composite offset like {@code {"kmo":5,"mbro":100}}, it seeks to Kafka
 * offset 5 and skips the first 100 records in that batch file.
 *
 * <p>Threading: This consumer delegates file downloads to {@link MicroBatchQueueManager},
 * which uses a configurable thread pool for parallel processing.
 *
 * @see MicroBatchProtocol for the wire protocol format
 * @see MicroBatchStreamPartitionMsgOffset for the composite offset format
 * @see MicroBatchQueueManager for batch file processing
 */
public class KafkaPartitionLevelMicroBatchConsumer extends KafkaPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaPartitionLevelMicroBatchConsumer.class);

  private final MicroBatchQueueManager _providerManager;

  private MicroBatchStreamPartitionMsgOffset _lastFetchedOffset = null;

  public KafkaPartitionLevelMicroBatchConsumer(
      String clientId, StreamConfig streamConfig, int partition, int numFileFetchThreads) {
    super(clientId, streamConfig, partition);
    _providerManager = new MicroBatchQueueManager(partition, numFileFetchThreads);
  }

  public KafkaPartitionLevelMicroBatchConsumer(
      String clientId,
      StreamConfig streamConfig,
      int partition,
      RetryPolicy retryPolicy,
      int numFileFetchThreads) {
    super(clientId, streamConfig, partition, retryPolicy);
    _providerManager = new MicroBatchQueueManager(partition, numFileFetchThreads);
  }

  @Override
  public synchronized MessageBatch<GenericRow> fetchMessages(
      StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    MicroBatchStreamPartitionMsgOffset mbStartOffset =
        (MicroBatchStreamPartitionMsgOffset) startMsgOffset;
    if (_lastFetchedOffset == null || !isContinuation(_lastFetchedOffset, mbStartOffset)) {
      LOGGER.debug(
          "Seeking to offset {} (last fetched offset was {})", mbStartOffset, _lastFetchedOffset);
      _providerManager.clear();
      long kafkaOffset = mbStartOffset.getKafkaMessageOffset();
      _consumer.seek(_topicPartition, kafkaOffset);
    }
    MessageBatch<GenericRow> messageBatch;
    if (!_providerManager.hasData()) {
      // Pass the startRecordOffset for mid-batch resume - only applies to the first Kafka message
      int startRecordOffset = (int) mbStartOffset.getRecordOffsetInMicroBatch();
      pollKafka(mbStartOffset.getKafkaMessageOffset(), startRecordOffset, timeoutMs);
    }
    messageBatch = _providerManager.getNextMessageBatch();
    if (messageBatch != null) {
      // Update _lastFetchedOffset from the last message in the batch
      if (messageBatch.getMessageCount() > 0) {
        int lastIndex = messageBatch.getMessageCount() - 1;
        _lastFetchedOffset = (MicroBatchStreamPartitionMsgOffset)
            messageBatch.getStreamMessage(lastIndex).getMetadata().getOffset();
      }
      return messageBatch;
    }
    _lastFetchedOffset =
        new MicroBatchStreamPartitionMsgOffset(mbStartOffset.getKafkaMessageOffset(), -1);
    return new GenericRowMessageBatch(Collections.emptyList(), 0, false, 0, mbStartOffset);
  }

  private boolean isContinuation(
      MicroBatchStreamPartitionMsgOffset lastFetchedOffset,
      MicroBatchStreamPartitionMsgOffset mbStartOffset) {
    return lastFetchedOffset.getKafkaMessageOffset() == mbStartOffset.getKafkaMessageOffset()
        && lastFetchedOffset.getRecordOffsetInMicroBatch()
            == mbStartOffset.getRecordOffsetInMicroBatch() - 1;
  }

  /**
   * Poll Kafka for messages and submit them to the queue manager.
   *
   * @param targetKafkaOffset the expected Kafka offset (for data loss detection)
   * @param startRecordOffset for the first Kafka message at targetKafkaOffset, skip this many
   *                          records (for mid-batch resume). Subsequent messages start from 0.
   * @param timeoutMs poll timeout in milliseconds
   */
  private void pollKafka(long targetKafkaOffset, int startRecordOffset, int timeoutMs) {
    ConsumerRecords<String, Bytes> consumerRecords = _consumer.poll(Duration.ofMillis(timeoutMs));
    List<ConsumerRecord<String, Bytes>> records = consumerRecords.records(_topicPartition);
    if (records.isEmpty()) {
      return;
    }

    // In case read_committed is enabled, the messages consumed are not guaranteed to have
    // consecutive offsets.
    // TODO: A better solution would be to fetch earliest offset from topic and see if it is greater
    // than startOffset.
    // However, this would require and additional call to Kafka which we want to avoid.
    long firstOffset = records.get(0).offset();
    boolean hasDataLoss = false;
    if (_config.getKafkaIsolationLevel() == null
        || _config
            .getKafkaIsolationLevel()
            .equals(
                KafkaStreamConfigProperties.LowLevelConsumer
                    .KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED)) {
      hasDataLoss = firstOffset > targetKafkaOffset;
    }

    int nullMessages = 0;
    for (ConsumerRecord<String, Bytes> record : records) {
      StreamMessageMetadata messageMetadata = extractMessageMetadata(record);
      String key = record.key();
      byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
      Bytes message = record.value();
      if (message == null) {
        LOGGER.debug("Tombstone message at offset: {}", record.offset());
        nullMessages++;
        continue;
      }
      try {
        // Parse JSON protocol - will throw exception if invalid
        MicroBatchProtocol protocol = MicroBatchProtocol.parse(message.get());

        // For the first message at targetKafkaOffset, use startRecordOffset to skip records.
        // Subsequent messages start from record 0.
        int recordsToSkip = (record.offset() == targetKafkaOffset) ? startRecordOffset : 0;

        MicroBatch microBatch =
            new MicroBatch(
                keyBytes, nullMessages + 1, protocol, messageMetadata, hasDataLoss, recordsToSkip);
        nullMessages = 0;
        hasDataLoss = false;

        _providerManager.submitBatch(microBatch);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to parse microbatch protocol at offset: " + record.offset(), e);
      }
    }
  }

  private StreamMessageMetadata extractMessageMetadata(ConsumerRecord<String, Bytes> record) {
    long timestamp = record.timestamp();
    long offset = record.offset();

    StreamMessageMetadata.Builder builder =
        new StreamMessageMetadata.Builder()
            .setRecordIngestionTimeMs(timestamp)
            .setOffset(
                MicroBatchStreamPartitionMsgOffset.of(offset),
                MicroBatchStreamPartitionMsgOffset.of(offset + 1))
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
      builder.setMetadata(
          Map.of(
              KafkaStreamMessageMetadata.RECORD_TIMESTAMP_KEY,
              String.valueOf(timestamp),
              KafkaStreamMessageMetadata.METADATA_OFFSET_KEY,
              String.valueOf(offset),
              KafkaStreamMessageMetadata.METADATA_PARTITION_KEY,
              String.valueOf(record.partition())));
    }
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    _providerManager.close();
    super.close();
  }
}
