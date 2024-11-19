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
package org.apache.pinot.plugin.stream.kinesis;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


/**
 * A {@link PartitionGroupConsumer} implementation for the Kinesis stream
 */
public class KinesisConsumer extends KinesisConnectionHandler implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumer.class);

  private String _nextStartSequenceNumber = null;
  private String _nextShardIterator = null;
  private int _currentSecond = 0;
  private int _numRequestsInCurrentSecond = 0;

  public KinesisConsumer(KinesisConfig config) {
    super(config);
    LOGGER.info("Created Kinesis consumer with topic: {}, RPS limit: {}, max records per fetch: {}",
        config.getStreamTopicName(), config.getRpsLimit(), config.getNumMaxRecordsToFetch());
  }

  @VisibleForTesting
  public KinesisConsumer(KinesisConfig config, KinesisClient kinesisClient) {
    super(config, kinesisClient);
  }

  @Override
  public synchronized KinesisMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    try {
      return getKinesisMessageBatch((KinesisPartitionGroupOffset) startMsgOffset);
    } catch (ProvisionedThroughputExceededException pte) {
      LOGGER.error("Rate limit exceeded while fetching messages from Kinesis stream: {} with threshold: {}",
          pte.getMessage(), _config.getRpsLimit());
      return new KinesisMessageBatch(List.of(), (KinesisPartitionGroupOffset) startMsgOffset, false);
    }
  }

  private KinesisMessageBatch getKinesisMessageBatch(KinesisPartitionGroupOffset startMsgOffset) {
    KinesisPartitionGroupOffset startOffset = startMsgOffset;
    String shardId = startOffset.getShardId();
    String startSequenceNumber = startOffset.getSequenceNumber();
    // Get the shard iterator
    String shardIterator;
    if (startSequenceNumber.equals(_nextStartSequenceNumber)) {
      shardIterator = _nextShardIterator;
    } else {
      // TODO: Revisit the offset handling logic. Reading after the start sequence number can lose the first message
      //       when consuming from a new partition because the initial start sequence number is inclusive.
      GetShardIteratorRequest getShardIteratorRequest =
          GetShardIteratorRequest.builder().streamName(_config.getStreamTopicName()).shardId(shardId)
              .startingSequenceNumber(startSequenceNumber).shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
              .build();
      shardIterator = _kinesisClient.getShardIterator(getShardIteratorRequest).shardIterator();
    }
    if (shardIterator == null) {
      return new KinesisMessageBatch(List.of(), startOffset, true);
    }

    // Read records
    rateLimitRequests();
    GetRecordsRequest getRecordRequest =
        GetRecordsRequest.builder().shardIterator(shardIterator).limit(_config.getNumMaxRecordsToFetch()).build();
    GetRecordsResponse getRecordsResponse = _kinesisClient.getRecords(getRecordRequest);
    List<Record> records = getRecordsResponse.records();
    List<BytesStreamMessage> messages;
    KinesisPartitionGroupOffset offsetOfNextBatch;
    if (!records.isEmpty()) {
      messages = records.stream().map(record -> extractStreamMessage(record, shardId)).collect(Collectors.toList());
      StreamMessageMetadata lastMessageMetadata = messages.get(messages.size() - 1).getMetadata();
      assert lastMessageMetadata != null;
      offsetOfNextBatch = (KinesisPartitionGroupOffset) lastMessageMetadata.getNextOffset();
    } else {
      // TODO: Revisit whether Kinesis can return empty batch when there are available records. The consumer cna handle
      //       empty message batch, but it will treat it as fully caught up.
      messages = List.of();
      offsetOfNextBatch = startOffset;
    }
    assert offsetOfNextBatch != null;
    _nextStartSequenceNumber = offsetOfNextBatch.getSequenceNumber();
    _nextShardIterator = getRecordsResponse.nextShardIterator();
    return new KinesisMessageBatch(messages, offsetOfNextBatch, _nextShardIterator == null);
  }

  /**
   * Kinesis enforces a limit of 5 getRecords request per second on each shard from AWS end, beyond which we start
   * getting {@link ProvisionedThroughputExceededException}. Rate limit the requests to avoid this.
   */
  private void rateLimitRequests() {
    long currentTimeMs = System.currentTimeMillis();
    int currentTimeSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(currentTimeMs);
    if (currentTimeSeconds == _currentSecond) {
      if (_numRequestsInCurrentSecond == _config.getRpsLimit()) {
        try {
          Thread.sleep(1000 - (currentTimeMs % 1000));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        _currentSecond = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        _numRequestsInCurrentSecond = 1;
      } else {
        _numRequestsInCurrentSecond++;
      }
    } else {
      _currentSecond = currentTimeSeconds;
      _numRequestsInCurrentSecond = 1;
    }
  }

  private BytesStreamMessage extractStreamMessage(Record record, String shardId) {
    byte[] key = record.partitionKey().getBytes(StandardCharsets.UTF_8);
    byte[] value = record.data().asByteArray();
    long timestamp = record.approximateArrivalTimestamp().toEpochMilli();
    String sequenceNumber = record.sequenceNumber();
    KinesisPartitionGroupOffset offset = new KinesisPartitionGroupOffset(shardId, sequenceNumber);
    // NOTE: Use the same offset as next offset because the consumer starts consuming AFTER the start sequence number.
    StreamMessageMetadata.Builder builder = new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(timestamp)
        .setSerializedValueSize(record.data().asByteArray().length).setOffset(offset, offset);
    if (_config.isPopulateMetadata()) {
      builder.setMetadata(Map.of(KinesisStreamMessageMetadata.APPRX_ARRIVAL_TIMESTAMP_KEY, String.valueOf(timestamp),
          KinesisStreamMessageMetadata.SEQUENCE_NUMBER_KEY, sequenceNumber));
    }
    StreamMessageMetadata metadata = builder.build();
    return new BytesStreamMessage(key, value, metadata);
  }

  @Override
  public void close() {
    super.close();
  }
}
