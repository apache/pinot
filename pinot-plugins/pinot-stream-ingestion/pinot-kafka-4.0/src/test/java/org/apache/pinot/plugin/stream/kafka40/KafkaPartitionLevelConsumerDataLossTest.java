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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Regression tests for [KafkaPartitionLevelConsumer] data-loss detection (DATA-2966).
///
/// A gap between the requested `startOffset` and the first returned record offset must NOT be
/// reported as data loss when it is caused by transactional control records (commit/abort
/// markers occupy offsets but are never delivered to the consumer). Real loss is only flagged
/// when the broker's log start offset has advanced past the requested `startOffset`.
public class KafkaPartitionLevelConsumerDataLossTest {
  private static final String TOPIC = "test-topic";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final String READ_COMMITTED = "read_committed";

  /// Offset gap caused by transactional control records, but everything from startOffset is
  /// still retained (logStartOffset <= startOffset) => no data loss.
  @Test
  public void testTransactionalGapWithRetainedDataIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    // Requested 100, first user record is at 105 (offsets 100-104 were commit/abort markers).
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(105L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 50L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), 10000);

    assertFalse(batch.hasDataLoss());
  }

  /// Requested startOffset is below the log start offset: the broker deleted (retention or
  /// truncation) records at/after startOffset => genuine data loss.
  @Test
  public void testStartOffsetBelowLogStartOffsetIsDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(150L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 150L));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), 10000);

    assertTrue(batch.hasDataLoss());
  }

  /// Contiguous batch (firstOffset == startOffset): no gap, so we must not even query the log
  /// start offset.
  @Test
  public void testContiguousBatchIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(100L)));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), 10000);

    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// Under read_committed, aborted-record gaps are always expected: never flag loss and never
  /// pay for the extra broker round-trip.
  @Test
  public void testReadCommittedGapIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(105L)));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(READ_COMMITTED), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), 10000);

    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// If the log start offset cannot be determined, default to "no data loss" so a transient
  /// broker hiccup does not manufacture a false StreamDataLoss alert.
  @Test
  public void testLogStartOffsetLookupFailureDefaultsToNoDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(105L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenThrow(new org.apache.kafka.common.errors.TimeoutException("boom"));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), 10000);

    assertFalse(batch.hasDataLoss());
  }

  private static KafkaPartitionLevelConsumer createConsumerWithMock(StreamConfig streamConfig,
      Consumer<Bytes, Bytes> mockConsumer) {
    class FakeKafkaPartitionLevelConsumer extends KafkaPartitionLevelConsumer {
      FakeKafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
        super(clientId, streamConfig, partition);
      }

      @Override
      protected Consumer<Bytes, Bytes> createConsumer(Properties consumerProp) {
        return mockConsumer;
      }
    }
    return new FakeKafkaPartitionLevelConsumer("clientId-test", streamConfig, 0);
  }

  private static ConsumerRecords<Bytes, Bytes> records(ConsumerRecord<Bytes, Bytes> record) {
    return new ConsumerRecords<>(Map.of(TOPIC_PARTITION, List.of(record)));
  }

  private static ConsumerRecord<Bytes, Bytes> record(long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, 3, 5, bytes("key"),
        bytes("value"), new RecordHeaders(), null);
  }

  private static Bytes bytes(String value) {
    return new Bytes(value.getBytes(StandardCharsets.UTF_8));
  }

  private static StreamConfig getStreamConfig(String isolationLevel) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TOPIC);
    streamConfigMap.put("stream.kafka.broker.list", "localhost:9092");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", KafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    if (isolationLevel != null) {
      streamConfigMap.put("stream.kafka.isolation.level", isolationLevel);
    }
    return new StreamConfig("tableName_REALTIME", streamConfigMap);
  }
}
