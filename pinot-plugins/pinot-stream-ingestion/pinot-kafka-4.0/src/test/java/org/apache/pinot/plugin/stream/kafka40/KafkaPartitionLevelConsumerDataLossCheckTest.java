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
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Unit tests for the data-loss decision in [KafkaPartitionLevelConsumer#fetchMessages] against a mocked Kafka
/// consumer, so that the number of broker round-trips is observable. The same decision is exercised against a real
/// broker, with real transaction control records, in [KafkaPartitionLevelConsumerDataLossIntegrationTest].
public class KafkaPartitionLevelConsumerDataLossCheckTest {
  private static final String TOPIC = "test-topic";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  private static final int TIMEOUT_MS = 10000;

  @Test
  public void testContiguousBatchDoesNotLookUpLogStartOffset() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(100L), record(101L)));

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getMessageCount(), 2);
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// The gap left by a transaction control record: the first record is past the requested offset, but the log still
  /// starts at or below the requested offset, so nothing was deleted.
  @Test
  public void testGapWithRetainedStartOffsetIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(101L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 40L));

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getFirstMessageOffset().toString(), "101");
    assertFalse(batch.hasDataLoss());
    // One round trip per gap, and only on the gap path
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// Real loss: records at/after the requested offset were deleted, so the log now starts above it.
  @Test
  public void testGapWithStartOffsetBelowLogStartIsDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(150L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenReturn(Map.of(TOPIC_PARTITION, 150L));

    KafkaMessageBatch batch = createConsumerWithMock(getStreamConfig(null), mockConsumer)
        .fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getFirstMessageOffset().toString(), "150");
    assertTrue(batch.hasDataLoss());
    verify(mockConsumer, times(1)).beginningOffsets(anyCollection(), any(Duration.class));
  }

  @Test
  public void testGapUnderReadCommittedIsNotDataLossWithoutLookup() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(101L)));

    KafkaMessageBatch batch = createConsumerWithMock(
        getStreamConfig(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED),
        mockConsumer).fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);

    assertEquals(batch.getFirstMessageOffset().toString(), "101");
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, never()).beginningOffsets(anyCollection(), any(Duration.class));
  }

  /// When the log start offset cannot be determined, the gap is treated as expected rather than as loss, so a broker
  /// hiccup cannot manufacture a false alert, and the failure does not propagate out of fetchMessages.
  @Test
  public void testLogStartOffsetLookupFailureIsNotDataLoss() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    when(mockConsumer.poll(any(Duration.class))).thenReturn(records(record(150L)), records(record(150L)));
    when(mockConsumer.beginningOffsets(anyCollection(), any(Duration.class)))
        .thenThrow(new TimeoutException("broker unavailable"));

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch batch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertFalse(batch.hasDataLoss());
    // A repeated failure within the WARN interval is throttled in the log but evaluated the same way
    batch = consumer.fetchMessages(new LongMsgOffset(100L), TIMEOUT_MS);
    assertFalse(batch.hasDataLoss());
    verify(mockConsumer, times(2)).beginningOffsets(anyCollection(), any(Duration.class));
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

  @SafeVarargs
  private static ConsumerRecords<Bytes, Bytes> records(ConsumerRecord<Bytes, Bytes>... records) {
    return new ConsumerRecords<>(Map.of(TOPIC_PARTITION, List.of(records)), Map.of());
  }

  private static ConsumerRecord<Bytes, Bytes> record(long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, 3, 5, bytes("key"),
        bytes("value"), new RecordHeaders(), Optional.empty());
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
