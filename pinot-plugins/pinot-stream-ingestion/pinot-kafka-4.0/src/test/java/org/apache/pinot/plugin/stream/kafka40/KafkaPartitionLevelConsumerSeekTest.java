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
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Regression tests for [KafkaPartitionLevelConsumer] seek decisions.
public class KafkaPartitionLevelConsumerSeekTest {
  private static final String TOPIC = "test-topic";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);

  @Test
  public void testRepeatedStartOffsetAfterNonEmptyFetchSeeksAgain() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    ConsumerRecords<Bytes, Bytes> consumerRecords = records(record(291L));
    when(mockConsumer.poll(any(Duration.class))).thenReturn(consumerRecords, consumerRecords);

    KafkaPartitionLevelConsumer consumer = createConsumerWithMock(getStreamConfig(null), mockConsumer);
    KafkaMessageBatch firstBatch = consumer.fetchMessages(new LongMsgOffset(291L), 10000);
    KafkaMessageBatch repeatedBatch = consumer.fetchMessages(new LongMsgOffset(291L), 10000);

    assertEquals(firstBatch.getOffsetOfNextBatch().toString(), "292");
    assertEquals(repeatedBatch.getFirstMessageOffset().toString(), "291");
    verify(mockConsumer, times(2)).seek(TOPIC_PARTITION, 291L);
  }

  @Test
  public void testRepeatedStartOffsetAfterReadCommittedEmptyFetchDoesNotSeekAgain() {
    Consumer<Bytes, Bytes> mockConsumer = mock(Consumer.class);
    ConsumerRecords<Bytes, Bytes> emptyRecords =
        new ConsumerRecords<>(Map.of(TOPIC_PARTITION, List.<ConsumerRecord<Bytes, Bytes>>of()));
    when(mockConsumer.poll(any(Duration.class))).thenReturn(emptyRecords, records(record(100L)));
    when(mockConsumer.position(eq(TOPIC_PARTITION), any(Duration.class))).thenReturn(100L);

    KafkaPartitionLevelConsumer consumer =
        createConsumerWithMock(getStreamConfig(
            KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED), mockConsumer);
    KafkaMessageBatch emptyBatch = consumer.fetchMessages(new LongMsgOffset(0L), 10000);
    KafkaMessageBatch nextBatch = consumer.fetchMessages(new LongMsgOffset(0L), 10000);

    assertEquals(emptyBatch.getMessageCount(), 0);
    assertEquals(emptyBatch.getOffsetOfNextBatch().toString(), "100");
    assertEquals(nextBatch.getFirstMessageOffset().toString(), "100");
    verify(mockConsumer, times(1)).seek(TOPIC_PARTITION, 0L);
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
