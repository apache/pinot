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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class KafkaStreamMetadataProviderTest {
  private static final ThreadLocal<Consumer<Bytes, Bytes>> MOCK_CONSUMER = new ThreadLocal<>();

  @Test
  public void testComputePartitionGroupMetadataRecoversMissingLowPartitionId()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 8);
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      List<PartitionGroupConsumptionStatus> currentStatuses = List.of(
          new PartitionGroupConsumptionStatus(0, 0, new LongMsgOffset(0), new LongMsgOffset(10), "DONE"),
          new PartitionGroupConsumptionStatus(1, 1, new LongMsgOffset(0), new LongMsgOffset(11), "DONE"),
          new PartitionGroupConsumptionStatus(3, 3, new LongMsgOffset(0), new LongMsgOffset(13), "DONE"),
          new PartitionGroupConsumptionStatus(4, 4, new LongMsgOffset(0), new LongMsgOffset(14), "DONE"),
          new PartitionGroupConsumptionStatus(5, 5, new LongMsgOffset(0), new LongMsgOffset(15), "DONE"),
          new PartitionGroupConsumptionStatus(6, 6, new LongMsgOffset(0), new LongMsgOffset(16), "DONE"),
          new PartitionGroupConsumptionStatus(7, 7, new LongMsgOffset(0), new LongMsgOffset(17), "DONE"));

      assertEquals(computePartitionIdsWithSizeBasedAlgorithm(currentStatuses, 8), List.of(0, 1, 3, 4, 5, 6, 7, 7));

      try (KafkaStreamMetadataProvider streamMetadataProvider =
          new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        List<PartitionGroupMetadata> partitionGroupMetadataList =
            streamMetadataProvider.computePartitionGroupMetadata("client", streamConfig, currentStatuses, 10000);

        assertEquals(partitionGroupMetadataList.stream().map(PartitionGroupMetadata::getPartitionGroupId)
            .collect(Collectors.toList()), List.of(0, 1, 2, 3, 4, 5, 6, 7));
        assertEquals(partitionGroupMetadataList.get(2).getStartOffset().toString(), "1002");
        assertEquals(partitionGroupMetadataList.get(3).getStartOffset().toString(), "13");
      }
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test
  public void testComputePartitionGroupMetadataHandlesTopicExpansion()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 8);
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      List<PartitionGroupConsumptionStatus> currentStatuses = List.of(
          new PartitionGroupConsumptionStatus(0, 0, new LongMsgOffset(0), new LongMsgOffset(10), "DONE"),
          new PartitionGroupConsumptionStatus(1, 1, new LongMsgOffset(0), new LongMsgOffset(11), "DONE"),
          new PartitionGroupConsumptionStatus(2, 2, new LongMsgOffset(0), new LongMsgOffset(12), "DONE"),
          new PartitionGroupConsumptionStatus(3, 3, new LongMsgOffset(0), new LongMsgOffset(13), "DONE"));

      try (KafkaStreamMetadataProvider streamMetadataProvider =
          new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        List<PartitionGroupMetadata> partitionGroupMetadataList =
            streamMetadataProvider.computePartitionGroupMetadata("client", streamConfig, currentStatuses, 10000);

        assertEquals(partitionGroupMetadataList.stream().map(PartitionGroupMetadata::getPartitionGroupId)
            .collect(Collectors.toList()), List.of(0, 1, 2, 3, 4, 5, 6, 7));
        assertEquals(partitionGroupMetadataList.stream().map(metadata -> metadata.getStartOffset().toString())
            .collect(Collectors.toList()), List.of("10", "11", "12", "13", "1004", "1005", "1006", "1007"));
      }
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test
  public void testGetCurrentPartitionLagStateHandlesInvalidIngestionTime()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 1);
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        long lastProcessedTimeMs = 1_700_000_100_000L;

        // Record with a valid upstream ingestion time yields a numeric availability lag.
        StreamMessageMetadata validMetadata = new StreamMessageMetadata.Builder()
            .setOffset(new LongMsgOffset(5), new LongMsgOffset(6))
            .setRecordIngestionTimeMs(lastProcessedTimeMs - 1000L)
            .build();
        // Records whose ingestion time is missing/invalid: unset (Builder default Long.MIN_VALUE), Kafka's
        // NO_TIMESTAMP (-1), and epoch 0 (the exact boundary of the > 0 guard). These stand in for a topic that is
        // unreachable/timing out or produces records without a timestamp.
        StreamMessageMetadata unsetIngestionTime = new StreamMessageMetadata.Builder()
            .setOffset(new LongMsgOffset(5), new LongMsgOffset(6))
            .build();
        StreamMessageMetadata noTimestampIngestionTime = new StreamMessageMetadata.Builder()
            .setOffset(new LongMsgOffset(5), new LongMsgOffset(6))
            .setRecordIngestionTimeMs(-1L)
            .build();
        StreamMessageMetadata zeroIngestionTime = new StreamMessageMetadata.Builder()
            .setOffset(new LongMsgOffset(5), new LongMsgOffset(6))
            .setRecordIngestionTimeMs(0L)
            .build();

        Map<String, ConsumerPartitionState> stateMap = new HashMap<>();
        stateMap.put("0", new ConsumerPartitionState("0", new LongMsgOffset(5), lastProcessedTimeMs,
            new LongMsgOffset(10), validMetadata));
        stateMap.put("1", new ConsumerPartitionState("1", new LongMsgOffset(5), lastProcessedTimeMs,
            new LongMsgOffset(10), unsetIngestionTime));
        stateMap.put("2", new ConsumerPartitionState("2", new LongMsgOffset(5), lastProcessedTimeMs,
            new LongMsgOffset(10), noTimestampIngestionTime));
        stateMap.put("3", new ConsumerPartitionState("3", new LongMsgOffset(5), lastProcessedTimeMs,
            new LongMsgOffset(10), zeroIngestionTime));
        // Partition with an unknown upstream offset exercises the offset-lag fallback.
        stateMap.put("4", new ConsumerPartitionState("4", new LongMsgOffset(5), lastProcessedTimeMs,
            null, validMetadata));

        Map<String, PartitionLagState> lagState = provider.getCurrentPartitionLagState(stateMap);

        // Offset lag: numeric when both offsets are known, NOT_CALCULATED when the upstream offset is unavailable.
        assertEquals(lagState.get("0").getRecordsLag(), "5");
        assertEquals(lagState.get("4").getRecordsLag(), PartitionLagState.NOT_CALCULATED);
        // Availability lag: numeric for a valid ingestion time, NOT_CALCULATED for every invalid one.
        // Regression for issue #18836: an invalid ingestion time must not leak an epoch-sized value
        // (lastProcessedTimeMs - Long.MIN_VALUE, or lastProcessedTimeMs - (-1) ~= now).
        assertEquals(lagState.get("0").getAvailabilityLagMs(), "1000");
        assertEquals(lagState.get("1").getAvailabilityLagMs(), PartitionLagState.NOT_CALCULATED);
        assertEquals(lagState.get("2").getAvailabilityLagMs(), PartitionLagState.NOT_CALCULATED);
        assertEquals(lagState.get("3").getAvailabilityLagMs(), PartitionLagState.NOT_CALCULATED);
      }
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  private static StreamConfig getStreamConfig(String topicName) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", topicName);
    streamConfigMap.put("stream.kafka.broker.list", "unused:9092");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", MockKafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka." + StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA, "smallest");
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    return new StreamConfig("tableName_REALTIME", streamConfigMap);
  }

  @SuppressWarnings("unchecked")
  private static Consumer<Bytes, Bytes> mockConsumer(String topicName, int partitionCount) {
    Consumer<Bytes, Bytes> consumer = mock(Consumer.class);
    List<PartitionInfo> partitionInfos = IntStream.range(0, partitionCount)
        .mapToObj(partitionId -> new PartitionInfo(topicName, partitionId, null, null, null))
        .collect(Collectors.toList());
    when(consumer.partitionsFor(eq(topicName), any(Duration.class))).thenReturn(partitionInfos);
    when(consumer.beginningOffsets(any(Collection.class), any(Duration.class))).thenAnswer(invocation -> {
      Collection<TopicPartition> topicPartitions = invocation.getArgument(0);
      Map<TopicPartition, Long> offsets = new HashMap<>();
      for (TopicPartition topicPartition : topicPartitions) {
        offsets.put(topicPartition, 1000L + topicPartition.partition());
      }
      return offsets;
    });
    when(consumer.endOffsets(any(Collection.class), any(Duration.class))).thenAnswer(invocation -> {
      Collection<TopicPartition> topicPartitions = invocation.getArgument(0);
      Map<TopicPartition, Long> offsets = new HashMap<>();
      for (TopicPartition topicPartition : topicPartitions) {
        offsets.put(topicPartition, 2000L + topicPartition.partition());
      }
      return offsets;
    });
    return consumer;
  }

  private static List<Integer> computePartitionIdsWithSizeBasedAlgorithm(
      List<PartitionGroupConsumptionStatus> currentStatuses, int partitionCount) {
    List<Integer> partitionIds = currentStatuses.stream()
        .map(PartitionGroupConsumptionStatus::getStreamPartitionGroupId)
        .collect(Collectors.toList());
    for (int partitionId = currentStatuses.size(); partitionId < partitionCount; partitionId++) {
      partitionIds.add(partitionId);
    }
    return partitionIds;
  }

  public static class MockKafkaConsumerFactory extends KafkaConsumerFactory {
    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      return new MockKafkaStreamMetadataProvider(clientId, _streamConfig, partition);
    }

    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      return new MockKafkaStreamMetadataProvider(clientId, _streamConfig);
    }
  }

  private static class MockKafkaStreamMetadataProvider extends KafkaStreamMetadataProvider {
    MockKafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
      super(clientId, streamConfig);
    }

    MockKafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
      super(clientId, streamConfig, partition);
    }

    @Override
    protected Consumer<Bytes, Bytes> createConsumer(Properties consumerProp) {
      return MOCK_CONSUMER.get();
    }
  }
}
