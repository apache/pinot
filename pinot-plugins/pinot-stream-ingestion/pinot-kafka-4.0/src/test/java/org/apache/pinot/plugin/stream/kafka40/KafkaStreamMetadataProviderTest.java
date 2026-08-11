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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.TransientConsumerException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

  @Test
  public void testComputePartitionGroupMetadataIssuesSingleBatchedOffsetFetch()
      throws Exception {
    // Regression for the controller ideal-state stall (batching): fetching offsets for the missing partitions must
    // be a single batched broker call, not one consumer creation / round-trip per partition.
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 8);
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      // Empty consumption status -> all 8 partitions are fetched from the stream.
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        provider.computePartitionGroupMetadata("client", streamConfig, List.of(), 10000);
      }
      // SMALLEST criteria -> exactly one batched beginningOffsets call, no endOffsets call, and that single call
      // must carry all 8 partitions (proving it is a true batch, not a per-partition loop).
      @SuppressWarnings("unchecked")
      ArgumentCaptor<Collection<TopicPartition>> captor = ArgumentCaptor.forClass(Collection.class);
      verify(consumer, times(1)).beginningOffsets(captor.capture(), any(Duration.class));
      verify(consumer, never()).endOffsets(any(Collection.class), any(Duration.class));
      assertEquals(captor.getValue().stream().map(TopicPartition::partition).sorted().collect(Collectors.toList()),
          List.of(0, 1, 2, 3, 4, 5, 6, 7));
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test
  public void testComputePartitionGroupMetadataLargestOffsetCriteria()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 4);
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName, "largest");
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        List<PartitionGroupMetadata> metadataList =
            provider.computePartitionGroupMetadata("client", streamConfig, List.of(), 10000);
        assertEquals(metadataList.stream().map(PartitionGroupMetadata::getPartitionGroupId)
            .collect(Collectors.toList()), List.of(0, 1, 2, 3));
        // LARGEST -> batched endOffsets (2000 + partition).
        assertEquals(metadataList.stream().map(metadata -> metadata.getStartOffset().toString())
            .collect(Collectors.toList()), List.of("2000", "2001", "2002", "2003"));
      }
      verify(consumer, times(1)).endOffsets(any(Collection.class), any(Duration.class));
      verify(consumer, never()).beginningOffsets(any(Collection.class), any(Duration.class));
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test
  public void testComputePartitionGroupMetadataTimestampFallsBackToEndOffsets()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 3);
    // offsetsForTimes: partition 0 has a matching offset (50); partitions 1 and 2 have none (null) and must fall
    // back to their end offset, all in a single batched endOffsets call.
    when(consumer.offsetsForTimes(any(Map.class), any(Duration.class))).thenAnswer(invocation -> {
      Map<TopicPartition, Long> query = invocation.getArgument(0);
      Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
      for (TopicPartition topicPartition : query.keySet()) {
        result.put(topicPartition, topicPartition.partition() == 0 ? new OffsetAndTimestamp(50L, 123L) : null);
      }
      return result;
    });
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName, "2022-08-09T12:31:38.222Z");
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        List<PartitionGroupMetadata> metadataList =
            provider.computePartitionGroupMetadata("client", streamConfig, List.of(), 10000);
        assertEquals(metadataList.stream().map(metadata -> metadata.getStartOffset().toString())
            .collect(Collectors.toList()), List.of("50", "2001", "2002"));
      }
      verify(consumer, times(1)).offsetsForTimes(any(Map.class), any(Duration.class));
      verify(consumer, times(1)).endOffsets(any(Collection.class), any(Duration.class));
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test
  public void testComputePartitionGroupMetadataPeriodOffsetCriteria()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 2);
    when(consumer.offsetsForTimes(any(Map.class), any(Duration.class))).thenAnswer(invocation -> {
      Map<TopicPartition, Long> query = invocation.getArgument(0);
      Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
      for (TopicPartition topicPartition : query.keySet()) {
        result.put(topicPartition, new OffsetAndTimestamp(70L + topicPartition.partition(), 123L));
      }
      return result;
    });
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName, "2h");
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        List<PartitionGroupMetadata> metadataList =
            provider.computePartitionGroupMetadata("client", streamConfig, List.of(), 10000);
        assertEquals(metadataList.stream().map(metadata -> metadata.getStartOffset().toString())
            .collect(Collectors.toList()), List.of("70", "71"));
      }
      // PERIOD resolves via a single batched offsetsForTimes call; none of the partitions need the endOffsets
      // fallback here.
      verify(consumer, times(1)).offsetsForTimes(any(Map.class), any(Duration.class));
      verify(consumer, never()).endOffsets(any(Collection.class), any(Duration.class));
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test
  public void testFetchStreamPartitionOffsetReturnsBatchedOffset()
      throws Exception {
    String topicName = "asset";
    Consumer<Bytes, Bytes> consumer = mockConsumer(topicName, 4);
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      // Partition-scoped provider: fetchStreamPartitionOffset now delegates to the batched fetch for its partition.
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig, 2)) {
        StreamPartitionMsgOffset offset = provider.fetchStreamPartitionOffset(
            new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000);
        // beginningOffsets returns 1000 + partition.
        assertEquals(offset.toString(), "1002");
      }
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test(expectedExceptions = TransientConsumerException.class)
  public void testFetchStreamPartitionOffsetThrowsWhenOffsetMissing()
      throws Exception {
    String topicName = "asset";
    @SuppressWarnings("unchecked")
    Consumer<Bytes, Bytes> consumer = mock(Consumer.class);
    // The stream returns no offset for the requested partition; the delegating method must fail loudly rather than
    // return null.
    when(consumer.beginningOffsets(any(Collection.class), any(Duration.class))).thenReturn(new HashMap<>());
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig, 0)) {
        provider.fetchStreamPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000);
      }
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  @Test(expectedExceptions = TransientConsumerException.class)
  public void testComputePartitionGroupMetadataThrowsWhenPartitionOffsetMissing()
      throws Exception {
    // If the stream returns no offset for a requested partition, the whole fetch must fail with a transient error so
    // PartitionGroupMetadataFetcher retries on the next run. The partition must NOT be silently dropped: downstream,
    // absence from the list is the end-of-life signal (the CONSUMING segment is marked ONLINE with no successor) and
    // also shrinks the derived partition count.
    String topicName = "asset";
    @SuppressWarnings("unchecked")
    Consumer<Bytes, Bytes> consumer = mock(Consumer.class);
    List<PartitionInfo> partitionInfos = IntStream.range(0, 3)
        .mapToObj(partitionId -> new PartitionInfo(topicName, partitionId, null, null, null))
        .collect(Collectors.toList());
    when(consumer.partitionsFor(eq(topicName), any(Duration.class))).thenReturn(partitionInfos);
    when(consumer.beginningOffsets(any(Collection.class), any(Duration.class))).thenAnswer(invocation -> {
      Collection<TopicPartition> topicPartitions = invocation.getArgument(0);
      Map<TopicPartition, Long> offsets = new HashMap<>();
      for (TopicPartition topicPartition : topicPartitions) {
        // Partition 1 is omitted: the stream returns no offset for it.
        if (topicPartition.partition() != 1) {
          offsets.put(topicPartition, 1000L + topicPartition.partition());
        }
      }
      return offsets;
    });
    MOCK_CONSUMER.set(consumer);
    try {
      StreamConfig streamConfig = getStreamConfig(topicName);
      try (KafkaStreamMetadataProvider provider = new MockKafkaStreamMetadataProvider("client", streamConfig)) {
        provider.computePartitionGroupMetadata("client", streamConfig, List.of(), 10000);
      }
    } finally {
      MOCK_CONSUMER.remove();
    }
  }

  private static StreamConfig getStreamConfig(String topicName) {
    return getStreamConfig(topicName, "smallest");
  }

  private static StreamConfig getStreamConfig(String topicName, String offsetCriteria) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", topicName);
    streamConfigMap.put("stream.kafka.broker.list", "unused:9092");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", MockKafkaConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka." + StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA, offsetCriteria);
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
