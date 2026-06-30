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
package org.apache.pinot.spi.stream;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PartitionGroupMetadataFetcherTest {

  @Test
  public void testFetchSingleStreamSuccess()
      throws Exception {
    // Setup
    StreamConfig streamConfig = createMockStreamConfig("test-topic", "test-table", false);
    List<StreamConfig> streamConfigs = List.of(streamConfig);

    PartitionGroupConsumptionStatus status = mock(PartitionGroupConsumptionStatus.class);
    when(status.getPartitionGroupId()).thenReturn(0);
    List<PartitionGroupConsumptionStatus> statusList = List.of(status);

    PartitionGroupMetadata metadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    List<PartitionGroupMetadata> metadataList = List.of(metadata);

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean())).thenReturn(metadataList);

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, List.of(), false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertTrue(result);
      List<StreamMetadata> streamMetadataList = fetcher.getStreamMetadataList();
      Assert.assertEquals(streamMetadataList.size(), 1);
      Assert.assertEquals(streamMetadataList.get(0).getNumPartitions(), 1);
      Assert.assertEquals(streamMetadataList.get(0).getPartitionGroupMetadataList().size(), 1);
      Assert.assertNull(fetcher.getException());
    }
  }

  @Test
  public void testFetchSingleStreamTransientException()
      throws Exception {
    // Setup
    StreamConfig streamConfig = createMockStreamConfig("test-topic", "test-table", false);
    List<StreamConfig> streamConfigs = List.of(streamConfig);

    List<PartitionGroupConsumptionStatus> statusList = List.of();

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.fetchPartitionCount(anyLong())).thenReturn(1);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new TransientConsumerException(new RuntimeException("Transient error")));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, List.of(), false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertFalse(result);
      Assert.assertTrue(fetcher.getException() instanceof TransientConsumerException);
    }
  }

  @Test
  public void testFetchMultipleStreams()
      throws Exception {
    // Setup
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    PartitionGroupConsumptionStatus status1 = new PartitionGroupConsumptionStatus(0, 0, null, null, "IN_PROGRESS");
    PartitionGroupConsumptionStatus status2 = new PartitionGroupConsumptionStatus(1, 1, null, null, "IN_PROGRESS");
    List<PartitionGroupConsumptionStatus> statusList = Arrays.asList(status1, status2);

    PartitionGroupMetadata mockedMetadata1 = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    PartitionGroupMetadata mockedMetadata2 = new PartitionGroupMetadata(1, mock(StreamPartitionMsgOffset.class));

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Arrays.asList(mockedMetadata1, mockedMetadata2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, List.of(), false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertTrue(result);
      List<StreamMetadata> streamMetadataList = fetcher.getStreamMetadataList();
      Assert.assertEquals(streamMetadataList.size(), 2);
      Assert.assertNull(fetcher.getException());

      Assert.assertEquals(streamMetadataList.get(0).getNumPartitions(), 2);
      Assert.assertEquals(streamMetadataList.get(0).getPartitionGroupMetadataList().size(), 2);
      Assert.assertEquals(streamMetadataList.get(1).getNumPartitions(), 2);
      Assert.assertEquals(streamMetadataList.get(1).getPartitionGroupMetadataList().size(), 2);

      // Verify the correct partition group IDs: 0, 1, 10000, 10001
      List<Integer> partitionIds = streamMetadataList.stream()
          .flatMap(sm -> sm.getPartitionGroupMetadataList().stream())
          .map(PartitionGroupMetadata::getPartitionGroupId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionIds, Arrays.asList(0, 1, 10000, 10001));
    }
  }

  @Test
  public void testFetchMultipleStreamsWithPause()
      throws Exception {
    // Setup
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", false);
    StreamConfig streamConfig3 = createMockStreamConfig("topic3", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2, streamConfig3);

    PartitionGroupConsumptionStatus status1 = new PartitionGroupConsumptionStatus(0, 0, null, null, "IN_PROGRESS");
    PartitionGroupConsumptionStatus status2 = new PartitionGroupConsumptionStatus(1, 1, null, null, "IN_PROGRESS");
    List<PartitionGroupConsumptionStatus> statusList = Arrays.asList(status1, status2);

    PartitionGroupMetadata mockedMetadata1 = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    PartitionGroupMetadata mockedMetadata2 = new PartitionGroupMetadata(1, mock(StreamPartitionMsgOffset.class));

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.fetchPartitionCount(anyLong())).thenReturn(3);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Arrays.asList(mockedMetadata1, mockedMetadata2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, List.of(1), false);

      // Execute
      Boolean result = fetcher.call();

      // Verify - 2 streams active (topic1 at index 0, topic3 at index 2; topic2 at index 1 is paused)
      Assert.assertTrue(result);
      List<StreamMetadata> streamMetadataList = fetcher.getStreamMetadataList();
      Assert.assertEquals(streamMetadataList.size(), 2);
      Assert.assertNull(fetcher.getException());

      // Verify the correct partition group IDs
      List<Integer> partitionIds = streamMetadataList.stream()
          .flatMap(sm -> sm.getPartitionGroupMetadataList().stream())
          .map(PartitionGroupMetadata::getPartitionGroupId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionIds, Arrays.asList(0, 1, 20000, 20001));
    }
  }

  @Test
  public void testDeprecatedGetPartitionGroupMetadataListFlatMaps()
      throws Exception {
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    PartitionGroupConsumptionStatus status1 = new PartitionGroupConsumptionStatus(0, 0, null, null, "IN_PROGRESS");
    List<PartitionGroupConsumptionStatus> statusList = List.of(status1);

    StreamPartitionMsgOffset offset = mock(StreamPartitionMsgOffset.class);
    PartitionGroupMetadata m1 = new PartitionGroupMetadata(0, offset);
    PartitionGroupMetadata m2 = new PartitionGroupMetadata(1, offset);

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.fetchPartitionCount(anyLong())).thenReturn(2);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean())).thenReturn(Arrays.asList(m1, m2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, List.of(), false);
      fetcher.call();

      // Deprecated method should flat-map across all streams
      List<PartitionGroupMetadata> flatList = fetcher.getPartitionGroupMetadataList();
      Assert.assertEquals(flatList.size(), 4); // 2 per stream * 2 streams
    }
  }

  @Test
  public void testExceptionResetOnRetry()
      throws Exception {
    StreamConfig streamConfig = createMockStreamConfig("test-topic", "test-table", false);
    List<StreamConfig> streamConfigs = List.of(streamConfig);

    StreamPartitionMsgOffset offset = mock(StreamPartitionMsgOffset.class);
    PartitionGroupMetadata metadata = new PartitionGroupMetadata(0, offset);

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.fetchPartitionCount(anyLong())).thenReturn(1);
    // First call: transient failure; second call: success
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new TransientConsumerException(new RuntimeException("Transient")))
        .thenReturn(List.of(metadata));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, List.of(), List.of(), false);

      // First call fails
      Boolean result1 = fetcher.call();
      Assert.assertFalse(result1);
      Assert.assertNotNull(fetcher.getException());

      // Second call succeeds - exception should be reset
      Boolean result2 = fetcher.call();
      Assert.assertTrue(result2);
      Assert.assertNull(fetcher.getException());
      Assert.assertEquals(fetcher.getStreamMetadataList().size(), 1);
    }
  }

  @Test
  public void testSequenceNumberPreservedInMultiStreamRemap()
      throws Exception {
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    List<PartitionGroupConsumptionStatus> statusList = List.of();

    StreamPartitionMsgOffset offset = mock(StreamPartitionMsgOffset.class);
    PartitionGroupMetadata m1 = new PartitionGroupMetadata(0, offset, 7);
    PartitionGroupMetadata m2 = new PartitionGroupMetadata(1, offset, 3);

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.fetchPartitionCount(anyLong())).thenReturn(2);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean())).thenReturn(Arrays.asList(m1, m2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, List.of(), false);
      fetcher.call();

      List<StreamMetadata> streamMetadataList = fetcher.getStreamMetadataList();
      Assert.assertEquals(streamMetadataList.size(), 2);

      // Second stream's partitions should have remapped IDs but preserved sequence numbers
      List<PartitionGroupMetadata> stream1Partitions = streamMetadataList.get(1).getPartitionGroupMetadataList();
      Assert.assertEquals(stream1Partitions.get(0).getPartitionGroupId(), 10000);
      Assert.assertEquals(stream1Partitions.get(0).getSequenceNumber(), 7);
      Assert.assertEquals(stream1Partitions.get(1).getPartitionGroupId(), 10001);
      Assert.assertEquals(stream1Partitions.get(1).getSequenceNumber(), 3);
    }
  }

  @Test
  public void testGetStreamMetadataListReturnsUnmodifiable()
      throws Exception {
    StreamConfig streamConfig = createMockStreamConfig("test-topic", "test-table", false);
    List<StreamConfig> streamConfigs = List.of(streamConfig);

    PartitionGroupMetadata metadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.fetchPartitionCount(anyLong())).thenReturn(1);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean())).thenReturn(List.of(metadata));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, List.of(), List.of(), false);
      fetcher.call();

      try {
        fetcher.getStreamMetadataList().add(
            new StreamMetadata(streamConfig, 1, List.of()));
        Assert.fail("Expected UnsupportedOperationException");
      } catch (UnsupportedOperationException e) {
        // expected
      }
    }
  }

  /**
   * When one topic in a multi-topic table is inaccessible (e.g. deleted from Kafka), the fetcher must
   * continue fetching metadata for the remaining topics and return partial results rather than re-throwing
   * and killing ingestion for all healthy topics.
   */
  @Test
  public void testFetchMultipleStreamsOneTopicPermanentFailure()
      throws Exception {
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table_REALTIME", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2-deleted", "test-table_REALTIME", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    StreamPartitionMsgOffset offset = mock(StreamPartitionMsgOffset.class);
    PartitionGroupMetadata metadata = new PartitionGroupMetadata(0, offset);

    // topic1 succeeds, topic2 throws a permanent (non-transient) exception
    StreamMetadataProvider goodProvider = mock(StreamMetadataProvider.class);
    when(goodProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean())).thenReturn(List.of(metadata));

    StreamMetadataProvider badProvider = mock(StreamMetadataProvider.class);
    when(badProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new PermanentConsumerException(new RuntimeException("Topic does not exist")));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString()))
        .thenReturn(goodProvider)   // called for topic1
        .thenReturn(badProvider);   // called for topic2

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, List.of(), List.of(), false);

      Boolean result = fetcher.call();

      // Fetch succeeds overall — only topic1's metadata is returned, topic2 is silently skipped
      Assert.assertTrue(result);
      Assert.assertEquals(fetcher.getStreamMetadataList().size(), 1);
      Assert.assertEquals(fetcher.getStreamMetadataList().get(0).getNumPartitions(), 1);
      Assert.assertNull(fetcher.getException());
    }
  }

  @Test
  public void testFetchMultipleStreamsFailedTopicDoesNotBlockOthersOnRetry()
      throws Exception {
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table_REALTIME", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table_REALTIME", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    StreamPartitionMsgOffset offset = mock(StreamPartitionMsgOffset.class);
    PartitionGroupMetadata metadata = new PartitionGroupMetadata(0, offset);

    // First call: topic2 throws. Second call: both succeed.
    StreamMetadataProvider provider = mock(StreamMetadataProvider.class);
    when(provider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(List.of(metadata))           // topic1 first call
        .thenThrow(new PermanentConsumerException(new RuntimeException("Topic does not exist")))   // topic2 first call
        .thenReturn(List.of(metadata))           // topic1 second call
        .thenReturn(List.of(metadata));          // topic2 second call

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(provider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, List.of(), List.of(), false);

      // First call: topic2 fails — only topic1's metadata returned, no exception
      Boolean result1 = fetcher.call();
      Assert.assertTrue(result1);
      Assert.assertEquals(fetcher.getStreamMetadataList().size(), 1);
      Assert.assertNull(fetcher.getException());

      // Second call: both succeed
      Boolean result2 = fetcher.call();
      Assert.assertTrue(result2);
      Assert.assertEquals(fetcher.getStreamMetadataList().size(), 2);
    }
  }

  @Test
  public void testFetchMultipleStreamsNonPermanentExceptionStillPropagates()
      throws Exception {
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table_REALTIME", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table_REALTIME", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    StreamMetadataProvider goodProvider = mock(StreamMetadataProvider.class);
    when(goodProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(List.of(new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class))));

    // Generic RuntimeException (not PermanentConsumerException) — auth error, NPE, etc.
    StreamMetadataProvider badProvider = mock(StreamMetadataProvider.class);
    when(badProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new RuntimeException("Auth failure"));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString()))
        .thenReturn(goodProvider)
        .thenReturn(badProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, List.of(), List.of(), false);

      try {
        fetcher.call();
        Assert.fail("Expected RuntimeException to propagate");
      } catch (RuntimeException e) {
        Assert.assertEquals(e.getMessage(), "Auth failure");
      }
    }
  }

  private StreamConfig createMockStreamConfig(String topicName, String tableName, boolean isEphemeral) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicName()).thenReturn(topicName);
    when(streamConfig.getTableNameWithType()).thenReturn(tableName);
    return streamConfig;
  }
}
