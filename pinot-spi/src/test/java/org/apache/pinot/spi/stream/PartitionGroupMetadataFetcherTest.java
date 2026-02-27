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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PartitionGroupMetadataFetcherTest {

  @Test
  public void testFetchSingleStreamSuccess()
      throws Exception {
    // Setup
    StreamConfig streamConfig = createMockStreamConfig("test-topic", "test-table", false);
    List<StreamConfig> streamConfigs = Collections.singletonList(streamConfig);

    PartitionGroupConsumptionStatus status = mock(PartitionGroupConsumptionStatus.class);
    when(status.getPartitionGroupId()).thenReturn(0);
    List<PartitionGroupConsumptionStatus> statusList = Collections.singletonList(status);

    PartitionGroupMetadata metadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));
    List<PartitionGroupMetadata> metadataList = Collections.singletonList(metadata);

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean())).thenReturn(metadataList);

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertTrue(result);
      Assert.assertEquals(fetcher.getPartitionGroupMetadataList().size(), 1);
      Assert.assertNull(fetcher.getException());
    }
  }

  @Test
  public void testFetchSingleStreamTransientException()
      throws Exception {
    // Setup
    StreamConfig streamConfig = createMockStreamConfig("test-topic", "test-table", false);
    List<StreamConfig> streamConfigs = Collections.singletonList(streamConfig);

    List<PartitionGroupConsumptionStatus> statusList = Collections.emptyList();

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new TransientConsumerException(new RuntimeException("Transient error")));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

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
    // Mock getTopics() to throw UnsupportedOperationException (bypasses topic existence check)
    when(metadataProvider.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Arrays.asList(mockedMetadata1, mockedMetadata2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertTrue(result);
      Assert.assertEquals(fetcher.getPartitionGroupMetadataList().size(), 4);
      Assert.assertNull(fetcher.getException());

      // Verify the correct partition group IDs: 0, 1, 10000, 10001
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();
      List<Integer> partitionIds = resultMetadata.stream()
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
    // Mock getTopics() to throw UnsupportedOperationException (bypasses topic existence check)
    when(metadataProvider.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Arrays.asList(mockedMetadata1, mockedMetadata2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Arrays.asList(1), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertTrue(result);
      Assert.assertEquals(fetcher.getPartitionGroupMetadataList().size(), 4);
      Assert.assertNull(fetcher.getException());

      // Verify the correct partition group IDs
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();
      List<Integer> partitionIds = resultMetadata.stream()
          .map(PartitionGroupMetadata::getPartitionGroupId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionIds, Arrays.asList(0, 1, 20000, 20001));
    }
  }

  @Test
  public void testFetchMultipleStreamsWithExceptionThrows()
      throws Exception {
    // Setup: 3 streams where the second one throws an exception
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2-failing", "test-table", false);
    StreamConfig streamConfig3 = createMockStreamConfig("topic3", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2, streamConfig3);

    PartitionGroupConsumptionStatus status1 = new PartitionGroupConsumptionStatus(0, 0, null, null, "IN_PROGRESS");
    List<PartitionGroupConsumptionStatus> statusList = Collections.singletonList(status1);

    PartitionGroupMetadata mockedMetadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));

    // Create separate metadata providers for each stream
    StreamMetadataProvider successProvider1 = mock(StreamMetadataProvider.class);
    when(successProvider1.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(successProvider1.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Collections.singletonList(mockedMetadata));

    StreamMetadataProvider failingProvider = mock(StreamMetadataProvider.class);
    when(failingProvider.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(failingProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new RuntimeException("Failed to fetch partition count for topic2-failing"));

    StreamMetadataProvider successProvider3 = mock(StreamMetadataProvider.class);
    when(successProvider3.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(successProvider3.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Collections.singletonList(mockedMetadata));

    StreamConsumerFactory factory1 = mock(StreamConsumerFactory.class);
    when(factory1.createStreamMetadataProvider(anyString())).thenReturn(successProvider1);

    StreamConsumerFactory factory2 = mock(StreamConsumerFactory.class);
    when(factory2.createStreamMetadataProvider(anyString())).thenReturn(failingProvider);

    StreamConsumerFactory factory3 = mock(StreamConsumerFactory.class);
    when(factory3.createStreamMetadataProvider(anyString())).thenReturn(successProvider3);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(streamConfig1)).thenReturn(factory1);
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(streamConfig2)).thenReturn(factory2);
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(streamConfig3)).thenReturn(factory3);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute and verify exception is thrown
      try {
        fetcher.call();
        Assert.fail("Expected RuntimeException to be thrown");
      } catch (RuntimeException e) {
        // Verify: exception should contain topic2-failing
        Assert.assertTrue(e.getMessage().contains("topic2-failing"));
        // Verify: exception should also be recorded
        Assert.assertNotNull(fetcher.getException());
        Assert.assertEquals(e, fetcher.getException());
      }

      // Verify: only metadata from topic1 should be collected (before exception)
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();
      Assert.assertEquals(resultMetadata.size(), 1);
      Assert.assertEquals(resultMetadata.get(0).getPartitionGroupId(), 0);
    }
  }

  @Test
  public void testFetchMultipleStreamsTransientExceptionStopsProcessing()
      throws Exception {
    // Setup: TransientConsumerException should cause immediate return with FALSE
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    List<PartitionGroupConsumptionStatus> statusList = Collections.emptyList();

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenThrow(new TransientConsumerException(new RuntimeException("Transient error")));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify: TransientConsumerException should return FALSE immediately
      Assert.assertFalse(result);
      Assert.assertTrue(fetcher.getException() instanceof TransientConsumerException);
    }
  }

  @Test
  public void testFetchMultipleStreamsSkipsNonExistentTopic()
      throws Exception {
    // Setup: 3 streams where the second topic does not exist
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", true);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2-nonexistent", "test-table", true);
    StreamConfig streamConfig3 = createMockStreamConfig("topic3", "test-table", true);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2, streamConfig3);

    List<PartitionGroupConsumptionStatus> statusList = Collections.emptyList();

    PartitionGroupMetadata mockedMetadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));

    // Create topic metadata for existing topics only (topic1 and topic3)
    StreamMetadataProvider.TopicMetadata topic1Metadata = mock(StreamMetadataProvider.TopicMetadata.class);
    when(topic1Metadata.getName()).thenReturn("topic1");
    StreamMetadataProvider.TopicMetadata topic3Metadata = mock(StreamMetadataProvider.TopicMetadata.class);
    when(topic3Metadata.getName()).thenReturn("topic3");

    // Provider for topic1 - topic exists
    StreamMetadataProvider provider1 = mock(StreamMetadataProvider.class);
    when(provider1.getTopics()).thenReturn(Arrays.asList(topic1Metadata, topic3Metadata));
    when(provider1.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Collections.singletonList(mockedMetadata));

    // Provider for topic2 - topic does NOT exist (getTopics returns list without topic2)
    StreamMetadataProvider provider2 = mock(StreamMetadataProvider.class);
    when(provider2.getTopics()).thenReturn(Arrays.asList(topic1Metadata, topic3Metadata));

    // Provider for topic3 - topic exists
    StreamMetadataProvider provider3 = mock(StreamMetadataProvider.class);
    when(provider3.getTopics()).thenReturn(Arrays.asList(topic1Metadata, topic3Metadata));
    when(provider3.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Collections.singletonList(mockedMetadata));

    StreamConsumerFactory factory1 = mock(StreamConsumerFactory.class);
    when(factory1.createStreamMetadataProvider(anyString())).thenReturn(provider1);

    StreamConsumerFactory factory2 = mock(StreamConsumerFactory.class);
    when(factory2.createStreamMetadataProvider(anyString())).thenReturn(provider2);

    StreamConsumerFactory factory3 = mock(StreamConsumerFactory.class);
    when(factory3.createStreamMetadataProvider(anyString())).thenReturn(provider3);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(streamConfig1)).thenReturn(factory1);
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(streamConfig2)).thenReturn(factory2);
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(streamConfig3)).thenReturn(factory3);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify: should return TRUE, non-existent topic is skipped
      Assert.assertTrue(result);
      Assert.assertNull(fetcher.getException());

      // Verify: metadata only from topic1 (index 0) and topic3 (index 2)
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();
      Assert.assertEquals(resultMetadata.size(), 2);

      List<Integer> partitionIds = resultMetadata.stream()
          .map(PartitionGroupMetadata::getPartitionGroupId)
          .sorted()
          .collect(Collectors.toList());

      // Partition IDs: 0 from topic1 (index 0), 20000 from topic3 (index 2)
      Assert.assertEquals(partitionIds, Arrays.asList(0, 20000));
    }
  }

  @Test
  public void testFetchMultipleStreamsProceedsWhenGetTopicsUnsupported()
      throws Exception {
    // Setup: getTopics() throws UnsupportedOperationException, should proceed without validation
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", false);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", false);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    List<PartitionGroupConsumptionStatus> statusList = Collections.emptyList();

    PartitionGroupMetadata mockedMetadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    // getTopics() throws UnsupportedOperationException (default behavior for non-Kafka streams)
    when(metadataProvider.getTopics()).thenThrow(new UnsupportedOperationException("Not supported"));
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Collections.singletonList(mockedMetadata));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify: should return TRUE, processing continues despite UnsupportedOperationException
      Assert.assertTrue(result);
      Assert.assertNull(fetcher.getException());

      // Verify: metadata from both topics should be collected
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();
      Assert.assertEquals(resultMetadata.size(), 2);

      List<Integer> partitionIds = resultMetadata.stream()
          .map(PartitionGroupMetadata::getPartitionGroupId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionIds, Arrays.asList(0, 10000));
    }
  }

  @Test
  public void testFetchMultipleStreamsTopicExistsCheckPasses()
      throws Exception {
    // Setup: All topics exist, processing should proceed normally
    StreamConfig streamConfig1 = createMockStreamConfig("topic1", "test-table", true);
    StreamConfig streamConfig2 = createMockStreamConfig("topic2", "test-table", true);
    List<StreamConfig> streamConfigs = Arrays.asList(streamConfig1, streamConfig2);

    List<PartitionGroupConsumptionStatus> statusList = Collections.emptyList();

    PartitionGroupMetadata mockedMetadata = new PartitionGroupMetadata(0, mock(StreamPartitionMsgOffset.class));

    // Create topic metadata for both topics
    StreamMetadataProvider.TopicMetadata topic1Metadata = mock(StreamMetadataProvider.TopicMetadata.class);
    when(topic1Metadata.getName()).thenReturn("topic1");
    StreamMetadataProvider.TopicMetadata topic2Metadata = mock(StreamMetadataProvider.TopicMetadata.class);
    when(topic2Metadata.getName()).thenReturn("topic2");

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.getTopics()).thenReturn(Arrays.asList(topic1Metadata, topic2Metadata));
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Collections.singletonList(mockedMetadata));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Collections.emptyList(), false, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify: should return TRUE
      Assert.assertTrue(result);
      Assert.assertNull(fetcher.getException());

      // Verify: metadata from both topics should be collected
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();
      Assert.assertEquals(resultMetadata.size(), 2);

      List<Integer> partitionIds = resultMetadata.stream()
          .map(PartitionGroupMetadata::getPartitionGroupId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionIds, Arrays.asList(0, 10000));
    }
  }

  private StreamConfig createMockStreamConfig(String topicName, String tableName,
      boolean topicExistenceCheckEnabled) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicName()).thenReturn(topicName);
    when(streamConfig.getTableNameWithType()).thenReturn(tableName);
    Map<String, String> configsMap = new HashMap<>();
    if (topicExistenceCheckEnabled) {
      configsMap.put(StreamConfigProperties.MULTITOPIC_SKIP_MISSING_TOPICS, "true");
    }
    when(streamConfig.getStreamConfigsMap()).thenReturn(configsMap);
    return streamConfig;
  }
}
