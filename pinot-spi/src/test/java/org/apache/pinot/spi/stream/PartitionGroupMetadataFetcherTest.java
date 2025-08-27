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
import java.util.List;
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
  public void testFetchSingleStreamSuccess() throws Exception {
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
          streamConfigs, statusList, Collections.emptyList(),false);

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
          streamConfigs, statusList, Collections.emptyList(), false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertFalse(result);
      Assert.assertTrue(fetcher.getException() instanceof TransientConsumerException);
    }
  }

  @Test
  public void testFetchMultipleStreams() throws Exception {
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
          streamConfigs, statusList, Collections.emptyList(),false);

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
  public void testFetchMultipleStreamsWithPause() throws Exception {
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
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Arrays.asList(mockedMetadata1, mockedMetadata2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {
      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, Arrays.asList(1),false);

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

  private StreamConfig createMockStreamConfig(String topicName, String tableName, boolean isEphemeral) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicName()).thenReturn(topicName);
    when(streamConfig.getTableNameWithType()).thenReturn(tableName);
    return streamConfig;
  }
}