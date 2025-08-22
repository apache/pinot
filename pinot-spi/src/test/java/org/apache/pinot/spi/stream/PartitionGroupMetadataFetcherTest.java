package org.apache.pinot.spi.stream;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
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

    PartitionGroupMetadata metadata = new PartitionGroupMetadata("", 0, mock(StreamPartitionMsgOffset.class));
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
          streamConfigs, statusList, false);

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
          streamConfigs, statusList, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertFalse(result);
      Assert.assertTrue(fetcher.getException() instanceof TransientConsumerException);
    }
  }

  @Test
  public void testFetchMultipleStreamsWithoutEphemeralTopics()
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
          streamConfigs, statusList, false);

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

      List<String> partitionTopicsAndIds = resultMetadata.stream()
          .map(PartitionGroupMetadata::getPartitionGroupTopicAndId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionTopicsAndIds, Arrays.asList("0", "1", "10000", "10001"));
    }
  }

  @Test
  public void testFetchMultipleStreamsWithEphemeralTopics()
      throws Exception {
    // Setup - mix of permanent and ephemeral topics
    StreamConfig permanentStream1 = createMockStreamConfig("permanent-topic1", "test-table", false);
    StreamConfig ephemeralStream1 = createMockStreamConfig("ephemeral-topic1", "test-table", true);
    StreamConfig permanentStream2 = createMockStreamConfig("permanent-topic2", "test-table", false);
    StreamConfig ephemeralStream2 = createMockStreamConfig("ephemeral-topic2", "test-table", true);
    List<StreamConfig> streamConfigs = Arrays.asList(permanentStream1, ephemeralStream1, permanentStream2, ephemeralStream2);

    // Mock consumption status for all topics
    PartitionGroupConsumptionStatus statusPer1 = new PartitionGroupConsumptionStatus(0, 0, null, null, "IN_PROGRESS");
    PartitionGroupConsumptionStatus statusPer2 = new PartitionGroupConsumptionStatus(
        10000, 0, 1, null, null, "IN_PROGRESS");
    PartitionGroupConsumptionStatus statusEph1 = new PartitionGroupConsumptionStatus(
        "ephemeral-topic1", 0, 0, 1, null, null, "IN_PROGRESS");

    List<PartitionGroupConsumptionStatus> statusList = Arrays.asList(statusPer1, statusPer2, statusEph1);

    // Mock metadata returned by stream providers, all topics are with 2 partitions
    PartitionGroupMetadata metadata1 = new PartitionGroupMetadata("", 0, mock(StreamPartitionMsgOffset.class));
    PartitionGroupMetadata metadata2 = new PartitionGroupMetadata("", 1, mock(StreamPartitionMsgOffset.class));

    StreamMetadataProvider metadataProvider = mock(StreamMetadataProvider.class);
    when(metadataProvider.computePartitionGroupMetadata(anyString(), any(StreamConfig.class),
        any(List.class), anyInt(), anyBoolean()))
        .thenReturn(Arrays.asList(metadata1, metadata2));

    StreamConsumerFactory factory = mock(StreamConsumerFactory.class);
    when(factory.createStreamMetadataProvider(anyString())).thenReturn(metadataProvider);

    try (MockedStatic<StreamConsumerFactoryProvider> mockedProvider = Mockito.mockStatic(
        StreamConsumerFactoryProvider.class)) {

      mockedProvider.when(() -> StreamConsumerFactoryProvider.create(any(StreamConfig.class))).thenReturn(factory);

      PartitionGroupMetadataFetcher fetcher = new PartitionGroupMetadataFetcher(
          streamConfigs, statusList, false);

      // Execute
      Boolean result = fetcher.call();

      // Verify
      Assert.assertTrue(result);
      Assert.assertEquals(fetcher.getPartitionGroupMetadataList().size(), 8);
      Assert.assertNull(fetcher.getException());

      // Verify the correct partition group IDs
      List<PartitionGroupMetadata> resultMetadata = fetcher.getPartitionGroupMetadataList();

      List<String> partitionTopicsAndIds = resultMetadata.stream()
          .map(PartitionGroupMetadata::getPartitionGroupTopicAndId)
          .sorted()
          .collect(Collectors.toList());

      Assert.assertEquals(partitionTopicsAndIds,
          Arrays.asList("0", "1", "10000", "10001", "ephemeral-topic1__0", "ephemeral-topic1__1", "ephemeral-topic2__0",
              "ephemeral-topic2__1"));
    }
  }

  private StreamConfig createMockStreamConfig(String topicName, String tableName, boolean isEphemeral) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTopicName()).thenReturn(topicName);
    when(streamConfig.getTableNameWithType()).thenReturn(tableName);
    when(streamConfig.isEphemeralBackfillTopic()).thenReturn(isEphemeral);
    return streamConfig;
  }
}