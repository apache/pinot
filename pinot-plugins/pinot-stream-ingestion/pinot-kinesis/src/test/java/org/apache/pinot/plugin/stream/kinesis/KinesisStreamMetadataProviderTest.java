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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import static org.apache.pinot.plugin.stream.kinesis.KinesisStreamMetadataProvider.SHARD_ID_PREFIX;
import static org.apache.pinot.spi.stream.OffsetCriteria.LARGEST_OFFSET_CRITERIA;
import static org.apache.pinot.spi.stream.OffsetCriteria.SMALLEST_OFFSET_CRITERIA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class KinesisStreamMetadataProviderTest {
  private static final String STREAM_NAME = "kinesis-test";
  private static final String AWS_REGION = "us-west-2";
  private static final String SHARD_ID_0 = "0";
  private static final String SHARD_ID_1 = "1";
  private static final String CLIENT_ID = "dummy";
  private static final int TIMEOUT = 1000;

  private KinesisConnectionHandler _kinesisConnectionHandler;
  private KinesisStreamMetadataProvider _kinesisStreamMetadataProvider;
  private StreamConsumerFactory _streamConsumerFactory;
  private PartitionGroupConsumer _partitionGroupConsumer;

  private StreamConfig getStreamConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(KinesisConfig.REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, "10");
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    props.put(StreamConfigProperties.STREAM_TYPE, "kinesis");
    props.put("stream.kinesis.consumer.type", "lowLevel");
    props.put("stream.kinesis.topic.name", STREAM_NAME);
    props.put("stream.kinesis.decoder.class.name", "ABCD");
    props.put("stream.kinesis.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory");
    return new StreamConfig("", props);
  }

  @BeforeMethod
  public void setupTest() {
    _kinesisConnectionHandler = mock(KinesisConnectionHandler.class);
    _streamConsumerFactory = mock(StreamConsumerFactory.class);
    _partitionGroupConsumer = mock(PartitionGroupConsumer.class);
    _kinesisStreamMetadataProvider =
        new KinesisStreamMetadataProvider(CLIENT_ID, getStreamConfig(), _kinesisConnectionHandler,
            _streamConsumerFactory);
  }

  @Test
  public void getPartitionsGroupInfoListTest()
      throws Exception {
    Shard shard0 = Shard.builder().shardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID_1)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();

    when(_kinesisConnectionHandler.getShards()).thenReturn(ImmutableList.of(shard0, shard1));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(), new ArrayList<>(),
            TIMEOUT);

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
    Assert.assertEquals(result.get(1).getPartitionGroupId(), 1);
  }

  @Test
  public void fetchStreamPartitionOffsetTest() {
    Shard shard0 = Shard.builder().shardId(SHARD_ID_PREFIX + SHARD_ID_0)
        .sequenceNumberRange(
            SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID_PREFIX + SHARD_ID_1)
        .sequenceNumberRange(
            SequenceNumberRange.builder().startingSequenceNumber("2").endingSequenceNumber("200").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(ImmutableList.of(shard0, shard1));

    KinesisStreamMetadataProvider kinesisStreamMetadataProviderShard0 =
        new KinesisStreamMetadataProvider(CLIENT_ID, getStreamConfig(), SHARD_ID_0, _kinesisConnectionHandler,
            _streamConsumerFactory);
    Assert.assertEquals(kinesisStreamMetadataProviderShard0.fetchPartitionCount(TIMEOUT), 2);

    KinesisPartitionGroupOffset kinesisPartitionGroupOffset =
        (KinesisPartitionGroupOffset) kinesisStreamMetadataProviderShard0.fetchStreamPartitionOffset(
            SMALLEST_OFFSET_CRITERIA, TIMEOUT);
    Assert.assertEquals(kinesisPartitionGroupOffset.getShardId(), SHARD_ID_PREFIX + SHARD_ID_0);
    Assert.assertEquals(kinesisPartitionGroupOffset.getSequenceNumber(), "1");

    kinesisPartitionGroupOffset =
        (KinesisPartitionGroupOffset) kinesisStreamMetadataProviderShard0.fetchStreamPartitionOffset(
            LARGEST_OFFSET_CRITERIA, TIMEOUT);
    Assert.assertEquals(kinesisPartitionGroupOffset.getShardId(), SHARD_ID_PREFIX + SHARD_ID_0);
    Assert.assertEquals(kinesisPartitionGroupOffset.getSequenceNumber(), "100");

    KinesisStreamMetadataProvider kinesisStreamMetadataProviderShard1 =
        new KinesisStreamMetadataProvider(CLIENT_ID, getStreamConfig(), SHARD_ID_1, _kinesisConnectionHandler,
            _streamConsumerFactory);
    Assert.assertEquals(kinesisStreamMetadataProviderShard1.fetchPartitionCount(TIMEOUT), 2);

    kinesisPartitionGroupOffset =
        (KinesisPartitionGroupOffset) kinesisStreamMetadataProviderShard1.fetchStreamPartitionOffset(
            SMALLEST_OFFSET_CRITERIA, TIMEOUT);
    Assert.assertEquals(kinesisPartitionGroupOffset.getShardId(), SHARD_ID_PREFIX + SHARD_ID_1);
    Assert.assertEquals(kinesisPartitionGroupOffset.getSequenceNumber(), "2");

    kinesisPartitionGroupOffset =
        (KinesisPartitionGroupOffset) kinesisStreamMetadataProviderShard1.fetchStreamPartitionOffset(
            LARGEST_OFFSET_CRITERIA, TIMEOUT);
    Assert.assertEquals(kinesisPartitionGroupOffset.getShardId(), SHARD_ID_PREFIX + SHARD_ID_1);
    Assert.assertEquals(kinesisPartitionGroupOffset.getSequenceNumber(), "200");
  }

  @Test
  public void getPartitionsGroupInfoEndOfShardTest()
      throws Exception {
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = new ArrayList<>();

    KinesisPartitionGroupOffset kinesisPartitionGroupOffset = new KinesisPartitionGroupOffset("0", "1");

    currentPartitionGroupMeta.add(
        new PartitionGroupConsumptionStatus(0, 1, kinesisPartitionGroupOffset, kinesisPartitionGroupOffset,
            "CONSUMING"));

    ArgumentCaptor<StreamPartitionMsgOffset> checkpointArgs = ArgumentCaptor.forClass(StreamPartitionMsgOffset.class);
    ArgumentCaptor<PartitionGroupConsumptionStatus> partitionGroupMetadataCapture =
        ArgumentCaptor.forClass(PartitionGroupConsumptionStatus.class);
    ArgumentCaptor<Integer> intArguments = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<String> stringCapture = ArgumentCaptor.forClass(String.class);

    Shard shard0 = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("1").build()).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID_1)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(ImmutableList.of(shard0, shard1));
    when(_streamConsumerFactory.createPartitionGroupConsumer(stringCapture.capture(),
        partitionGroupMetadataCapture.capture())).thenReturn(_partitionGroupConsumer);
    when(_partitionGroupConsumer.fetchMessages(checkpointArgs.capture(), intArguments.capture())).thenReturn(
        new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, true));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    Assert.assertEquals(partitionGroupMetadataCapture.getValue().getSequenceNumber(), 1);
  }

  @Test
  public void getPartitionsGroupInfoChildShardsest()
      throws Exception {
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = new ArrayList<>();

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("1", "1");
    KinesisPartitionGroupOffset kinesisPartitionGroupOffset = new KinesisPartitionGroupOffset("1", "1");

    currentPartitionGroupMeta.add(
        new PartitionGroupConsumptionStatus(0, 1, kinesisPartitionGroupOffset, kinesisPartitionGroupOffset,
            "CONSUMING"));

    ArgumentCaptor<StreamPartitionMsgOffset> checkpointArgs = ArgumentCaptor.forClass(StreamPartitionMsgOffset.class);
    ArgumentCaptor<PartitionGroupConsumptionStatus> partitionGroupMetadataCapture =
        ArgumentCaptor.forClass(PartitionGroupConsumptionStatus.class);
    ArgumentCaptor<Integer> intArguments = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<String> stringCapture = ArgumentCaptor.forClass(String.class);

    Shard shard0 = Shard.builder().shardId(SHARD_ID_0).parentShardId(SHARD_ID_1)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID_1).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("1").build()).build();

    when(_kinesisConnectionHandler.getShards()).thenReturn(ImmutableList.of(shard0, shard1));
    when(_streamConsumerFactory.createPartitionGroupConsumer(stringCapture.capture(),
        partitionGroupMetadataCapture.capture())).thenReturn(_partitionGroupConsumer);
    when(_partitionGroupConsumer.fetchMessages(checkpointArgs.capture(), intArguments.capture())).thenReturn(
        new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, true));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
    Assert.assertEquals(partitionGroupMetadataCapture.getValue().getSequenceNumber(), 1);
  }
}
