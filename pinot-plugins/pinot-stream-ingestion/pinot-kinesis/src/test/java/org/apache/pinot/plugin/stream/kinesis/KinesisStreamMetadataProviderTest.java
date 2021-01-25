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
import org.apache.pinot.spi.stream.Checkpoint;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupInfo;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.captureInt;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

@PrepareForTest(Shard.class)
public class KinesisStreamMetadataProviderTest extends PowerMockTestCase {
  private static final String SHARD_ID_0 = "0";
  private static final String SHARD_ID_1 = "1";
  public static final String CLIENT_ID = "dummy";
  public static final int TIMEOUT = 1000;

  private static Shard shard0;
  private static Shard shard1;
  private static KinesisConnectionHandler kinesisConnectionHandler;
  private KinesisStreamMetadataProvider kinesisStreamMetadataProvider;
  private static StreamConsumerFactory streamConsumerFactory;
  private static PartitionGroupConsumer partitionGroupConsumer;

  @BeforeMethod
  public void setupTest() {
    kinesisConnectionHandler = PowerMock.createMock(KinesisConnectionHandler.class);
    shard0 = PowerMock.createMock(Shard.class);
    shard1 = PowerMock.createMock(Shard.class);
    streamConsumerFactory = PowerMock.createMock(StreamConsumerFactory.class);
    partitionGroupConsumer = PowerMock.createNiceMock(PartitionGroupConsumer.class);
    kinesisStreamMetadataProvider =
        new KinesisStreamMetadataProvider(CLIENT_ID, TestUtils.getStreamConfig(), kinesisConnectionHandler,
            streamConsumerFactory);
  }

  @Test
  public void getPartitionsGroupInfoListTest()
      throws Exception {
    expect(kinesisConnectionHandler.getShards()).andReturn(ImmutableList.of(shard0, shard1)).anyTimes();
    expect(shard0.shardId()).andReturn(SHARD_ID_0).anyTimes();
    expect(shard1.shardId()).andReturn(SHARD_ID_1).anyTimes();
    expect(shard0.parentShardId()).andReturn(null).anyTimes();
    expect(shard1.parentShardId()).andReturn(null).anyTimes();
    expect(shard0.sequenceNumberRange())
        .andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();
    expect(shard1.sequenceNumberRange())
        .andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();

    replay(kinesisConnectionHandler, shard0, shard1);

    List<PartitionGroupInfo> result = kinesisStreamMetadataProvider
        .getPartitionGroupInfoList(CLIENT_ID, TestUtils.getStreamConfig(), new ArrayList<>(), TIMEOUT);

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
    Assert.assertEquals(result.get(1).getPartitionGroupId(), 1);
  }

  @Test
  public void getPartitionsGroupInfoEndOfShardTest()
      throws Exception {
    List<PartitionGroupMetadata> currentPartitionGroupMeta = new ArrayList<>();

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("0", "1");
    KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shardToSequenceMap);

    currentPartitionGroupMeta.add(new PartitionGroupMetadata(0, 1, kinesisCheckpoint, kinesisCheckpoint, "CONSUMING"));

    Capture<Checkpoint> checkpointArgs = newCapture(CaptureType.ALL);
    Capture<PartitionGroupMetadata> partitionGroupMetadataCapture = newCapture(CaptureType.ALL);
    Capture<Integer> intArguments = newCapture(CaptureType.ALL);
    Capture<String> stringCapture = newCapture(CaptureType.ALL);

    expect(kinesisConnectionHandler.getShards()).andReturn(ImmutableList.of(shard0, shard1)).anyTimes();
    expect(shard0.shardId()).andReturn(SHARD_ID_0).anyTimes();
    expect(shard1.shardId()).andReturn(SHARD_ID_1).anyTimes();
    expect(shard0.parentShardId()).andReturn(null).anyTimes();
    expect(shard1.parentShardId()).andReturn(null).anyTimes();
    expect(shard0.sequenceNumberRange())
        .andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("1").build())
        .anyTimes();
    expect(shard1.sequenceNumberRange())
        .andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();
    expect(streamConsumerFactory
        .createPartitionGroupConsumer(capture(stringCapture), capture(partitionGroupMetadataCapture)))
        .andReturn(partitionGroupConsumer).anyTimes();
    expect(partitionGroupConsumer
        .fetchMessages(capture(checkpointArgs), capture(checkpointArgs), captureInt(intArguments)))
        .andReturn(new KinesisRecordsBatch(new ArrayList<>(), "0", true)).anyTimes();

    replay(kinesisConnectionHandler, shard0, shard1, streamConsumerFactory, partitionGroupConsumer);

    List<PartitionGroupInfo> result = kinesisStreamMetadataProvider
        .getPartitionGroupInfoList(CLIENT_ID, TestUtils.getStreamConfig(), currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
  }

  @Test
  public void getPartitionsGroupInfoChildShardsest()
      throws Exception {
    List<PartitionGroupMetadata> currentPartitionGroupMeta = new ArrayList<>();

    Map<String, String> shardToSequenceMap = new HashMap<>();
    shardToSequenceMap.put("1", "1");
    KinesisCheckpoint kinesisCheckpoint = new KinesisCheckpoint(shardToSequenceMap);

    currentPartitionGroupMeta.add(new PartitionGroupMetadata(0, 1, kinesisCheckpoint, kinesisCheckpoint, "CONSUMING"));

    Capture<Checkpoint> checkpointArgs = newCapture(CaptureType.ALL);
    Capture<PartitionGroupMetadata> partitionGroupMetadataCapture = newCapture(CaptureType.ALL);
    Capture<Integer> intArguments = newCapture(CaptureType.ALL);
    Capture<String> stringCapture = newCapture(CaptureType.ALL);

    expect(kinesisConnectionHandler.getShards()).andReturn(ImmutableList.of(shard0, shard1)).anyTimes();
    expect(shard0.shardId()).andReturn(SHARD_ID_0).anyTimes();
    expect(shard1.shardId()).andReturn(SHARD_ID_1).anyTimes();
    expect(shard0.parentShardId()).andReturn(SHARD_ID_1).anyTimes();
    expect(shard1.parentShardId()).andReturn(null).anyTimes();
    expect(shard0.sequenceNumberRange())
        .andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").build()).anyTimes();
    expect(shard1.sequenceNumberRange())
        .andReturn(SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("1").build())
        .anyTimes();
    expect(streamConsumerFactory
        .createPartitionGroupConsumer(capture(stringCapture), capture(partitionGroupMetadataCapture)))
        .andReturn(partitionGroupConsumer).anyTimes();
    expect(partitionGroupConsumer
        .fetchMessages(capture(checkpointArgs), capture(checkpointArgs), captureInt(intArguments)))
        .andReturn(new KinesisRecordsBatch(new ArrayList<>(), "0", true)).anyTimes();

    replay(kinesisConnectionHandler, shard0, shard1, streamConsumerFactory, partitionGroupConsumer);

    List<PartitionGroupInfo> result = kinesisStreamMetadataProvider
        .getPartitionGroupInfoList(CLIENT_ID, TestUtils.getStreamConfig(), currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
  }
}
