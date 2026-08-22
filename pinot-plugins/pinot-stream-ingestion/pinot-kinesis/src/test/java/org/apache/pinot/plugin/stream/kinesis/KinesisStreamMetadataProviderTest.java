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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    props.put("stream.kinesis.topic.name", STREAM_NAME);
    props.put("stream.kinesis.decoder.class.name", "ABCD");
    props.put("stream.kinesis.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory");
    return new StreamConfig("", props);
  }

  private StreamConfig getStreamConfig(int fetchTimeoutMs) {
    Map<String, String> props = new HashMap<>(getStreamConfig().getStreamConfigsMap());
    props.put("stream.kinesis." + StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS, String.valueOf(fetchTimeoutMs));
    return new StreamConfig("", props);
  }

  /// Provider whose end-of-shard probe loop reads `nowMs` instead of the wall clock, so the time budget is
  /// driven entirely by what the mocked consumer does.
  private KinesisStreamMetadataProvider providerWithClock(StreamConfig streamConfig, AtomicLong nowMs) {
    return new KinesisStreamMetadataProvider(CLIENT_ID, streamConfig, _kinesisConnectionHandler,
        _streamConsumerFactory) {
      @Override
      long currentTimeMillis() {
        return nowMs.get();
      }
    };
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

    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(shard0, shard1));

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
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(shard0, shard1));

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

    // Checkpoint below ending SN so the probe path runs (not the sequence short-circuit).
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
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard shard1 = Shard.builder().shardId(SHARD_ID_1)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(shard0, shard1));
    when(_streamConsumerFactory.createPartitionGroupConsumer(stringCapture.capture(),
        partitionGroupMetadataCapture.capture())).thenReturn(_partitionGroupConsumer);
    when(_partitionGroupConsumer.fetchMessages(checkpointArgs.capture(), intArguments.capture())).thenReturn(
        new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, true, 0));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    Assert.assertEquals(partitionGroupMetadataCapture.getValue().getSequenceNumber(), 1);

    // Simulate the case where initial calls to fetchMessages returns empty messages but non-null next shard iterator
    when(_partitionGroupConsumer.fetchMessages(checkpointArgs.capture(), intArguments.capture()))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, true, 0));
    result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    Assert.assertEquals(partitionGroupMetadataCapture.getValue().getSequenceNumber(), 1);

    // Closed shard + only empty non-EOP polls: assume ended (parent removed). Previously this incorrectly
    // kept the parent live and blocked child shards / caused empty commit loops (#17209).
    when(_partitionGroupConsumer.fetchMessages(checkpointArgs.capture(), intArguments.capture()))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, false, 0));

    result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
  }

  @Test
  public void getPartitionsGroupInfoChildShardsest()
      throws Exception {
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = new ArrayList<>();

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
    // Parent closed with ending SN above checkpoint so EOP probe path runs.
    Shard shard1 = Shard.builder().shardId(SHARD_ID_1).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();

    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(shard0, shard1));
    when(_streamConsumerFactory.createPartitionGroupConsumer(stringCapture.capture(),
        partitionGroupMetadataCapture.capture())).thenReturn(_partitionGroupConsumer);
    when(_partitionGroupConsumer.fetchMessages(checkpointArgs.capture(), intArguments.capture())).thenReturn(
        new KinesisMessageBatch(new ArrayList<>(), kinesisPartitionGroupOffset, true, 0));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
    Assert.assertEquals(partitionGroupMetadataCapture.getValue().getSequenceNumber(), 1);
  }

  @Test
  public void testClosedShardSequenceNumberShortCircuit()
      throws Exception {
    // Checkpoint at/beyond ending SN ⇒ fully consumed without GetRecords.
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "100");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard closedParent = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard child = Shard.builder().shardId(SHARD_ID_1).parentShardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(closedParent, child));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    verify(_streamConsumerFactory, never()).createPartitionGroupConsumer(anyString(), any());

    // Also true when checkpoint is past ending (defensive).
    Assert.assertTrue(KinesisStreamMetadataProvider.hasConsumedThroughEndingSequence(
        new KinesisPartitionGroupOffset(SHARD_ID_0, "150"), "100"));
    Assert.assertTrue(KinesisStreamMetadataProvider.hasConsumedThroughEndingSequence(
        new KinesisPartitionGroupOffset(SHARD_ID_0, "100"), "100"));
    Assert.assertFalse(KinesisStreamMetadataProvider.hasConsumedThroughEndingSequence(
        new KinesisPartitionGroupOffset(SHARD_ID_0, "99"), "100"));
    // BigInteger compare — string compare would get "9" > "100" wrong for unequal lengths.
    Assert.assertFalse(KinesisStreamMetadataProvider.hasConsumedThroughEndingSequence(
        new KinesisPartitionGroupOffset(SHARD_ID_0, "9"), "100"));
    Assert.assertTrue(KinesisStreamMetadataProvider.hasConsumedThroughEndingSequence(
        new KinesisPartitionGroupOffset(SHARD_ID_0, "1000"), "100"));
  }

  @Test
  public void testClosedShardEmptyOnlyProbesAssumeEndedAndAdmitChild()
      throws Exception {
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "1");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard closedParent = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard child = Shard.builder().shardId(SHARD_ID_1).parentShardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(closedParent, child));
    when(_streamConsumerFactory.createPartitionGroupConsumer(anyString(), any()))
        .thenReturn(_partitionGroupConsumer);
    when(_partitionGroupConsumer.fetchMessages(any(), anyInt()))
        .thenReturn(new KinesisMessageBatch(new ArrayList<>(), endOffset, false, 0));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    verify(_partitionGroupConsumer, times(KinesisStreamMetadataProvider.MAX_END_OF_SHARD_EMPTY_PROBES))
        .fetchMessages(any(), anyInt());
  }

  @Test
  public void testClosedShardMessagesRemainingKeepsParent()
      throws Exception {
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "1");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard closedParent = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard child = Shard.builder().shardId(SHARD_ID_1).parentShardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(closedParent, child));
    when(_streamConsumerFactory.createPartitionGroupConsumer(anyString(), any()))
        .thenReturn(_partitionGroupConsumer);

    BytesStreamMessage remaining = new BytesStreamMessage(new byte[]{1},
        new StreamMessageMetadata.Builder().setOffset(endOffset, endOffset).setRecordIngestionTimeMs(1L).build());
    when(_partitionGroupConsumer.fetchMessages(any(), anyInt()))
        .thenReturn(new KinesisMessageBatch(List.of(remaining), endOffset, false, 1));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    // Parent still has unconsumed messages — keep parent, do not admit child yet.
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
  }

  @Test
  public void testActiveIdleShardStaysLive()
      throws Exception {
    // endingSequenceNumber == null means the shard is still open; must not run EOL probe / drop partition.
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "1");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard activeIdle = Shard.builder().shardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(activeIdle));

    List<PartitionGroupMetadata> result =
        _kinesisStreamMetadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(),
            currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
    verify(_streamConsumerFactory, never()).createPartitionGroupConsumer(anyString(), any());
  }

  @Test
  public void testThrottleEmptiesThenEopAssumesEnded()
      throws Exception {
    // Rate-limit/timeout empties burn most of the fetch timeout and must not exhaust the hard empty-probe
    // budget before a later real EOP can be observed.
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "1");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard closedParent = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(closedParent));

    StreamConfig streamConfig = getStreamConfig(100);
    AtomicLong nowMs = new AtomicLong(1_000_000L);
    AtomicInteger fetchCount = new AtomicInteger();
    KinesisStreamMetadataProvider provider = providerWithClock(streamConfig, nowMs);

    when(_streamConsumerFactory.createPartitionGroupConsumer(anyString(), any()))
        .thenReturn(_partitionGroupConsumer);
    when(_partitionGroupConsumer.fetchMessages(any(), anyInt())).thenAnswer(invocation -> {
      int n = fetchCount.incrementAndGet();
      int timeoutMs = invocation.getArgument(1);
      // First several responses simulate throttle/timeout empties (consume most of timeout).
      if (n <= 6) {
        nowMs.addAndGet(Math.max(timeoutMs * 3L / 4L, 1L));
        return new KinesisMessageBatch(List.of(), endOffset, false, 0);
      }
      // Then real EOP
      return new KinesisMessageBatch(List.of(), endOffset, true, 0);
    });

    List<PartitionGroupMetadata> result =
        provider.computePartitionGroupMetadata(CLIENT_ID, streamConfig, currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(result.size(), 0);
    Assert.assertTrue(fetchCount.get() >= 7, "expected throttle empties then EOP, fetches=" + fetchCount.get());
  }

  @Test
  public void testOneMsFetchTimeoutFastEmptiesStillEndShard()
      throws Exception {
    // fetchTimeoutMs = 1 rounds the 3/4-of-timeout transient threshold down to 0. Without the Math.max(1, ...)
    // clamp every instant empty batch looks like a throttle, emptyProbes never increments, and the closed parent
    // is kept live forever (#17209 empty commit loop).
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "1");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard closedParent = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard child = Shard.builder().shardId(SHARD_ID_1).parentShardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(closedParent, child));
    when(_streamConsumerFactory.createPartitionGroupConsumer(anyString(), any()))
        .thenReturn(_partitionGroupConsumer);

    AtomicInteger fetchCount = new AtomicInteger();
    AtomicInteger lastFetchTimeoutMs = new AtomicInteger();
    // Clock never advances: every empty batch comes back in 0ms, i.e. a hard (non-throttle) empty.
    when(_partitionGroupConsumer.fetchMessages(any(), anyInt())).thenAnswer(invocation -> {
      int timeoutMs = invocation.getArgument(1);
      lastFetchTimeoutMs.set(timeoutMs);
      fetchCount.incrementAndGet();
      return new KinesisMessageBatch(List.of(), endOffset, false, 0);
    });

    StreamConfig streamConfig = getStreamConfig(1);
    List<PartitionGroupMetadata> result = providerWithClock(streamConfig, new AtomicLong(1_000_000L))
        .computePartitionGroupMetadata(CLIENT_ID, streamConfig, currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(lastFetchTimeoutMs.get(), 1);
    // Closed parent with only hard empties: ends after the probe budget and the child is admitted.
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    Assert.assertEquals(fetchCount.get(), KinesisStreamMetadataProvider.MAX_END_OF_SHARD_EMPTY_PROBES);
  }

  @Test
  public void testNearDeadlineShrunkTimeoutStillEndsShard()
      throws Exception {
    // Same rounding edge on a normal 100ms config: one throttle empty burns the time budget down to 1ms, which
    // clamps every later fetch timeout to 1ms.
    KinesisPartitionGroupOffset endOffset = new KinesisPartitionGroupOffset(SHARD_ID_0, "1");
    List<PartitionGroupConsumptionStatus> currentPartitionGroupMeta = List.of(
        new PartitionGroupConsumptionStatus(0, 1, endOffset, endOffset, "DONE"));

    Shard closedParent = Shard.builder().shardId(SHARD_ID_0).sequenceNumberRange(
        SequenceNumberRange.builder().startingSequenceNumber("1").endingSequenceNumber("100").build()).build();
    Shard child = Shard.builder().shardId(SHARD_ID_1).parentShardId(SHARD_ID_0)
        .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("1").build()).build();
    when(_kinesisConnectionHandler.getShards()).thenReturn(List.of(closedParent, child));
    when(_streamConsumerFactory.createPartitionGroupConsumer(anyString(), any()))
        .thenReturn(_partitionGroupConsumer);

    int fetchTimeoutMs = 100;
    StreamConfig streamConfig = getStreamConfig(fetchTimeoutMs);
    long timeBudgetMs = (long) fetchTimeoutMs * KinesisStreamMetadataProvider.MAX_END_OF_SHARD_EMPTY_PROBES;
    AtomicLong nowMs = new AtomicLong(1_000_000L);
    AtomicInteger fetchCount = new AtomicInteger();
    AtomicInteger lastFetchTimeoutMs = new AtomicInteger();
    when(_partitionGroupConsumer.fetchMessages(any(), anyInt())).thenAnswer(invocation -> {
      int timeoutMs = invocation.getArgument(1);
      lastFetchTimeoutMs.set(timeoutMs);
      if (fetchCount.incrementAndGet() == 1) {
        // Throttle empty: burns the whole fetch timeout and leaves 1ms of time budget.
        nowMs.addAndGet(timeBudgetMs - 1);
      }
      return new KinesisMessageBatch(List.of(), endOffset, false, 0);
    });

    List<PartitionGroupMetadata> result = providerWithClock(streamConfig, nowMs)
        .computePartitionGroupMetadata(CLIENT_ID, streamConfig, currentPartitionGroupMeta, TIMEOUT);

    Assert.assertEquals(lastFetchTimeoutMs.get(), 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 1);
    // One transient (throttle) empty, then MAX_END_OF_SHARD_EMPTY_PROBES hard empties at the clamped 1ms timeout.
    Assert.assertEquals(fetchCount.get(), KinesisStreamMetadataProvider.MAX_END_OF_SHARD_EMPTY_PROBES + 1);
  }

  @Test
  public void testGetCurrentPartitionLagStateHandlesInvalidIngestionTime() {
    long lastProcessedTimeMs = 1_700_000_100_000L;

    // Shard with a valid upstream ingestion time yields a numeric availability lag.
    StreamMessageMetadata validMetadata = new StreamMessageMetadata.Builder()
        .setOffset(new LongMsgOffset(5), new LongMsgOffset(6))
        .setRecordIngestionTimeMs(lastProcessedTimeMs - 1000L)
        .build();
    // Shards whose ingestion time is missing/invalid: unset (Builder default Long.MIN_VALUE), NO_TIMESTAMP (-1),
    // and epoch 0 (the exact boundary of the > 0 guard). These stand in for a stream that is unreachable/timing
    // out or produces records without a timestamp.
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

    Map<String, PartitionLagState> lagState = _kinesisStreamMetadataProvider.getCurrentPartitionLagState(stateMap);

    Assert.assertEquals(lagState.get("0").getAvailabilityLagMs(), "1000");
    // Regression for issue #18836: an invalid ingestion time must report the NOT_CALCULATED sentinel instead of an
    // epoch-sized value (lastProcessedTimeMs - Long.MIN_VALUE, or lastProcessedTimeMs - (-1) ~= now).
    Assert.assertEquals(lagState.get("1").getAvailabilityLagMs(), PartitionLagState.NOT_CALCULATED);
    Assert.assertEquals(lagState.get("2").getAvailabilityLagMs(), PartitionLagState.NOT_CALCULATED);
    Assert.assertEquals(lagState.get("3").getAvailabilityLagMs(), PartitionLagState.NOT_CALCULATED);
  }
}
