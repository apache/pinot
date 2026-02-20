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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.plugin.stream.kafka30.KafkaStreamMetadataProvider;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MicroBatchStreamMetadataProviderTest {

  @Mock
  private KafkaStreamMetadataProvider _mockKafkaProvider;

  private MicroBatchStreamMetadataProvider _provider;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _provider = new MicroBatchStreamMetadataProvider(_mockKafkaProvider);
  }

  @Test
  public void testSupportsOffsetLagReturnsFalse() {
    // MicroBatch cannot accurately estimate offset lag because it doesn't know
    // how many records are in each microbatch
    assertFalse(_provider.supportsOffsetLag());
  }

  @Test
  public void testFetchPartitionCountDelegatesToKafkaProvider() {
    when(_mockKafkaProvider.fetchPartitionCount(5000L)).thenReturn(4);

    int count = _provider.fetchPartitionCount(5000L);

    assertEquals(count, 4);
    verify(_mockKafkaProvider).fetchPartitionCount(5000L);
  }

  @Test
  public void testFetchPartitionIdsDelegatesToKafkaProvider() {
    Set<Integer> expectedPartitions = new HashSet<>();
    expectedPartitions.add(0);
    expectedPartitions.add(1);
    expectedPartitions.add(2);
    when(_mockKafkaProvider.fetchPartitionIds(5000L)).thenReturn(expectedPartitions);

    Set<Integer> partitions = _provider.fetchPartitionIds(5000L);

    assertEquals(partitions, expectedPartitions);
    verify(_mockKafkaProvider).fetchPartitionIds(5000L);
  }

  @Test
  public void testFetchLatestStreamOffsetWrapsOffsets() {
    // Setup mock to return LongMsgOffset
    Map<Integer, StreamPartitionMsgOffset> kafkaOffsets = new HashMap<>();
    kafkaOffsets.put(0, new LongMsgOffset(100L));
    kafkaOffsets.put(1, new LongMsgOffset(200L));

    Set<Integer> partitionIds = new HashSet<>();
    partitionIds.add(0);
    partitionIds.add(1);

    when(_mockKafkaProvider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(kafkaOffsets);

    Map<Integer, StreamPartitionMsgOffset> result = _provider.fetchLatestStreamOffset(partitionIds, 5000L);

    assertNotNull(result);
    assertEquals(result.size(), 2);

    // Verify offsets are wrapped as MicroBatchStreamPartitionMsgOffset
    assertTrue(result.get(0) instanceof MicroBatchStreamPartitionMsgOffset);
    assertTrue(result.get(1) instanceof MicroBatchStreamPartitionMsgOffset);

    MicroBatchStreamPartitionMsgOffset offset0 = (MicroBatchStreamPartitionMsgOffset) result.get(0);
    assertEquals(offset0.getKafkaMessageOffset(), 100L);
    assertEquals(offset0.getRecordOffsetInMicroBatch(), 0L);

    MicroBatchStreamPartitionMsgOffset offset1 = (MicroBatchStreamPartitionMsgOffset) result.get(1);
    assertEquals(offset1.getKafkaMessageOffset(), 200L);
    assertEquals(offset1.getRecordOffsetInMicroBatch(), 0L);
  }

  @Test
  public void testFetchStreamPartitionOffsetWrapsOffset() {
    when(_mockKafkaProvider.fetchStreamPartitionOffset(any(OffsetCriteria.class), anyLong()))
        .thenReturn(new LongMsgOffset(500L));

    StreamPartitionMsgOffset result = _provider.fetchStreamPartitionOffset(
        OffsetCriteria.SMALLEST_OFFSET_CRITERIA, 5000L);

    assertNotNull(result);
    assertTrue(result instanceof MicroBatchStreamPartitionMsgOffset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) result;
    assertEquals(mbOffset.getKafkaMessageOffset(), 500L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 0L);
  }

  @Test
  public void testGetOffsetAtTimestampWrapsOffset() {
    when(_mockKafkaProvider.getOffsetAtTimestamp(anyInt(), anyLong(), anyLong()))
        .thenReturn(new LongMsgOffset(750L));

    StreamPartitionMsgOffset result = _provider.getOffsetAtTimestamp(0, 1234567890L, 5000L);

    assertNotNull(result);
    assertTrue(result instanceof MicroBatchStreamPartitionMsgOffset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) result;
    assertEquals(mbOffset.getKafkaMessageOffset(), 750L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 0L);
  }

  @Test
  public void testGetStreamStartOffsetsWrapsOffsets() {
    Map<String, StreamPartitionMsgOffset> kafkaOffsets = new HashMap<>();
    kafkaOffsets.put("0", new LongMsgOffset(0L));
    kafkaOffsets.put("1", new LongMsgOffset(10L));

    when(_mockKafkaProvider.getStreamStartOffsets()).thenReturn(kafkaOffsets);

    Map<String, StreamPartitionMsgOffset> result = _provider.getStreamStartOffsets();

    assertNotNull(result);
    assertEquals(result.size(), 2);

    assertTrue(result.get("0") instanceof MicroBatchStreamPartitionMsgOffset);
    MicroBatchStreamPartitionMsgOffset offset0 = (MicroBatchStreamPartitionMsgOffset) result.get("0");
    assertEquals(offset0.getKafkaMessageOffset(), 0L);
  }

  @Test
  public void testGetStreamEndOffsetsWrapsOffsets() {
    Map<String, StreamPartitionMsgOffset> kafkaOffsets = new HashMap<>();
    kafkaOffsets.put("0", new LongMsgOffset(1000L));
    kafkaOffsets.put("1", new LongMsgOffset(2000L));

    when(_mockKafkaProvider.getStreamEndOffsets()).thenReturn(kafkaOffsets);

    Map<String, StreamPartitionMsgOffset> result = _provider.getStreamEndOffsets();

    assertNotNull(result);
    assertEquals(result.size(), 2);

    assertTrue(result.get("0") instanceof MicroBatchStreamPartitionMsgOffset);
    MicroBatchStreamPartitionMsgOffset offset0 = (MicroBatchStreamPartitionMsgOffset) result.get("0");
    assertEquals(offset0.getKafkaMessageOffset(), 1000L);

    MicroBatchStreamPartitionMsgOffset offset1 = (MicroBatchStreamPartitionMsgOffset) result.get("1");
    assertEquals(offset1.getKafkaMessageOffset(), 2000L);
  }

  @Test
  public void testGetTopicsDelegatesToKafkaProvider() {
    _provider.getTopics();
    verify(_mockKafkaProvider).getTopics();
  }

  @Test
  public void testCloseDelegatesToKafkaProvider() throws IOException {
    _provider.close();
    verify(_mockKafkaProvider).close();
  }

  @Test
  public void testWrappedOffsetHasZeroRecordOffset() {
    // All wrapped offsets should have recordOffsetInMicroBatch = 0
    // because we're wrapping Kafka offsets which don't have microbatch record info
    when(_mockKafkaProvider.fetchStreamPartitionOffset(any(OffsetCriteria.class), anyLong()))
        .thenReturn(new LongMsgOffset(999L));

    StreamPartitionMsgOffset result = _provider.fetchStreamPartitionOffset(
        OffsetCriteria.LARGEST_OFFSET_CRITERIA, 5000L);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) result;
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 0L,
        "Wrapped offsets should always have recordOffsetInMicroBatch = 0");
  }
}
