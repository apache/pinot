/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.realtime.segment;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FlushThresholdUpdaterTest {

  /**
   * Tests that we have the right flush threshold set in the segment metadata given the various combinations of servers, partitions and replicas
   */
  @Test
  public void testDefaultUpdateFlushThreshold() {

    PartitionAssignment partitionAssignment = new PartitionAssignment("fakeTable_REALTIME");
    // 4 partitions assigned to 4 servers, 4 replicas => the segments should have 250k rows each (1M / 4)
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();

      for (int replicaId = 1; replicaId <= 4; ++replicaId) {
        instances.add("Server_1.2.3.4_123" + replicaId);
      }

      partitionAssignment.addPartition(Integer.toString(segmentId), instances);
    }

    FlushThresholdUpdater flushThresholdUpdater = new DefaultFlushThresholdUpdater(1000000);
    FlushThresholdUpdaterParams params = new FlushThresholdUpdaterParams();
    params.setPartitionAssignment(partitionAssignment);
    // Check that each segment has 250k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      flushThresholdUpdater.updateFlushThreshold(metadata, params);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 250000);
    }

    // 4 partitions assigned to 4 servers, 2 replicas, 2 partitions/server => the segments should have 500k rows each (1M / 2)
    partitionAssignment.getPartitionToInstances().clear();
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();

      for (int replicaId = 1; replicaId <= 2; ++replicaId) {
        instances.add("Server_1.2.3.4_123" + ((replicaId + segmentId) % 4));
      }

      partitionAssignment.addPartition(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 500k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      flushThresholdUpdater.updateFlushThreshold(metadata, params);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }

    // 4 partitions assigned to 4 servers, 1 replica, 1 partition/server => the segments should have 1M rows each (1M / 1)
    partitionAssignment.getPartitionToInstances().clear();
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      List<String> instances = new ArrayList<>();
      instances.add("Server_1.2.3.4_123" + segmentId);
      partitionAssignment.addPartition(Integer.toString(segmentId), instances);
    }

    // Check that each segment has 1M rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      flushThresholdUpdater.updateFlushThreshold(metadata, params);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 1000000);
    }

    // Assign another partition to all servers => the servers should have 500k rows each (1M / 2)
    List<String> instances = new ArrayList<>();
    for (int replicaId = 1; replicaId <= 4; ++replicaId) {
      instances.add("Server_1.2.3.4_123" + replicaId);
    }
    partitionAssignment.addPartition("5", instances);

    // Check that each segment has 500k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      flushThresholdUpdater.updateFlushThreshold(metadata, params);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }
  }

  private String makeFakeSegmentName(int id) {
    return new LLCSegmentName("fakeTable_REALTIME", id, 0, 1234L).getSegmentName();
  }

  @Test
  public void testSegmentSizeBasedFlushThresholdUpdate() {
    String tableName = "aRealtimeTable_REALTIME";
    SegmentSizeBasedFlushThresholdUpdater segmentSizeBasedFlushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater();

    long startOffset_p0 = 0;
    int seqNum_p0 = 0;
    int partitionId_0 = 0;
    double prevRatio;
    double currentRatio;
    int expectedNumRows;
    long segmentSizeBytes;
    FlushThresholdUpdaterParams params;

    // partition:0 segment:0 - new segment
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_0 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p0_0, params);
    Assert.assertEquals(segmentMetadata_p0_0.getSizeThresholdToFlushSegment(),
        SegmentSizeBasedFlushThresholdUpdater.INITIAL_ROWS_THRESHOLD);

    // partition:0 segment:0 commits with 200M size
    segmentSizeBytes = 200 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p0_0, startOffset_p0,
        segmentMetadata_p0_0.getSizeThresholdToFlushSegment());
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_1 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_0);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p0_1, params);
    currentRatio = (double) segmentMetadata_p0_0.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = segmentMetadata_p0_0.getSizeThresholdToFlushSegment() * 2;
    Assert.assertEquals(segmentMetadata_p0_1.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:0 segment:1 commits with 420M size
    segmentSizeBytes = 420 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p0_1, startOffset_p0,
        segmentMetadata_p0_1.getSizeThresholdToFlushSegment());
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_2 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_1);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p0_2, params);
    currentRatio = (double) segmentMetadata_p0_1.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_2.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:0 segment:2 commits with 485M size
    segmentSizeBytes = 485 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p0_2, startOffset_p0,
        segmentMetadata_p0_2.getSizeThresholdToFlushSegment());
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_3 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_2);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p0_3, params);
    currentRatio = (double) segmentMetadata_p0_2.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_3.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:1 segment:0 created
    long startOffset_p1 = 0;
    int seqNum_p1 = 0;
    int partitionId_1 = 1;
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_0 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p1_0, params);
    expectedNumRows = getTargetNumRows(prevRatio);
    Assert.assertEquals(segmentMetadata_p1_0.getSizeThresholdToFlushSegment(), expectedNumRows);

    // partition:0 segment:3 commits with 520M size
    segmentSizeBytes = 520 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p0_3, startOffset_p0,
        segmentMetadata_p0_3.getSizeThresholdToFlushSegment());
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_4 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_3);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p0_4, params);
    currentRatio = (double) segmentMetadata_p0_3.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_4.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:1 segment:0 in error state. repair case. committing segment size=0.
    segmentSizeBytes = 0;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p1_0, -1, -1);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_1 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p1_0);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p1_1, params);
    expectedNumRows = segmentMetadata_p1_0.getSizeThresholdToFlushSegment();
    Assert.assertEquals(segmentMetadata_p1_1.getSizeThresholdToFlushSegment(), expectedNumRows);

    // partition:1 segment:1 commits with 490M size
    segmentSizeBytes = 490 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p1_1, startOffset_p1,
        segmentMetadata_p1_1.getSizeThresholdToFlushSegment());
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_2 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p1_1);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p1_2, params);
    currentRatio = (double) segmentMetadata_p1_1.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p1_2.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:1 segment:2 commits with 790M size
    segmentSizeBytes = 790 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p1_2, startOffset_p1,
        segmentMetadata_p1_2.getSizeThresholdToFlushSegment());
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_3 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p1_2);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p1_3, params);
    currentRatio = (double) segmentMetadata_p1_2.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = segmentMetadata_p1_2.getSizeThresholdToFlushSegment() / 2;
    Assert.assertEquals(segmentMetadata_p1_3.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:0 segment:4 commits, time threshold reached
    segmentSizeBytes = 310 * 1024 * 1024;
    startOffset_p0 += 1000;
    updateCommittingSegmentMetadata(segmentMetadata_p0_4, startOffset_p0,
        segmentMetadata_p0_4.getSizeThresholdToFlushSegment() - 200);
    segmentMetadata_p0_4.setCreationTime(System.currentTimeMillis() - TimeUtils.convertPeriodToMillis("8h"));
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_5 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_4);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(segmentMetadata_p0_5, params);
    currentRatio =
        (double) (segmentMetadata_p0_4.getTotalRawDocs()) / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_5.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;
  }

  private int getTargetNumRows(double currentRatio, double prevRatio) {
    return (int) (SegmentSizeBasedFlushThresholdUpdater.IDEAL_SEGMENT_SIZE_BYTES * (
        SegmentSizeBasedFlushThresholdUpdater.CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio
            + SegmentSizeBasedFlushThresholdUpdater.PREVIOUS_SEGMENT_RATIO_WEIGHT * prevRatio));
  }

  private int getTargetNumRows(double prevRatio) {
    return (int) (SegmentSizeBasedFlushThresholdUpdater.IDEAL_SEGMENT_SIZE_BYTES * prevRatio);
  }

  private LLCRealtimeSegmentZKMetadata getNextSegmentMetadata(String realtimeTableName, long startOffset,
      int partitionId, int seqNum) {
    LLCSegmentName newSegmentName =
        new LLCSegmentName(realtimeTableName, partitionId, seqNum, System.currentTimeMillis());
    LLCRealtimeSegmentZKMetadata newSegMetadata = new LLCRealtimeSegmentZKMetadata();
    newSegMetadata.setCreationTime(System.currentTimeMillis());
    newSegMetadata.setStartOffset(startOffset);
    newSegMetadata.setEndOffset(Long.MAX_VALUE);
    newSegMetadata.setNumReplicas(3);
    newSegMetadata.setTableName(realtimeTableName);
    newSegMetadata.setSegmentName(newSegmentName.getSegmentName());
    newSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    return newSegMetadata;
  }

  private void updateCommittingSegmentMetadata(LLCRealtimeSegmentZKMetadata committingSegmentMetadata, long endOffset,
      long numDocs) {
    committingSegmentMetadata.setEndOffset(endOffset);
    committingSegmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    committingSegmentMetadata.setStartTime(System.currentTimeMillis());
    committingSegmentMetadata.setEndTime(System.currentTimeMillis());
    committingSegmentMetadata.setTotalRawDocs(numDocs);
  }

  /**
   * Tests that the flush threshold manager returns the right updater given various scenarios of flush threshold setting in the table config
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void testFlushThresholdUpdater() throws IOException, JSONException {
    FlushThresholdUpdateManager manager = new FlushThresholdUpdateManager();
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME);
    tableConfigBuilder.setTableName("tableName_REALTIME");
    TableConfig realtimeTableConfig;

    FlushThresholdUpdater flushThresholdUpdater;

    // null stream configs - default flush size
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(),
        KafkaHighLevelStreamProviderConfig.getDefaultMaxRealtimeRowsCount());

    // nothing set - default flush size
    Map<String, String> streamConfigs = new HashMap<>();
    tableConfigBuilder.setStreamConfigs(streamConfigs);
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(),
        KafkaHighLevelStreamProviderConfig.getDefaultMaxRealtimeRowsCount());

    // flush size set
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, "10000");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 10000);

    // llc flush size set
    streamConfigs.remove(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE);
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE, "5000");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 5000);

    // invalid string flush size set
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE, "aaa");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(),
        KafkaHighLevelStreamProviderConfig.getDefaultMaxRealtimeRowsCount());

    // negative flush size set
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE, "-10");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(),
        KafkaHighLevelStreamProviderConfig.getDefaultMaxRealtimeRowsCount());

    // 0 flush size set
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE, "0");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), SegmentSizeBasedFlushThresholdUpdater.class);

    // called again with 0 flush size - same object as above
    streamConfigs.remove(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE);
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, "0");
    realtimeTableConfig = tableConfigBuilder.build();
    FlushThresholdUpdater flushThresholdUpdaterSame = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdaterSame.getClass(), SegmentSizeBasedFlushThresholdUpdater.class);
    Assert.assertEquals(flushThresholdUpdater, flushThresholdUpdaterSame);

    // flush size reset to some number - default received, map cleared of segmentsize based
    streamConfigs.put(CommonConstants.Helix.DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, "20000");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 20000);
  }

  @Test
  public void testUpdaterChange() {
    String tableName = "fakeTable_REALTIME";
    int tableFlushSize = 1_000_000;
    int partitionId = 0;
    int seqNum = 0;
    long startOffset = 0;
    long committingSegmentSizeBytes = 0;

    PartitionAssignment partitionAssignment = new PartitionAssignment(tableName);
    // 4 partitions assigned to 4 servers, 4 replicas => the segments should have 250k rows each (1M / 4)
    for (int p = 0; p < 4; p++) {
      List<String> instances = new ArrayList<>();

      for (int replicaId = 0; replicaId < 4; replicaId++) {
        instances.add("Server_1.2.3.4_123" + replicaId);
      }

      partitionAssignment.addPartition(Integer.toString(p), instances);
    }

    // Initially we were using default flush threshold updation - verify that thresholds are as per default strategy
    LLCRealtimeSegmentZKMetadata metadata0 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);

    FlushThresholdUpdater flushThresholdUpdater = new DefaultFlushThresholdUpdater(tableFlushSize);
    FlushThresholdUpdaterParams params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentSizeBytes(committingSegmentSizeBytes);
    params.setCommittingSegmentZkMetadata(null);
    params.setPartitionAssignment(partitionAssignment);
    flushThresholdUpdater.updateFlushThreshold(metadata0, params);

    Assert.assertEquals(metadata0.getSizeThresholdToFlushSegment(), 250_000);
    Assert.assertNull(metadata0.getTimeThresholdToFlushSegment());

    // before committing segment, we switched to size based updation - verify that new thresholds are set as per size based strategy
    flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();

    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata0, startOffset, 225_000);
    committingSegmentSizeBytes = 400 * 1024 * 1024;
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentSizeBytes(committingSegmentSizeBytes);
    params.setCommittingSegmentZkMetadata(metadata0);
    params.setPartitionAssignment(partitionAssignment);
    LLCRealtimeSegmentZKMetadata metadata1 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    flushThresholdUpdater.updateFlushThreshold(metadata1, params);

    double currentRatio = (double) (metadata0.getTotalRawDocs()) / committingSegmentSizeBytes;
    int expectedNumRows = getTargetNumRows(currentRatio);
    Assert.assertEquals(metadata1.getSizeThresholdToFlushSegment(), expectedNumRows);

    // before committing we switched back to default strategy, verify that thresholds are set according to default logic
    flushThresholdUpdater = new DefaultFlushThresholdUpdater(tableFlushSize);

    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata1, startOffset, metadata1.getSizeThresholdToFlushSegment());
    committingSegmentSizeBytes = 420 * 1024 * 1024;
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentSizeBytes(committingSegmentSizeBytes);
    params.setCommittingSegmentZkMetadata(metadata1);
    params.setPartitionAssignment(partitionAssignment);
    LLCRealtimeSegmentZKMetadata metadata2 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    flushThresholdUpdater.updateFlushThreshold(metadata2, params);

    Assert.assertEquals(metadata2.getSizeThresholdToFlushSegment(), 250_000);
    Assert.assertNull(metadata2.getTimeThresholdToFlushSegment());
  }
}
