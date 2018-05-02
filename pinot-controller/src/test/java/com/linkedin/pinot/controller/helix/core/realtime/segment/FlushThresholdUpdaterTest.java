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

import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.ArrayList;
import java.util.List;
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

    //Scenarios:
    // 1. starts at lesser than min
    SegmentSizeBasedFlushThresholdUpdater updater = new SegmentSizeBasedFlushThresholdUpdater();

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
    updater.updateFlushThreshold(segmentMetadata_p0_0, params);
    Assert.assertEquals(segmentMetadata_p0_0.getSizeThresholdToFlushSegment(),
        SegmentSizeBasedFlushThresholdUpdater.INITIAL_ROWS_THRESHOLD);
    Assert.assertEquals(segmentMetadata_p0_0.getTimeThresholdToFlushSegment(),
        SegmentSizeBasedFlushThresholdUpdater.MAX_TIME_THRESHOLD);

    // partition:0 segment:0 commits with 200M size
    segmentSizeBytes = 200 * 1024 * 1024;
    startOffset_p0 += segmentMetadata_p0_0.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p0_0, startOffset_p0);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_1 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_0);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p0_1, params);
    currentRatio = (double) segmentMetadata_p0_0.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = segmentMetadata_p0_0.getSizeThresholdToFlushSegment() * 2;
    Assert.assertEquals(segmentMetadata_p0_1.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:0 segment:1 commits with 420M size
    segmentSizeBytes = 420 * 1024 * 1024;
    startOffset_p0 += segmentMetadata_p0_1.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p0_1, startOffset_p0);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_2 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_1);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p0_2, params);
    currentRatio = (double) segmentMetadata_p0_1.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_2.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:0 segment:2 commits with 485M size
    segmentSizeBytes = 485 * 1024 * 1024;
    startOffset_p0 += segmentMetadata_p0_2.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p0_2, startOffset_p0);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_3 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_2);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p0_3, params);
    currentRatio = (double) segmentMetadata_p0_2.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_3.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:1 segment:0
    long startOffset_p1 = 0;
    int seqNum_p1 = 0;
    int partitionId_1 = 1;
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_0 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    updater.updateFlushThreshold(segmentMetadata_p1_0, params);
    expectedNumRows = getTargetNumRows(prevRatio);
    Assert.assertEquals(segmentMetadata_p1_0.getSizeThresholdToFlushSegment(), expectedNumRows);

    // partition:0 segment:3 commits with 520M size
    segmentSizeBytes = 520 * 1024 * 1024;
    startOffset_p0 += segmentMetadata_p0_3.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p0_3, startOffset_p0);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_4 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_3);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p0_4, params);
    currentRatio = (double) segmentMetadata_p0_3.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p0_4.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:1 segment:0 in error state. repair case. committing segment size=0.
    segmentSizeBytes = 0;
    startOffset_p1 += segmentMetadata_p1_0.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p1_0, startOffset_p1);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_1 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p1_0);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p1_1, params);
    expectedNumRows = segmentMetadata_p1_0.getSizeThresholdToFlushSegment();
    Assert.assertEquals(segmentMetadata_p1_1.getSizeThresholdToFlushSegment(), expectedNumRows);

    // partition:1 segment:1 commits with 490M size
    segmentSizeBytes = 490 * 1024 * 1024;
    startOffset_p1 += segmentMetadata_p1_1.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p1_1, startOffset_p1);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_2 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p1_1);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p1_2, params);
    currentRatio = (double) segmentMetadata_p1_1.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = getTargetNumRows(currentRatio, prevRatio);
    Assert.assertEquals(segmentMetadata_p1_2.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:1 segment:2 commits with 790M size
    segmentSizeBytes = 790 * 1024 * 1024;
    startOffset_p1 += segmentMetadata_p1_2.getSizeThresholdToFlushSegment();
    updateCommittingSegmentMetadata(segmentMetadata_p1_2, startOffset_p1);
    LLCRealtimeSegmentZKMetadata segmentMetadata_p1_3 =
        getNextSegmentMetadata(tableName, startOffset_p1, partitionId_1, seqNum_p1++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p1_2);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p1_3, params);
    currentRatio = (double) segmentMetadata_p1_2.getSizeThresholdToFlushSegment() / segmentSizeBytes;
    expectedNumRows = segmentMetadata_p1_2.getSizeThresholdToFlushSegment() / 2;
    Assert.assertEquals(segmentMetadata_p1_3.getSizeThresholdToFlushSegment(), expectedNumRows);
    prevRatio = currentRatio;

    // partition:0 segment:4 commits, time threshold reached
    segmentSizeBytes = 310 * 1024 * 1024;
    startOffset_p0 += segmentMetadata_p0_4.getSizeThresholdToFlushSegment() - 2000;
    updateCommittingSegmentMetadata(segmentMetadata_p0_4, startOffset_p0);
    segmentMetadata_p0_4.setStartTime(segmentMetadata_p0_4.getEndTime() - TimeUtils.convertPeriodToMillis(
        SegmentSizeBasedFlushThresholdUpdater.MAX_TIME_THRESHOLD));
    LLCRealtimeSegmentZKMetadata segmentMetadata_p0_5 =
        getNextSegmentMetadata(tableName, startOffset_p0, partitionId_0, seqNum_p0++);
    params = new FlushThresholdUpdaterParams();
    params.setCommittingSegmentZkMetadata(segmentMetadata_p0_4);
    params.setCommittingSegmentSizeBytes(segmentSizeBytes);
    updater.updateFlushThreshold(segmentMetadata_p0_5, params);
    currentRatio = (double) (segmentMetadata_p0_4.getEndOffset() - segmentMetadata_p0_4.getStartOffset()) / segmentSizeBytes;
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

  private void updateCommittingSegmentMetadata(LLCRealtimeSegmentZKMetadata committingSegmentMetadata, long endOffset) {
    committingSegmentMetadata.setEndOffset(endOffset);
    committingSegmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    committingSegmentMetadata.setStartTime(System.currentTimeMillis());
    committingSegmentMetadata.setEndTime(System.currentTimeMillis());
  }
}
