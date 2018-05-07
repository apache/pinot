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
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FlushThresholdUpdaterTest {
  private Random _random;
  private Map<String, double[][]> datasetGraph;

  @BeforeClass
  public void setup() {
    long seed = new Random().nextLong();
    System.out.println("Random seed for " + FlushThresholdUpdater.class.getSimpleName() + " is " + seed);
    _random = new Random(seed);

    datasetGraph = new HashMap<>(3);
    double[][] exponentialGrowth = {{100000, 50}, {200000, 60}, {300000, 70}, {400000, 83}, {500000, 98}, {600000, 120},
        {700000, 160}, {800000, 200}, {900000, 250}, {1000000, 310}, {1100000, 400}, {1200000, 500}, {1300000, 600},
        {1400000, 700}, {1500000, 800}, {1600000, 950}, {1700000, 1130}, {1800000, 1400}, {1900000, 1700},
        {2000000, 2000}};
    double[][] logarithmicGrowth = {{100000, 70}, {200000, 180}, {300000, 290}, {400000, 400}, {500000, 500},
        {600000, 605}, {700000, 690}, {800000, 770}, {900000, 820}, {1000000, 865}, {1100000, 895}, {1200000, 920},
        {1300000, 940}, {1400000, 955}, {1500000, 970}, {1600000, 980}, {1700000, 1000}, {1800000, 1012},
        {1900000, 1020}, {2000000, 1030}};
    double[][] steps = {{100000, 100}, {200000, 100}, {300000, 200}, {400000, 200}, {500000, 300}, {600000, 300},
        {700000, 400}, {800000, 400}, {900000, 500}, {1000000, 500}, {1100000, 600}, {1200000, 600}, {1300000, 700},
        {1400000, 700}, {1500000, 800}, {1600000, 800}, {1700000, 900}, {1800000, 900}, {1900000, 1000},
        {20000000, 1000}};
    datasetGraph.put("exponentialGrowth", exponentialGrowth);
    datasetGraph.put("logarithmicGrowth", logarithmicGrowth);
    datasetGraph.put("steps", steps);
  }

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

  /**
   * Tests the segment size based flush threshold updater. A series of 500 runs is started.
   * We have 3 types of datasets, each having a different segment size to num rows ratio (exponential growth, logarithmic growth, steps)
   * We let 500 segments pass through our algorithm, each time feeding a segment size based on the graph.
   * Towards the end, we begin to see that the segment size and number of rows begins to stabilize around the 500M mark
   */
  @Test
  public void testSegmentSizeBasedFlushThreshold() {
    String tableName = "aRealtimeTable_REALTIME";

    for (Map.Entry<String, double[][]> entry : datasetGraph.entrySet()) {

      SegmentSizeBasedFlushThresholdUpdater segmentSizeBasedFlushThresholdUpdater =
          new SegmentSizeBasedFlushThresholdUpdater();

      double[][] numRowsToSegmentSize = entry.getValue();

      int numRuns = 500;
      double checkRunsAfter = 400;
      long idealSegmentSize = segmentSizeBasedFlushThresholdUpdater.getIdealSegmentSizeBytes();
      double segmentSizeSwivel = idealSegmentSize * 0.3;
      int numRowsLowerLimit = 0;
      int numRowsUpperLimit = 0;
      for (int i = 0; i < numRowsToSegmentSize.length; i++) {
        if (numRowsToSegmentSize[i][1] * 1024 * 1024 >= idealSegmentSize) {
          numRowsLowerLimit = (int) numRowsToSegmentSize[i - 2][0];
          numRowsUpperLimit = (int) numRowsToSegmentSize[i + 3][0];
          break;
        }
      }
      long startOffset = 0;
      int seqNum = 0;
      int partitionId = 0;
      long segmentSizeBytes;
      FlushThresholdUpdaterParams params;
      LLCRealtimeSegmentZKMetadata committingSegmentMetadata;
      LLCRealtimeSegmentZKMetadata newSegmentMetadata;

      committingSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
      params = new FlushThresholdUpdaterParams();
      segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(committingSegmentMetadata, params);
      Assert.assertEquals(committingSegmentMetadata.getSizeThresholdToFlushSegment(),
          segmentSizeBasedFlushThresholdUpdater.getInitialRowsThreshold());

      for (int run = 0; run < numRuns; run++) {

        // get a segment size from the graph
        segmentSizeBytes =
            getSegmentSize(committingSegmentMetadata.getSizeThresholdToFlushSegment(), numRowsToSegmentSize);

        startOffset += 1000; // if stopped on time, increment less than 1000
        updateCommittingSegmentMetadata(committingSegmentMetadata, startOffset,
            committingSegmentMetadata.getSizeThresholdToFlushSegment());
        newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
        params = new FlushThresholdUpdaterParams();
        params.setCommittingSegmentZkMetadata(committingSegmentMetadata);
        params.setCommittingSegmentSizeBytes(segmentSizeBytes);
        segmentSizeBasedFlushThresholdUpdater.updateFlushThreshold(newSegmentMetadata, params);

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          Assert.assertTrue(
              segmentSizeBytes > ((idealSegmentSize - segmentSizeSwivel)) && segmentSizeBytes < (idealSegmentSize
                  + segmentSizeSwivel), "Segment size check failed for dataset " + entry.getKey());
          Assert.assertTrue(committingSegmentMetadata.getSizeThresholdToFlushSegment() > numRowsLowerLimit
                  && committingSegmentMetadata.getSizeThresholdToFlushSegment() < numRowsUpperLimit,
              "Num rows check failed for dataset " + entry.getKey());
        }

        committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(newSegmentMetadata.toZNRecord());
      }
    }
  }

  long getSegmentSize(int numRowsConsumed, double[][] numRowsToSegmentSize) {
    double segmentSize = 0;
    if (numRowsConsumed < numRowsToSegmentSize[0][0]) {
      segmentSize = numRowsConsumed / numRowsToSegmentSize[0][0] * numRowsToSegmentSize[0][1];
    } else if (numRowsConsumed >= numRowsToSegmentSize[numRowsToSegmentSize.length - 1][0]) {
      segmentSize = numRowsConsumed / numRowsToSegmentSize[numRowsToSegmentSize.length - 1][0] * numRowsToSegmentSize[
          numRowsToSegmentSize.length - 1][1];
    } else {
      for (int i = 1; i < numRowsToSegmentSize.length; i++) {
        if (numRowsConsumed < numRowsToSegmentSize[i][0]) {
          segmentSize = _random.nextDouble() * (numRowsToSegmentSize[i][1] - numRowsToSegmentSize[i - 1][1])
              + numRowsToSegmentSize[i - 1][1];
          break;
        }
      }
    }
    return (long) (segmentSize * 1024 * 1024);
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

  /**
   * Tests change of config which enables SegmentSize based flush threshold updater, and tests the resetting of it back to default
   */
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
    Assert.assertTrue(
        metadata1.getSizeThresholdToFlushSegment() != 0 && metadata1.getSizeThresholdToFlushSegment() != 250_000);

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
