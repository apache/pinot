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
package org.apache.pinot.controller.helix.core.realtime.segment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.partition.PartitionAssignment;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.realtime.stream.PartitionLevelStreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/*
 * TODO: FIXME
 * - Consider changing the test to use mock streamConfig than the one generated via real configurations.
 *   Makes things a lot easier. Or, have a class that inherits from StreamConfig and has setters/getters
 *   to return the values that we care about.
 * - Change the SegmentSizeBasedFlushThresholdUpdater class to use a method to get time, and fake it
 *   in this class to set time to arbitrary value. Calling System.currentTime() in the class under
 *   test can cause this test to be flaky. We need to use the current time to generate segment start
 *   times, and if for any reason threads get slower, the test will fail.
 * - Some tests in testFlushThresholdUpdater() seem to test StreamConfig more than the threshold updater.
 *   The lines are commented out so that we don't lose the tests, but we need to move those lines
 *   to a different class that does not use use flush threshold updater.
 */
public class FlushThresholdUpdaterTest {
  private static final long DESIRED_SEGMENT_SIZE = StreamConfig.getDefaultDesiredSegmentSizeBytes();
  private static final int DEFAULT_INITIAL_ROWS_THRESHOLD = StreamConfig.getDefaultFlushAutotuneInitialRows();
  private Random _random;
  private Map<String, double[][]> datasetGraph;

  @BeforeClass
  public void setup() {
    long seed = new Random().nextLong();
    System.out.println("Random seed for " + FlushThresholdUpdater.class.getSimpleName() + " is " + seed);
    _random = new Random(seed);

    datasetGraph = new HashMap<>(3);
    double[][] exponentialGrowth =
        {{100000, 50}, {200000, 60}, {300000, 70}, {400000, 83}, {500000, 98}, {600000, 120}, {700000, 160}, {800000, 200}, {900000, 250}, {1000000, 310}, {1100000, 400}, {1200000, 500}, {1300000, 600}, {1400000, 700}, {1500000, 800}, {1600000, 950}, {1700000, 1130}, {1800000, 1400}, {1900000, 1700}, {2000000, 2000}};
    double[][] logarithmicGrowth =
        {{100000, 70}, {200000, 180}, {300000, 290}, {400000, 400}, {500000, 500}, {600000, 605}, {700000, 690}, {800000, 770}, {900000, 820}, {1000000, 865}, {1100000, 895}, {1200000, 920}, {1300000, 940}, {1400000, 955}, {1500000, 970}, {1600000, 980}, {1700000, 1000}, {1800000, 1012}, {1900000, 1020}, {2000000, 1030}};
    double[][] steps =
        {{100000, 100}, {200000, 100}, {300000, 200}, {400000, 200}, {500000, 300}, {600000, 300}, {700000, 400}, {800000, 400}, {900000, 500}, {1000000, 500}, {1100000, 600}, {1200000, 600}, {1300000, 700}, {1400000, 700}, {1500000, 800}, {1600000, 800}, {1700000, 900}, {1800000, 900}, {1900000, 1000}, {20000000, 1000}};
    datasetGraph.put("exponentialGrowth", exponentialGrowth);
    datasetGraph.put("logarithmicGrowth", logarithmicGrowth);
    datasetGraph.put("steps", steps);
  }

  StreamConfig convertToAutoTune(StreamConfig streamConfig) {
    Map<String, String> streamConfigsMap = streamConfig.getStreamConfigsMap();
    streamConfigsMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
    return new StreamConfig(streamConfig.getTableNameWithType(), streamConfigsMap);
  }

  StreamConfig updateDesiredSegmentSize(StreamConfig streamConfig, long desiredSegmentSize) {
    Map<String, String> streamConfigsMap = streamConfig.getStreamConfigsMap();
    streamConfigsMap.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, String.valueOf(desiredSegmentSize));
    return new StreamConfig(streamConfig.getTableNameWithType(), streamConfigsMap);
  }

  StreamConfig updateTimeThreshold(StreamConfig streamConfig, long timeThresholdMillis) {
    Map<String, String> streamConfigsMap = streamConfig.getStreamConfigsMap();
    streamConfigsMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, String.valueOf(timeThresholdMillis));
    return new StreamConfig(streamConfig.getTableNameWithType(), streamConfigsMap);
  }

  StreamConfig updateInitialRowsForAutoTune(StreamConfig streamConfig, long initialRowsForAutoTune) {
    Map<String, String> streamConfigsMap = streamConfig.getStreamConfigsMap();
    streamConfigsMap.put(StreamConfigProperties.SEGMENT_FLUSH_AUTOTUNE_INITIAL_ROWS, String.valueOf(initialRowsForAutoTune));
    return new StreamConfig(streamConfig.getTableNameWithType(), streamConfigsMap);
  }

  StreamConfig updateRowThreshold(StreamConfig streamConfig, int rowThreshold) {
    Map<String, String> streamConfigsMap = streamConfig.getStreamConfigsMap();
    streamConfigsMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, String.valueOf(rowThreshold));
    return new StreamConfig(streamConfig.getTableNameWithType(), streamConfigsMap);
  }
  /**
   * Tests that we have the right flush threshold set in the segment metadata given the various combinations of servers, partitions and replicas
   */
  @Test
  public void testDefaultUpdateFlushThreshold() {

    final StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
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
    // Check that each segment has 250k rows each
    for (int segmentId = 1; segmentId <= 4; ++segmentId) {
      LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
      metadata.setSegmentName(makeFakeSegmentName(segmentId));
      flushThresholdUpdater.updateFlushThreshold(metadata, streamConfig, null, null, partitionAssignment);
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
      flushThresholdUpdater.updateFlushThreshold(metadata, streamConfig, null, null, partitionAssignment);
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
      flushThresholdUpdater.updateFlushThreshold(metadata, streamConfig, null, null, partitionAssignment);
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
      flushThresholdUpdater.updateFlushThreshold(metadata, streamConfig, null, null, partitionAssignment);
      Assert.assertEquals(metadata.getSizeThresholdToFlushSegment(), 500000);
    }
  }

  private String makeFakeSegmentName(int id) {
    return new LLCSegmentName("fakeTable_REALTIME", id, 0, 1234L).getSegmentName();
  }

  @Test
  public void testSegmentSizeBasedUpdaterWithModifications() {
    final String tableName = "xyz_REALTIME";
    final int initialDefaultRows = 100_000;
    final int partitionId = 0;
    final long startOffset = 0L;

    int segmentSeqNum = 17;
    final long now = System.currentTimeMillis();

    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    streamConfig = convertToAutoTune(streamConfig);

    int consumedRows;
    long consumptionDuration;
    int nextThreshold;
    LLCRealtimeSegmentZKMetadata committingSegmentMetadata;
    LLCRealtimeSegmentZKMetadata newSegmentMetadata;
    long committingSegmentSize;

    // Create an updater
    SegmentSizeBasedFlushThresholdUpdater updater = new SegmentSizeBasedFlushThresholdUpdater();

    CommittingSegmentDescriptor committingSegmentDescriptor;

    // Assume that we were asked to consume 5M rows, and we did so in 90% of the time, producing a 180M segment
    consumedRows = 5_000_000;
    consumptionDuration = streamConfig.getFlushThresholdTimeMillis() * 90/100;
    committingSegmentSize = 180_000_000;

    committingSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum, now - consumptionDuration);
    committingSegmentMetadata.setSizeThresholdToFlushSegment(consumedRows);
    committingSegmentMetadata.setTotalRawDocs(consumedRows);
    committingSegmentDescriptor = new CommittingSegmentDescriptor(committingSegmentMetadata.getSegmentName(), startOffset,
        committingSegmentSize);

    newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum +1, now);
    updater.updateFlushThreshold(newSegmentMetadata, streamConfig, committingSegmentMetadata, committingSegmentDescriptor, null);
    nextThreshold = newSegmentMetadata.getSizeThresholdToFlushSegment();

    // Since we hit the row threshold, and have not reached desired segment size, we should see higher number of rows.
    Assert.assertTrue(nextThreshold > consumedRows);
    // We should have set the ratio now.
    double ratio = updater.getLatestSegmentRowsToSizeRatio();

    // Now we reach the time threshold for the next segment, consuming 50% of the number we were asked to do, and
    // generate half the size of the segment we last did.
    consumedRows = nextThreshold * 5/10;
    consumptionDuration = streamConfig.getFlushThresholdTimeMillis() * 9/10;  // We cannot mock time in the class!
    committingSegmentSize /= 2;

    committingSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum, now - consumptionDuration);
    committingSegmentMetadata.setSizeThresholdToFlushSegment(nextThreshold);
    committingSegmentMetadata.setTotalRawDocs(consumedRows);
    committingSegmentDescriptor = new CommittingSegmentDescriptor( committingSegmentMetadata.getSegmentName(), startOffset,
        committingSegmentSize);

    newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum +1, now);
    updater.updateFlushThreshold(newSegmentMetadata, streamConfig, committingSegmentMetadata, committingSegmentDescriptor, null);
    nextThreshold = newSegmentMetadata.getSizeThresholdToFlushSegment();

    // The new threshold should be slightly above the number of rows we consumed
    Assert.assertEquals(nextThreshold, (int) (consumedRows * updater.getRowsMultiplierWhenTimeThresholdHit()));

    // Now let us assume that the admin has reduced the segment size threshold to be lower than the size of segment we produced last time,
    // and we hit the time threshold again, but we made a bigger segment than the new desired segment size. We should reduce the
    // number of rows to consume next time.
    streamConfig = updateDesiredSegmentSize(streamConfig, committingSegmentSize * 9/10);
    consumedRows = nextThreshold * 9/10;

    committingSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum, now - consumptionDuration);
    committingSegmentMetadata.setSizeThresholdToFlushSegment(nextThreshold);
    committingSegmentMetadata.setTotalRawDocs(consumedRows);
    committingSegmentDescriptor = new CommittingSegmentDescriptor( committingSegmentMetadata.getSegmentName(), startOffset,
        committingSegmentSize);

    newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum +1, now);
    updater.updateFlushThreshold(newSegmentMetadata, streamConfig, committingSegmentMetadata, committingSegmentDescriptor, null);
    nextThreshold = newSegmentMetadata.getSizeThresholdToFlushSegment();

    Assert.assertTrue(nextThreshold < consumedRows);

    // Now the admin adjusts the time threshold to be lower. We reach the time limit on the current segment, consuming at a much
    // smaller rate of consumption
    streamConfig = updateTimeThreshold(streamConfig, streamConfig.getFlushThresholdTimeMillis() * 6/10);
    consumedRows = nextThreshold * 5/10;

    committingSegmentSize = streamConfig.getFlushSegmentDesiredSizeBytes() * 9/10;
    committingSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum, now - consumptionDuration);
    committingSegmentMetadata.setSizeThresholdToFlushSegment(nextThreshold);
    committingSegmentMetadata.setTotalRawDocs(consumedRows);
    committingSegmentDescriptor = new CommittingSegmentDescriptor( committingSegmentMetadata.getSegmentName(), startOffset,
        committingSegmentSize);

    newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, segmentSeqNum +1, now);
    updater.updateFlushThreshold(newSegmentMetadata, streamConfig, committingSegmentMetadata, committingSegmentDescriptor, null);
    nextThreshold = newSegmentMetadata.getSizeThresholdToFlushSegment();

    // Once again, the next threshold should reduce to about half of the threshold we set for the previous segment
    // (since we consumed half the number of rows in 60% of the time allotted to us)
    Assert.assertTrue(nextThreshold < consumedRows, "nextThreshold=" + nextThreshold +",consumedRows=" + consumedRows);
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
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    streamConfig = convertToAutoTune(streamConfig);

    for (Map.Entry<String, double[][]> entry : datasetGraph.entrySet()) {

      SegmentSizeBasedFlushThresholdUpdater segmentSizeBasedFlushThresholdUpdater =
          new SegmentSizeBasedFlushThresholdUpdater();

      double[][] numRowsToSegmentSize = entry.getValue();

      int numRuns = 500;
      double checkRunsAfter = 400;
      long idealSegmentSize = DESIRED_SEGMENT_SIZE;
      long segmentSizeSwivel = (long) (idealSegmentSize * 0.5);
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
      long segmentSizeBytes = 0;
      CommittingSegmentDescriptor committingSegmentDescriptor;
      LLCRealtimeSegmentZKMetadata committingSegmentMetadata;
      LLCRealtimeSegmentZKMetadata newSegmentMetadata;

      newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
      committingSegmentDescriptor = new CommittingSegmentDescriptor(null, startOffset, segmentSizeBytes);
      segmentSizeBasedFlushThresholdUpdater
          .updateFlushThreshold(newSegmentMetadata, streamConfig, null, committingSegmentDescriptor, null);
      Assert.assertEquals(newSegmentMetadata.getSizeThresholdToFlushSegment(),
          DEFAULT_INITIAL_ROWS_THRESHOLD);

      System.out.println("NumRowsThreshold, SegmentSize");
      for (int run = 0; run < numRuns; run++) {
        committingSegmentMetadata = new LLCRealtimeSegmentZKMetadata(newSegmentMetadata.toZNRecord());

        // get a segment size from the graph
        segmentSizeBytes =
            getSegmentSize(committingSegmentMetadata.getSizeThresholdToFlushSegment(), numRowsToSegmentSize);

        startOffset += 1000; // if stopped on time, increment less than 1000
        updateCommittingSegmentMetadata(committingSegmentMetadata, startOffset,
            committingSegmentMetadata.getSizeThresholdToFlushSegment());
        newSegmentMetadata = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
        committingSegmentDescriptor =
            new CommittingSegmentDescriptor(committingSegmentMetadata.getSegmentName(), startOffset, segmentSizeBytes);
        segmentSizeBasedFlushThresholdUpdater
            .updateFlushThreshold(newSegmentMetadata, streamConfig,
                committingSegmentMetadata, committingSegmentDescriptor, null);

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          Assert.assertTrue(segmentSizeBytes < (idealSegmentSize + segmentSizeSwivel),
              "Segment size check failed for dataset " + entry.getKey());
          Assert.assertTrue(committingSegmentMetadata.getSizeThresholdToFlushSegment() > numRowsLowerLimit
                  && committingSegmentMetadata.getSizeThresholdToFlushSegment() < numRowsUpperLimit,
              "Num rows check failed for dataset " + entry.getKey());
        }
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
      int partitionId, int seqNum, long creationTime) {

    LLCSegmentName newSegmentName = new LLCSegmentName(realtimeTableName, partitionId, seqNum, creationTime);
    LLCRealtimeSegmentZKMetadata newSegMetadata = new LLCRealtimeSegmentZKMetadata();
    newSegMetadata.setCreationTime(creationTime);
    newSegMetadata.setStartOffset(startOffset);
    newSegMetadata.setEndOffset(Long.MAX_VALUE);
    newSegMetadata.setNumReplicas(3);
    newSegMetadata.setSegmentName(newSegmentName.getSegmentName());
    newSegMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    return newSegMetadata;
  }

  private LLCRealtimeSegmentZKMetadata getNextSegmentMetadata(String realtimeTableName, long startOffset,
      int partitionId, int seqNum) {
    return getNextSegmentMetadata(realtimeTableName, startOffset, partitionId, seqNum, System.currentTimeMillis());
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
   */
  @Test
  public void testFlushThresholdUpdater() {
    FlushThresholdUpdateManager manager = new FlushThresholdUpdateManager();
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME);
    tableConfigBuilder.setTableName("tableName_REALTIME");
    TableConfig realtimeTableConfig;

    FlushThresholdUpdater flushThresholdUpdater;
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();

    // flush size set
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "10000");
    streamConfig = updateRowThreshold(streamConfig, 10000);
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 10000);

    // llc flush size set
//    streamConfigs.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX, "5000");
    streamConfig = updateRowThreshold(streamConfig, 5000);
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 5000);

    // 0 flush size set
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX, "0");
    streamConfig = convertToAutoTune(streamConfig);
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), SegmentSizeBasedFlushThresholdUpdater.class);

    // called again with 0 flush size - same object as above
    streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    streamConfig = convertToAutoTune(streamConfig);
    realtimeTableConfig = tableConfigBuilder.build();
    FlushThresholdUpdater flushThresholdUpdaterSame = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdaterSame.getClass(), SegmentSizeBasedFlushThresholdUpdater.class);
    Assert.assertEquals(flushThresholdUpdater, flushThresholdUpdaterSame);
//    Assert.assertEquals(((SegmentSizeBasedFlushThresholdUpdater) (flushThresholdUpdater)).getDesiredSegmentSizeBytes(),
//        StreamConfig.getDefaultDesiredSegmentSizeBytes());

    // flush size reset to some number - default received, map cleared of segmentsize based
    streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    streamConfig = updateRowThreshold(streamConfig, 20000);
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "20000");
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);
    Assert.assertEquals(((DefaultFlushThresholdUpdater) flushThresholdUpdater).getTableFlushSize(), 20000);

    // optimal segment size set to invalid value. Default remains the same.
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    tableConfigBuilder.setStreamConfigs(streamConfigs);
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, "Invalid");
    realtimeTableConfig = tableConfigBuilder.build();
    streamConfig =
        new PartitionLevelStreamConfig(realtimeTableConfig.getTableName(), realtimeTableConfig.getIndexingConfig().getStreamConfigs());
    flushThresholdUpdater = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), SegmentSizeBasedFlushThresholdUpdater.class);
//    Assert.assertEquals(((SegmentSizeBasedFlushThresholdUpdater) (flushThresholdUpdater)).getDesiredSegmentSizeBytes(),
//        StreamConfig.getDefaultDesiredSegmentSizeBytes());

    // Clear the flush threshold updater for this table.
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "20000");
    realtimeTableConfig = tableConfigBuilder.build();
    streamConfig =
        new PartitionLevelStreamConfig(realtimeTableConfig.getTableName(), realtimeTableConfig.getIndexingConfig().getStreamConfigs());
    realtimeTableConfig = tableConfigBuilder.build();
    flushThresholdUpdater = manager.getFlushThresholdUpdater(streamConfig);
    Assert.assertEquals(flushThresholdUpdater.getClass(), DefaultFlushThresholdUpdater.class);

    // optimal segment size set to 500M
//    long desiredSegSize = 500 * 1024 * 1024;
//    streamConfig = convertToAutoTune(streamConfig);
//    streamConfig = updateDesiredSegmentSize(streamConfig, desiredSegSize);
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "0");
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, Long.toString(desiredSegSize));
    realtimeTableConfig = tableConfigBuilder.build();
//    flushThresholdUpdater = manager.getFlushThresholdUpdater(realtimeTableConfig, null);
//    Assert.assertEquals(((SegmentSizeBasedFlushThresholdUpdater) (flushThresholdUpdater)).getDesiredSegmentSizeBytes(),
//        desiredSegSize);
//    Assert.assertEquals(((SegmentSizeBasedFlushThresholdUpdater) (flushThresholdUpdater)).getAutotuneInitialRows(),
//        DEFAULT_INITIAL_ROWS_THRESHOLD);

    // initial rows threshold
//    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_AUTOTUNE_INITIAL_ROWS, "500000");
//    realtimeTableConfig = tableConfigBuilder.build();
//    FlushThresholdUpdateManager newManager = new FlushThresholdUpdateManager();
//    flushThresholdUpdater = newManager.getFlushThresholdUpdater(realtimeTableConfig, null);
//    Assert.assertEquals(((SegmentSizeBasedFlushThresholdUpdater) (flushThresholdUpdater)).getAutotuneInitialRows(),
//        500_000);
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
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs(4);

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
    flushThresholdUpdater.updateFlushThreshold(metadata0, streamConfig, null, null, partitionAssignment);

    Assert.assertEquals(metadata0.getSizeThresholdToFlushSegment(), 250_000);
    Assert.assertNull(metadata0.getTimeThresholdToFlushSegment());

    // before committing segment, we switched to size based updation - verify that new thresholds are set as per size based strategy
    streamConfig = convertToAutoTune(streamConfig);
    flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();

    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata0, startOffset, 250_000);
    committingSegmentSizeBytes = 180 * 1024 * 1024;
    CommittingSegmentDescriptor committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata1 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    flushThresholdUpdater.updateFlushThreshold(metadata1, streamConfig, metadata0, committingSegmentDescriptor, partitionAssignment);
    Assert.assertTrue(
        metadata1.getSizeThresholdToFlushSegment() != 0 && metadata1.getSizeThresholdToFlushSegment() != 250_000);

    // before committing we switched back to default strategy, verify that thresholds are set according to default logic
    streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs(4);
    flushThresholdUpdater = new DefaultFlushThresholdUpdater(tableFlushSize);

    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata1, startOffset, metadata1.getSizeThresholdToFlushSegment());
    committingSegmentSizeBytes = 190 * 1024 * 1024;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata1.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata2 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    flushThresholdUpdater.updateFlushThreshold(metadata2, streamConfig, metadata1, committingSegmentDescriptor, partitionAssignment);

    Assert.assertEquals(metadata2.getSizeThresholdToFlushSegment(), 250_000);
    Assert.assertNull(metadata2.getTimeThresholdToFlushSegment());
  }

  @Test
  public void testTimeThresholdInSegmentSizeBased() {
    int partitionId = 0;
    int seqNum = 0;
    long startOffset = 0;
    long committingSegmentSizeBytes;
    CommittingSegmentDescriptor committingSegmentDescriptor;
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    streamConfig = convertToAutoTune(streamConfig);
    final String tableName = streamConfig.getTableNameWithType();


    // initial segment
    LLCRealtimeSegmentZKMetadata metadata0 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater();
    committingSegmentDescriptor = new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, 0);
    flushThresholdUpdater.updateFlushThreshold(metadata0, streamConfig, null, committingSegmentDescriptor, null);
    Assert.assertEquals(metadata0.getSizeThresholdToFlushSegment(), DEFAULT_INITIAL_ROWS_THRESHOLD);

    // next segment hit time threshold
    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata0, startOffset, 98372);
    committingSegmentSizeBytes = 180 * 1024 * 1024;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata0.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata1 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    flushThresholdUpdater.updateFlushThreshold(metadata1, streamConfig, metadata0, committingSegmentDescriptor, null);
    Assert.assertEquals(metadata1.getSizeThresholdToFlushSegment(),
        (int) (metadata0.getTotalRawDocs() * flushThresholdUpdater.getRowsMultiplierWhenTimeThresholdHit()));

    // now we hit rows threshold
    startOffset += 1000;
    updateCommittingSegmentMetadata(metadata1, startOffset, metadata1.getSizeThresholdToFlushSegment());
    committingSegmentSizeBytes = 240 * 1024 * 1024;
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(metadata1.getSegmentName(), startOffset, committingSegmentSizeBytes);
    LLCRealtimeSegmentZKMetadata metadata2 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    flushThresholdUpdater.updateFlushThreshold(metadata2, streamConfig, metadata1, committingSegmentDescriptor, null);
    Assert.assertTrue(metadata2.getSizeThresholdToFlushSegment() != metadata1.getSizeThresholdToFlushSegment());
  }

  @Test
  public void testMinThreshold() {
    final int partitionId = 0;
    int seqNum = 0;
    long startOffset = 0;
    long committingSegmentSizeBytes;
    CommittingSegmentDescriptor committingSegmentDescriptor;
    long now = System.currentTimeMillis();
    long seg0time = now - 1334_650;
    long seg1time = seg0time + 14_000;

    final StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    final String tableName = streamConfig.getTableNameWithType();

    // initial segment consumes only 15 rows, so next segment has 10k rows min.
    LLCSegmentName seg0SegmentName = new LLCSegmentName(tableName, partitionId, seqNum, seg0time);
    LLCRealtimeSegmentZKMetadata metadata0 = getNextSegmentMetadata(tableName, startOffset, partitionId, seqNum++);
    metadata0.setSegmentName(seg0SegmentName.getSegmentName());
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater();
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(seg0SegmentName.getSegmentName(), startOffset, 10_000);
    metadata0.setTotalRawDocs(15);
    metadata0.setCreationTime(seg0time);
    metadata0.setSizeThresholdToFlushSegment(874_990);
    LLCSegmentName seg1SegmentName = new LLCSegmentName(tableName, partitionId, seqNum + 1, seg1time);
    LLCRealtimeSegmentZKMetadata metadata1 = new LLCRealtimeSegmentZKMetadata();
    metadata1.setSegmentName(seg1SegmentName.getSegmentName());
    metadata1.setCreationTime(seg1time);
    flushThresholdUpdater.updateFlushThreshold(metadata1, streamConfig, metadata0, committingSegmentDescriptor, null);
    Assert.assertEquals(metadata1.getSizeThresholdToFlushSegment(), flushThresholdUpdater.getMinimumNumRowsThreshold());

    // seg1 also consumes 20 rows, so seg2 also gets 10k as threshold.
    LLCSegmentName seg2SegmentName = new LLCSegmentName(tableName, partitionId, seqNum + 2, now);
    LLCRealtimeSegmentZKMetadata metadata2 = new LLCRealtimeSegmentZKMetadata();
    metadata2.setSegmentName(seg2SegmentName.getSegmentName());
    metadata2.setStartTime(now);
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(seg1SegmentName.getSegmentName(), startOffset + 1000, 14_000);
    metadata1.setTotalRawDocs(25);
    flushThresholdUpdater.updateFlushThreshold(metadata2, streamConfig, metadata1, committingSegmentDescriptor, null);
    Assert.assertEquals(metadata2.getSizeThresholdToFlushSegment(), flushThresholdUpdater.getMinimumNumRowsThreshold());
  }

  @Test
  public void testNonZeroPartitionUpdates() {
    int seqNum = 0;
    long startOffset = 0;
    CommittingSegmentDescriptor committingSegmentDescriptor;
    long now = System.currentTimeMillis();
    long seg0time = now - 1334_650;
    long seg1time = seg0time + 14_000;
    final StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    final String tableName = streamConfig.getTableNameWithType();

    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater =
        new SegmentSizeBasedFlushThresholdUpdater();

    // Initial update is from partition 1
    LLCSegmentName seg0SegmentName = new LLCSegmentName(tableName, 1, seqNum, seg0time);
    LLCRealtimeSegmentZKMetadata metadata0 = getNextSegmentMetadata(tableName, startOffset, 1, seqNum++);
    metadata0.setSegmentName(seg0SegmentName.getSegmentName());
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(seg0SegmentName.getSegmentName(), startOffset, 3_110_000);
    metadata0.setTotalRawDocs(1_234_000);
    metadata0.setCreationTime(seg0time);
    metadata0.setSizeThresholdToFlushSegment(874_990);
    LLCSegmentName seg1SegmentName = new LLCSegmentName(tableName, 1, seqNum + 1, seg1time);
    LLCRealtimeSegmentZKMetadata metadata1 = new LLCRealtimeSegmentZKMetadata();
    metadata1.setSegmentName(seg1SegmentName.getSegmentName());
    metadata1.setCreationTime(seg1time);
    Assert.assertEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), 0.0);
    flushThresholdUpdater.updateFlushThreshold(metadata1, streamConfig, metadata0, committingSegmentDescriptor, null);
    final double currentRatio = flushThresholdUpdater.getLatestSegmentRowsToSizeRatio();
    Assert.assertTrue(currentRatio > 0.0);

    // Next segment update from partition 1 does not change the ratio.

    LLCSegmentName seg2SegmentName = new LLCSegmentName(tableName, 1, seqNum + 2, now);
    LLCRealtimeSegmentZKMetadata metadata2 = new LLCRealtimeSegmentZKMetadata();
    metadata2.setSegmentName(seg2SegmentName.getSegmentName());
    metadata2.setStartTime(now);
    committingSegmentDescriptor =
        new CommittingSegmentDescriptor(seg1SegmentName.getSegmentName(), startOffset + 1000, 256_000_000);
    metadata1.setTotalRawDocs(2_980_880);
    flushThresholdUpdater.updateFlushThreshold(metadata2, streamConfig, metadata1, committingSegmentDescriptor, null);
    Assert.assertEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), currentRatio);

    // But if seg1 is from partition 0, the ratio is changed.
    seg1SegmentName = new LLCSegmentName(tableName, 0, seqNum + 1, seg1time);
    metadata1.setSegmentName(seg1SegmentName.getSegmentName());
    flushThresholdUpdater.updateFlushThreshold(metadata2, streamConfig, metadata1, committingSegmentDescriptor, null);
    Assert.assertTrue(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio() != currentRatio);
  }
}
