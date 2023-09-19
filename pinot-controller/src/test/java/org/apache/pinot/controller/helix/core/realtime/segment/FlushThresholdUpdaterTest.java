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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


// TODO: In SegmentSizeBasedFlushThresholdUpdater, timeConsumed is calculated based on System.currentTimeMillis(). Mock
//       the time if necessary.
public class FlushThresholdUpdaterTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  // Segment sizes in MB for each 100_000 rows consumed
  private static final long[] EXPONENTIAL_GROWTH_SEGMENT_SIZES_MB =
      {50, 60, 70, 83, 98, 120, 160, 200, 250, 310, 400, 500, 600, 700, 800, 950, 1130, 1400, 1700, 2000};
  private static final long[] LOGARITHMIC_GROWTH_SEGMENT_SIZES_MB =
      {70, 180, 290, 400, 500, 605, 690, 770, 820, 865, 895, 920, 940, 955, 970, 980, 1000, 1012, 1020, 1030};
  private static final long[] STEPS_SEGMENT_SIZES_MB =
      {100, 100, 200, 200, 300, 300, 400, 400, 500, 500, 600, 600, 700, 700, 800, 800, 900, 900, 1000, 1000};

  /**
   * Tests that the flush threshold update manager returns the right updater given various scenarios of flush threshold
   * setting in the stream config.
   */
  @Test
  public void testFlushThresholdUpdateManager() {
    FlushThresholdUpdateManager flushThresholdUpdateManager = new FlushThresholdUpdateManager();

    // Flush threshold rows larger than 0 - DefaultFlushThresholdUpdater should be returned
    FlushThresholdUpdater defaultFlushThresholdUpdater = flushThresholdUpdateManager
        .getFlushThresholdUpdater(mockStreamConfig(StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS));
    assertTrue(defaultFlushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) defaultFlushThresholdUpdater).getTableFlushSize(),
        StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);

    // Flush threshold rows set to 0 - SegmentSizeBasedFlushThresholdUpdater should be returned
    StreamConfig autotuneStreamConfig = mockDefaultAutotuneStreamConfig();
    FlushThresholdUpdater autotuneFlushThresholdUpdater =
        flushThresholdUpdateManager.getFlushThresholdUpdater(autotuneStreamConfig);
    assertTrue(autotuneFlushThresholdUpdater instanceof SegmentSizeBasedFlushThresholdUpdater);

    // Call again with flush threshold rows set to 0 - same Object should be returned
    assertSame(flushThresholdUpdateManager.getFlushThresholdUpdater(mockAutotuneStreamConfig(10000L, 10000L, 10000)),
        autotuneFlushThresholdUpdater);

    // Call again with flush threshold rows set larger than 0 - DefaultFlushThresholdUpdater should be returned
    defaultFlushThresholdUpdater = flushThresholdUpdateManager.getFlushThresholdUpdater(mockStreamConfig(10000));
    assertTrue(defaultFlushThresholdUpdater instanceof DefaultFlushThresholdUpdater);
    assertEquals(((DefaultFlushThresholdUpdater) defaultFlushThresholdUpdater).getTableFlushSize(), 10000);

    // Call again with flush threshold rows set to 0 - a different Object should be returned
    assertNotSame(flushThresholdUpdateManager.getFlushThresholdUpdater(autotuneStreamConfig),
        autotuneFlushThresholdUpdater);

    // Clear the updater
    flushThresholdUpdateManager.clearFlushThresholdUpdater(REALTIME_TABLE_NAME);

    // Call again with flush threshold rows set to 0 - a different Object should be returned
    assertNotSame(flushThresholdUpdateManager.getFlushThresholdUpdater(autotuneStreamConfig),
        autotuneFlushThresholdUpdater);
  }

  private StreamConfig mockStreamConfig(int flushThresholdRows) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTableNameWithType()).thenReturn(REALTIME_TABLE_NAME);
    when(streamConfig.getFlushThresholdRows()).thenReturn(flushThresholdRows);
    return streamConfig;
  }

  private StreamConfig mockAutotuneStreamConfig(long flushSegmentDesiredSizeBytes,
      long flushThresholdTimeMillis, int flushAutotuneInitialRows) {
    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getTableNameWithType()).thenReturn(REALTIME_TABLE_NAME);
    when(streamConfig.getFlushThresholdRows()).thenReturn(0);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(flushSegmentDesiredSizeBytes);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(flushThresholdTimeMillis);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(flushAutotuneInitialRows);
    return streamConfig;
  }

  private StreamConfig mockDefaultAutotuneStreamConfig() {
    return mockAutotuneStreamConfig(StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES,
        StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS, StreamConfig.DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS);
  }

  /**
   * Tests the segment size based flush threshold updater.
   * We have 3 types of dataset, each having a different segment size to num rows ratio (exponential growth, logarithmic
   * growth, steps). For each type of dataset, we let 500 segments pass through our algorithm, and always hit the rows
   * threshold. Towards the end, we should get the segment size stabilized around the desired segment size (200MB).
   */
  @Test
  public void testSegmentSizeBasedFlushThreshold() {
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();
    long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    long segmentSizeLowerLimit = (long) (desiredSegmentSizeBytes * 0.99);
    long segmentSizeHigherLimit = (long) (desiredSegmentSizeBytes * 1.01);

    for (long[] segmentSizesMB : Arrays
        .asList(EXPONENTIAL_GROWTH_SEGMENT_SIZES_MB, LOGARITHMIC_GROWTH_SEGMENT_SIZES_MB, STEPS_SEGMENT_SIZES_MB)) {
      SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();

      // Start consumption
      SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
      CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(0L);
      flushThresholdUpdater
          .updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor, null, 1,
              Collections.emptyList());
      assertEquals(newSegmentZKMetadata.getSizeThresholdToFlushSegment(), streamConfig.getFlushAutotuneInitialRows());

      int numRuns = 500;
      int checkRunsAfter = 400;
      for (int run = 0; run < numRuns; run++) {
        int numRowsConsumed = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
        long segmentSizeBytes = getSegmentSizeBytes(numRowsConsumed, segmentSizesMB);
        committingSegmentDescriptor = getCommittingSegmentDescriptor(segmentSizeBytes);
        SegmentZKMetadata committingSegmentZKMetadata =
            getCommittingSegmentZKMetadata(System.currentTimeMillis(), numRowsConsumed, numRowsConsumed);
        flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
            committingSegmentZKMetadata, 1, Collections.emptyList());

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          assertTrue(segmentSizeBytes > segmentSizeLowerLimit && segmentSizeBytes < segmentSizeHigherLimit);
        }
      }
    }
  }

  @Test
  public void testSegmentSizeBasedFlushThresholdMinPartition() {
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();
    long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    long segmentSizeLowerLimit = (long) (desiredSegmentSizeBytes * 0.99);
    long segmentSizeHigherLimit = (long) (desiredSegmentSizeBytes * 1.01);

    for (long[] segmentSizesMB : Arrays
        .asList(EXPONENTIAL_GROWTH_SEGMENT_SIZES_MB, LOGARITHMIC_GROWTH_SEGMENT_SIZES_MB, STEPS_SEGMENT_SIZES_MB)) {
      SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();

      // Start consumption
      SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(1);
      CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(0L);
      flushThresholdUpdater
          .updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor, null, 1,
              getPartitionGroupMetadataList(3, 1));
      assertEquals(newSegmentZKMetadata.getSizeThresholdToFlushSegment(), streamConfig.getFlushAutotuneInitialRows());

      int numRuns = 500;
      int checkRunsAfter = 400;
      for (int run = 0; run < numRuns; run++) {
        int numRowsConsumed = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
        long segmentSizeBytes = getSegmentSizeBytes(numRowsConsumed, segmentSizesMB);
        committingSegmentDescriptor = getCommittingSegmentDescriptor(segmentSizeBytes);
        SegmentZKMetadata committingSegmentZKMetadata =
            getCommittingSegmentZKMetadata(System.currentTimeMillis(), numRowsConsumed, numRowsConsumed);
        flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
            committingSegmentZKMetadata, 1, getPartitionGroupMetadataList(3, 1));

        // Assert that segment size is in limits
        if (run > checkRunsAfter) {
          assertTrue(segmentSizeBytes > segmentSizeLowerLimit && segmentSizeBytes < segmentSizeHigherLimit);
        }
      }
    }
  }

  private SegmentZKMetadata getNewSegmentZKMetadata(int partitionId) {
    return new SegmentZKMetadata(
        new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, System.currentTimeMillis()).getSegmentName());
  }

  private List<PartitionGroupMetadata> getPartitionGroupMetadataList(int numPartitions, int startPartitionId) {
    List<PartitionGroupMetadata> newPartitionGroupMetadataList = new ArrayList<>();

    for (int i = 0; i < numPartitions; i++) {
      newPartitionGroupMetadataList.add(new PartitionGroupMetadata(startPartitionId + i, null));
    }

    return newPartitionGroupMetadataList;
  }

  private CommittingSegmentDescriptor getCommittingSegmentDescriptor(long segmentSizeBytes) {
    return new CommittingSegmentDescriptor(null, new LongMsgOffset(0).toString(), segmentSizeBytes);
  }

  private SegmentZKMetadata getCommittingSegmentZKMetadata(long creationTime, int sizeThresholdToFlushSegment,
      int totalDocs) {
    SegmentZKMetadata committingSegmentZKMetadata = new SegmentZKMetadata("ignored");
    committingSegmentZKMetadata.setCreationTime(creationTime);
    committingSegmentZKMetadata.setSizeThresholdToFlushSegment(sizeThresholdToFlushSegment);
    committingSegmentZKMetadata.setTotalDocs(totalDocs);
    return committingSegmentZKMetadata;
  }

  private long getSegmentSizeBytes(int numRowsConsumed, long[] segmentSizesMB) {
    double segmentSizeMB;
    if (numRowsConsumed < 100_000) {
      segmentSizeMB = (double) segmentSizesMB[0] / 100_000 * numRowsConsumed;
    } else {
      int index = Integer.min(numRowsConsumed / 100_000, 19);
      segmentSizeMB = segmentSizesMB[index] + (double) (segmentSizesMB[index] - segmentSizesMB[index - 1]) / 100_000 * (
          numRowsConsumed - index * 100_000);
    }
    return (long) (segmentSizeMB * 1024 * 1024);
  }

  @Test
  public void testTimeThreshold() {
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();

    // Start consumption
    SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(0L);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor, null, 1,
        Collections.emptyList());
    int sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();

    // First segment consumes rows less than the threshold
    committingSegmentDescriptor = getCommittingSegmentDescriptor(128_000L);
    int numRowsConsumed = 15_000;
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold,
        (int) (numRowsConsumed * SegmentFlushThresholdComputer.ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));

    // Second segment hits the rows threshold
    numRowsConsumed = sizeThreshold;
    committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    assertNotEquals(newSegmentZKMetadata.getSizeThresholdToFlushSegment(),
        (int) (numRowsConsumed * SegmentFlushThresholdComputer.ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));
  }

  @Test
  public void testMinThreshold() {
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();

    // Start consumption
    SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(0L);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor, null, 1,
        Collections.emptyList());
    int sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();

    // First segment only consumed 15 rows, so next segment should have size threshold of 10_000
    committingSegmentDescriptor = getCommittingSegmentDescriptor(128L);
    int numRowsConsumed = 15;
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold, SegmentFlushThresholdComputer.MINIMUM_NUM_ROWS_THRESHOLD);

    // Next segment only consumed 20 rows, so size threshold should still be 10_000
    numRowsConsumed = 20;
    committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold, SegmentFlushThresholdComputer.MINIMUM_NUM_ROWS_THRESHOLD);
  }

  @Test
  public void testNonZeroPartitionUpdates() {
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();
    StreamConfig streamConfig = mockDefaultAutotuneStreamConfig();

    // Start consumption for 2 partitions
    SegmentZKMetadata newSegmentZKMetadataForPartition0 = getNewSegmentZKMetadata(0);
    SegmentZKMetadata newSegmentZKMetadataForPartition1 = getNewSegmentZKMetadata(1);
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(0L);
    flushThresholdUpdater
        .updateFlushThreshold(streamConfig, newSegmentZKMetadataForPartition0, committingSegmentDescriptor, null, 1,
            Collections.emptyList());
    flushThresholdUpdater
        .updateFlushThreshold(streamConfig, newSegmentZKMetadataForPartition1, committingSegmentDescriptor, null, 1,
            Collections.emptyList());
    int sizeThresholdForPartition0 = newSegmentZKMetadataForPartition0.getSizeThresholdToFlushSegment();
    int sizeThresholdForPartition1 = newSegmentZKMetadataForPartition1.getSizeThresholdToFlushSegment();
    double sizeRatio = flushThresholdUpdater.getLatestSegmentRowsToSizeRatio();
    assertEquals(sizeThresholdForPartition0, streamConfig.getFlushAutotuneInitialRows());
    assertEquals(sizeThresholdForPartition1, streamConfig.getFlushAutotuneInitialRows());
    assertEquals(sizeRatio, 0.0);

    // First segment from partition 1 should change the size ratio
    committingSegmentDescriptor = getCommittingSegmentDescriptor(128_000_000L);
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThresholdForPartition1,
            sizeThresholdForPartition1);
    flushThresholdUpdater
        .updateFlushThreshold(streamConfig, newSegmentZKMetadataForPartition1, committingSegmentDescriptor,
            committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThresholdForPartition1 = newSegmentZKMetadataForPartition1.getSizeThresholdToFlushSegment();
    sizeRatio = flushThresholdUpdater.getLatestSegmentRowsToSizeRatio();
    assertTrue(sizeRatio > 0.0);

    // Second segment update from partition 1 should not change the size ratio
    committingSegmentDescriptor = getCommittingSegmentDescriptor(256_000_000L);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThresholdForPartition1,
        sizeThresholdForPartition1);
    flushThresholdUpdater
        .updateFlushThreshold(streamConfig, newSegmentZKMetadataForPartition1, committingSegmentDescriptor,
            committingSegmentZKMetadata, 1, Collections.emptyList());
    assertEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), sizeRatio);

    // First segment update from partition 0 should change the size ratio
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(System.currentTimeMillis(), sizeThresholdForPartition0,
        sizeThresholdForPartition0);
    flushThresholdUpdater
        .updateFlushThreshold(streamConfig, newSegmentZKMetadataForPartition0, committingSegmentDescriptor,
            committingSegmentZKMetadata, 1, Collections.emptyList());
    assertNotEquals(flushThresholdUpdater.getLatestSegmentRowsToSizeRatio(), sizeRatio);
  }

  @Test
  public void testSegmentSizeBasedUpdaterWithModifications() {
    SegmentSizeBasedFlushThresholdUpdater flushThresholdUpdater = new SegmentSizeBasedFlushThresholdUpdater();

    // Use customized stream config
    long flushSegmentDesiredSizeBytes = StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES / 2;
    long flushThresholdTimeMillis = StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS / 2;
    int flushAutotuneInitialRows = StreamConfig.DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS / 2;
    StreamConfig streamConfig =
        mockAutotuneStreamConfig(flushSegmentDesiredSizeBytes, flushThresholdTimeMillis, flushAutotuneInitialRows);

    // Start consumption
    SegmentZKMetadata newSegmentZKMetadata = getNewSegmentZKMetadata(0);
    CommittingSegmentDescriptor committingSegmentDescriptor = getCommittingSegmentDescriptor(0L);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor, null, 1,
        Collections.emptyList());
    int sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold, flushAutotuneInitialRows);

    // Hit the row threshold within 90% of the time threshold, produce a segment smaller than the desired size, and
    // should get a higher row threshold
    int numRowsConsumed = sizeThreshold;
    long committingSegmentSize = flushSegmentDesiredSizeBytes * 9 / 10;
    long consumptionDuration = flushThresholdTimeMillis * 9 / 10;
    long creationTime = System.currentTimeMillis() - consumptionDuration;
    committingSegmentDescriptor = getCommittingSegmentDescriptor(committingSegmentSize);
    SegmentZKMetadata committingSegmentZKMetadata =
        getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertTrue(sizeThreshold > numRowsConsumed);

    // Still hit the row threshold within 90% of the time threshold, produce a segment the same size of the previous
    // one, but change the desired size in stream config to be smaller than the segment size, and should get a lower row
    // threshold
    numRowsConsumed = sizeThreshold;
    flushSegmentDesiredSizeBytes = committingSegmentSize * 9 / 10;
    streamConfig =
        mockAutotuneStreamConfig(flushSegmentDesiredSizeBytes, flushThresholdTimeMillis, flushAutotuneInitialRows);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertTrue(sizeThreshold < numRowsConsumed);

    // Does not hit the row threshold within 90% of the time threshold, produce a segment smaller than the desired size,
    // and should get a row threshold based on the number of rows consumed
    numRowsConsumed = sizeThreshold * 9 / 10;
    committingSegmentSize = flushSegmentDesiredSizeBytes * 9 / 10;
    committingSegmentDescriptor = getCommittingSegmentDescriptor(committingSegmentSize);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertEquals(sizeThreshold,
        (long) (numRowsConsumed * SegmentFlushThresholdComputer.ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));

    // Still not hit the row threshold within 90% of the time threshold, produce a segment the same size of the previous
    // one, but reduce the time threshold by half, and should get a lower row threshold
    numRowsConsumed = sizeThreshold * 9 / 10;
    flushThresholdTimeMillis /= 2;
    streamConfig =
        mockAutotuneStreamConfig(flushSegmentDesiredSizeBytes, flushThresholdTimeMillis, flushAutotuneInitialRows);
    committingSegmentZKMetadata = getCommittingSegmentZKMetadata(creationTime, sizeThreshold, numRowsConsumed);
    flushThresholdUpdater.updateFlushThreshold(streamConfig, newSegmentZKMetadata, committingSegmentDescriptor,
        committingSegmentZKMetadata, 1, Collections.emptyList());
    sizeThreshold = newSegmentZKMetadata.getSizeThresholdToFlushSegment();
    assertTrue(sizeThreshold < numRowsConsumed);
  }
}
