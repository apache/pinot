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

import java.time.Clock;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pinot.common.protocols.SegmentCompletionProtocol.REASON_FORCE_COMMIT_MESSAGE_RECEIVED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SizeBasedSegmentFlushThresholdComputerTest {

  @Test
  public void testUseAutoTuneInitialRowsIfFirstSegmentInPartition() {
    int autoTuneInitialRows = 1_000;
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(autoTuneInitialRows);

    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    assertEquals(threshold, autoTuneInitialRows);
  }

  @Test
  public void testUseLastSegmentSizeTimesRatioIfFirstSegmentInPartitionAndNewPartitionGroup() {
    long segmentSizeBytes = 20000L;
    double segmentRowsToSizeRatio = 1.5;
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer(Clock.systemUTC());
    computer.setSizeForLastSegment(segmentSizeBytes);
    computer.setSegmentRowsToSizeRatio(segmentRowsToSizeRatio);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(segmentSizeBytes);

    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // segmentSize * 1.5
    // 20000 * 1.5
    assertEquals(threshold, 30000);
  }

  @Test
  public void testUseLastSegmentSizeTimesRatioIfFirstSegmentInPartitionAndNewPartitionGroupMinimumSize10000Rows() {
    long segmentSizeBytes = 2000L;
    double segmentRowsToSizeRatio = 1.5;
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer(Clock.systemUTC());
    computer.setSizeForLastSegment(segmentSizeBytes);
    computer.setSegmentRowsToSizeRatio(segmentRowsToSizeRatio);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(segmentSizeBytes);

    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    assertEquals(threshold, 10000);
  }

  @Test
  public void testUseLastSegmentsThresholdIfSegmentSizeMissing() {
    long segmentSizeBytes = 0L;
    int segmentSizeThreshold = 5_000;
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(123L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(segmentSizeBytes);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(segmentSizeThreshold);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    assertEquals(threshold, segmentSizeThreshold);
  }

  @Test
  public void testUseLastSegmentsThresholdIfSegmentIsCommittingDueToForceCommit() {
    long committingSegmentSizeBytes = 500_000L;
    int committingSegmentSizeThreshold = 25_000;
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(committingSegmentSizeBytes);
    when(committingSegmentDescriptor.getStopReason()).thenReturn(REASON_FORCE_COMMIT_MESSAGE_RECEIVED);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(committingSegmentSizeThreshold);

    StreamConfig streamConfig = mock(StreamConfig.class);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    assertEquals(threshold, committingSegmentSizeThreshold);
  }

  @Test
  public void testApplyMultiplierToTotalDocsWhenTimeThresholdNotReached() {
    long currentTime = 1640216032391L;
    Clock clock = Clock.fixed(java.time.Instant.ofEpochMilli(currentTime), ZoneId.of("UTC"));

    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer(clock);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(MILLISECONDS.convert(6, TimeUnit.HOURS));

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(10_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);
    when(committingSegmentZKMetadata.getCreationTime()).thenReturn(
        currentTime - MILLISECONDS.convert(1, TimeUnit.HOURS));

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // totalDocs * 1.1
    // 10000 * 1.1
    assertEquals(threshold, 11_000);
  }

  @Test
  public void testApplyMultiplierToAdjustedTotalDocsWhenTimeThresholdIsReached() {
    long currentTime = 1640216032391L;
    Clock clock = Clock.fixed(java.time.Instant.ofEpochMilli(currentTime), ZoneId.of("UTC"));

    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer(clock);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(MILLISECONDS.convert(1, TimeUnit.HOURS));

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(60_000);
    when(committingSegmentZKMetadata.getCreationTime()).thenReturn(
        currentTime - MILLISECONDS.convert(2, TimeUnit.HOURS));

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // (totalDocs / 2) * 1.1
    // (30000 / 2) * 1.1
    // 15000 * 1.1
    assertEquals(threshold, 16_500);
  }

  @Test
  public void testSegmentSizeTooSmall() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(500_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // totalDocs / 2
    // 30000 / 2
    assertEquals(threshold, 15_000);
  }

  @Test
  public void testSegmentSizeTooBig() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(500_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // totalDocs + (totalDocs / 2)
    // 30000 + (30000 / 2)
    assertEquals(threshold, 45_000);
  }

  @Test
  public void testSegmentSizeJustRight() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(250_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // (totalDocs / segmentSize) * flushThresholdSegmentSize
    // (30000 / 250000) * 300000
    assertEquals(threshold, 36_000);
  }

  @Test
  public void testNoRows() {
    int autoTuneInitialRows = 1_000;
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(autoTuneInitialRows);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(250_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(0L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(0);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Should use initial rows
    assertEquals(threshold, autoTuneInitialRows);
  }

  @Test
  public void testAdjustRowsToSizeRatio() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L, 50_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(60_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);

    // (totalDocs / segmentSize)
    // (30000 / 200000)
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.15);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);

    // (0.1 * (totalDocs / segmentSize)) + (0.9 * lastRatio)
    // (0.1 * (50000 / 200000)) + (0.9 * 0.15)
    // (0.1 * 0.25) + (0.9 * 0.15)
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.16);
  }

  @Test(invocationCount = 1000)
  public void testSegmentFlushThresholdVariance() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();
    int threshold = 90000;
    for (double var = 0; var <= 0.5; var += 0.05) {
      StreamConfig streamConfig = mock(StreamConfig.class);
      when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(200_0000L);
      when(streamConfig.getStreamConfigsMap()).thenReturn(
          Map.of("realtime.segment.flush.threshold.variance.percentage", String.valueOf(var)));

      CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
      when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(300_000L);

      SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
      when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(60_000L, 50_000L);
      when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(60_000);

      computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
      int computedThreshold = computer.computeThreshold(streamConfig, "newSegmentName");

      assertTrue(computedThreshold >= (1.0 - var) * threshold && computedThreshold <= (1.0 + var) * threshold);
    }
  }

  // ===== COMMIT-TIME COMPACTION TESTS =====

  @Test
  public void testTraditionalBehaviorWithoutCommitTimeCompaction() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(-1); // Default: not available

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L); // Post-commit rows
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Should use post-commit rows (30,000) for calculation
    // (30,000 / 200,000) * 300,000 = 45,000
    assertEquals(threshold, 45_000);
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.15); // 30,000 / 200,000
  }

  @Test
  public void testTraditionalBehaviorWithZeroPreCommitRows() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(-1); // No pre-commit data available

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(25_000L); // Post-commit rows
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Should use post-commit rows (25,000) for calculation
    // (25,000 / 200,000) * 300,000 = 37,500
    assertEquals(threshold, 37_500);
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.125); // 25,000 / 200,000
  }

  @Test
  public void testCommitTimeCompactionWithHighCompactionRatio() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(100_000); // High pre-commit count

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(20_000L); // Low post-commit (80% compaction)
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(80_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Uses estimated pre-commit size (derived) leading to size too big, so halves pre-commit rows
    // Estimated pre-commit size = 200,000 * (100,000 / 20,000) = 1,000,000 > 450,000 => 100,000 / 2 = 50,000
    assertEquals(threshold, 50_000);
    // Ratio equals post-commit rows / post-commit size
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.1);
  }

  @Test
  public void testCommitTimeCompactionWithMediumCompactionRatio() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(400_000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(250_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(60_000); // Pre-commit count

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(40_000L); // Post-commit (33% compaction)
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(50_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Estimated pre-commit size = 250,000 * (60,000 / 40,000) = 375,000 (within optimal)
    // Ratio equals 40,000 / 250,000 = 0.16 -> target rows = 400,000 * 0.16 = 64,000
    assertEquals(threshold, 64_000);
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.16);
  }

  @Test
  public void testCommitTimeCompactionWithLowCompactionRatio() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(350_000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(180_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(45_000); // Pre-commit count

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(42_000L); // Post-commit (7% compaction)
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(40_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Estimated pre-commit size = 180,000 * (45,000 / 42,000) ≈ 192,857 (within optimal)
    // Ratio equals 45,000 / 192,857 ≈ 0.23333 -> target rows ≈ 81,666
    assertEquals(threshold, 81_666);
    assertEquals(computer.getSegmentRowsToSizeRatio(), 45000d / 192857d, 1.0e-6);
  }

  @Test
  public void testCommitTimeCompactionWithTimeThresholdHit() {
    long currentTime = 1640216032391L;
    Clock clock = Clock.fixed(java.time.Instant.ofEpochMilli(currentTime), ZoneId.of("UTC"));

    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer(clock);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(MILLISECONDS.convert(6, TimeUnit.HOURS));

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(80_000); // High pre-commit count

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(15_000L); // Low post-commit (81% compaction)
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(100_000);
    when(committingSegmentZKMetadata.getCreationTime()).thenReturn(
        currentTime - MILLISECONDS.convert(1, TimeUnit.HOURS)); // Time threshold not hit

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Estimated pre-commit size = 200,000 * (80,000 / 15,000) ≈ 1,066,667 (> optimal), so halve pre-commit rows
    assertEquals(threshold, 40_000);
  }

  @Test
  public void testRatioCalculationProgressionWithCommitTimeCompaction() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    // First segment: traditional (no compaction)
    CommittingSegmentDescriptor firstDescriptor = mock(CommittingSegmentDescriptor.class);
    when(firstDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(firstDescriptor.getPreCommitRowCount()).thenReturn(-1); // No compaction

    SegmentZKMetadata firstMetadata = mock(SegmentZKMetadata.class);
    when(firstMetadata.getTotalDocs()).thenReturn(40_000L);
    when(firstMetadata.getSizeThresholdToFlushSegment()).thenReturn(30_000);

    computer.onSegmentCommit(firstDescriptor, firstMetadata);
    double firstRatio = computer.getSegmentRowsToSizeRatio();
    assertEquals(firstRatio, 0.2); // 40,000 / 200,000

    // Second segment: with compaction
    CommittingSegmentDescriptor secondDescriptor = mock(CommittingSegmentDescriptor.class);
    when(secondDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(secondDescriptor.getPreCommitRowCount()).thenReturn(60_000); // With compaction

    SegmentZKMetadata secondMetadata = mock(SegmentZKMetadata.class);
    when(secondMetadata.getTotalDocs()).thenReturn(30_000L); // Post-commit (50% compaction)
    when(secondMetadata.getSizeThresholdToFlushSegment()).thenReturn(40_000);

    computer.onSegmentCommit(secondDescriptor, secondMetadata);
    double secondRatio = computer.getSegmentRowsToSizeRatio();

    // Ratio uses post-commit rows/size for the update when pre-commit rows are present
    // (0.1 * (30,000 / 200,000)) + (0.9 * 0.2) = (0.1 * 0.15) + (0.9 * 0.2) = 0.195
    assertEquals(secondRatio, 0.195, 0.001);
  }

  @Test
  public void testDebugMixedScenarios() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    // First segment: With compaction
    CommittingSegmentDescriptor compactedDescriptor = mock(CommittingSegmentDescriptor.class);
    when(compactedDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(compactedDescriptor.getPreCommitRowCount()).thenReturn(80_000);

    SegmentZKMetadata compactedMetadata = mock(SegmentZKMetadata.class);
    when(compactedMetadata.getTotalDocs()).thenReturn(20_000L);
    when(compactedMetadata.getSizeThresholdToFlushSegment()).thenReturn(60_000);
    when(compactedMetadata.getSegmentName()).thenReturn("segment1");

    computer.onSegmentCommit(compactedDescriptor, compactedMetadata);
    double firstRatio = computer.getSegmentRowsToSizeRatio();
    assertEquals(firstRatio, 0.1, 0.001); // uses post-commit rows/size ratio

    // Test threshold after first segment
    int firstThreshold = computer.computeThreshold(streamConfig, "nextSegment1");
    // Estimated pre-commit size is large -> halve pre-commit rows -> 40,000
    assertEquals(firstThreshold, 40_000);
  }

  @Test
  public void testMixedScenariosWithAndWithoutCompaction() {
    long currentTime = System.currentTimeMillis();
    Clock clock = Clock.fixed(java.time.Instant.ofEpochMilli(currentTime), ZoneId.of("UTC"));

    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer(clock);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(MILLISECONDS.convert(6, TimeUnit.HOURS));

    // Segment 1: With compaction - Make it hit row threshold, not time threshold
    CommittingSegmentDescriptor compactedDescriptor = mock(CommittingSegmentDescriptor.class);
    when(compactedDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(compactedDescriptor.getPreCommitRowCount()).thenReturn(80_000);

    SegmentZKMetadata compactedMetadata = mock(SegmentZKMetadata.class);
    when(compactedMetadata.getTotalDocs()).thenReturn(20_000L);
    // Less than pre-commit to hit row threshold
    when(compactedMetadata.getSizeThresholdToFlushSegment()).thenReturn(60_000);
    when(compactedMetadata.getSegmentName()).thenReturn("segment1");
    when(compactedMetadata.getCreationTime()).thenReturn(currentTime - MILLISECONDS.convert(1, TimeUnit.HOURS));

    computer.onSegmentCommit(compactedDescriptor, compactedMetadata);
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.1); // uses post-commit rows/size ratio

    // Segment 2: Without compaction - Make it hit row threshold, not time threshold
    CommittingSegmentDescriptor traditionalDescriptor = mock(CommittingSegmentDescriptor.class);
    when(traditionalDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(traditionalDescriptor.getPreCommitRowCount()).thenReturn(-1); // No compaction data

    SegmentZKMetadata traditionalMetadata = mock(SegmentZKMetadata.class);
    when(traditionalMetadata.getTotalDocs()).thenReturn(30_000L);
    // Less than rows consumed to hit row threshold
    when(traditionalMetadata.getSizeThresholdToFlushSegment()).thenReturn(25_000);
    when(traditionalMetadata.getSegmentName()).thenReturn("segment2");
    when(traditionalMetadata.getCreationTime()).thenReturn(currentTime - MILLISECONDS.convert(1, TimeUnit.HOURS));

    computer.onSegmentCommit(traditionalDescriptor, traditionalMetadata);

    // Traditional segment uses post-commit rows/size; new ratio: 0.1*0.15 + 0.9*0.1 = 0.105
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.105, 0.001);

    // Next threshold calculation should work correctly
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");
    // (300,000 * 0.105) = 31,500
    assertEquals(threshold, 31_500);
  }

  @Test
  public void testEdgeCaseZeroPreCommitRowsWithCompaction() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(0); // Actually zero pre-commit rows

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(25_000L); // Post-commit rows
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Should use post-commit rows (25,000) since pre-commit is 0
    // (25,000 / 200,000) * 300,000 = 37,500
    assertEquals(threshold, 37_500);
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.125); // 25,000 / 200,000
  }

  @Test
  public void testEdgeCaseZeroPostCommitRowsWithPreCommitRows() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(50_000);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(100_000); // Has pre-commit data

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(0L); // Zero post-commit rows
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(80_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);

    // Should use pre-commit rows (100,000) since it's > 0, even though post-commit is 0
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.5); // 100,000 / 200,000

    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");
    // Should not fall back to initial rows since we have valid ratio
    assertTrue(threshold > 50_000); // Should be significantly higher than initial rows
  }

  @Test
  public void testForceCommitSkipsRatioUpdateWithCommitTimeCompaction() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(25_000);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(500_000L);
    when(committingSegmentDescriptor.getPreCommitRowCount()).thenReturn(100_000); // High pre-commit count
    when(committingSegmentDescriptor.getStopReason()).thenReturn(REASON_FORCE_COMMIT_MESSAGE_RECEIVED);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(20_000L); // Low post-commit
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(80_000);

    computer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);

    // Ratio should remain 0 due to force commit, regardless of pre-commit data
    assertEquals(computer.getSegmentRowsToSizeRatio(), 0.0);

    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");
    // Should use the previous segment's threshold (80,000)
    assertEquals(threshold, 80_000);
  }

  @Test
  public void testSegmentSizeAdjustmentWithCommitTimeCompaction() {
    SizeBasedSegmentFlushThresholdComputer computer = new SizeBasedSegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_000L);

    // Test case: segment size too big
    CommittingSegmentDescriptor largeSizeDescriptor = mock(CommittingSegmentDescriptor.class);
    when(largeSizeDescriptor.getSegmentSizeBytes()).thenReturn(500_000L); // Much larger than target
    when(largeSizeDescriptor.getPreCommitRowCount()).thenReturn(120_000); // Pre-commit count

    SegmentZKMetadata largeSizeMetadata = mock(SegmentZKMetadata.class);
    when(largeSizeMetadata.getTotalDocs()).thenReturn(24_000L); // Post-commit (80% compaction)
    when(largeSizeMetadata.getSizeThresholdToFlushSegment()).thenReturn(100_000);

    computer.onSegmentCommit(largeSizeDescriptor, largeSizeMetadata);
    int threshold = computer.computeThreshold(streamConfig, "newSegmentName");

    // Should use pre-commit rows (120,000) and halve it due to large segment size
    // 120,000 / 2 = 60,000
    assertEquals(threshold, 60_000);
  }
}
