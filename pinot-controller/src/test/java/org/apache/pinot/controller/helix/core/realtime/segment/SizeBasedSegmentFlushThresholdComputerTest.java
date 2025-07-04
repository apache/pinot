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
}
