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
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.pinot.common.protocols.SegmentCompletionProtocol.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class SegmentFlushThresholdComputerTest {

  @Test
  public void testUseAutoTuneInitialRowsIfFirstSegmentInPartition() {
    int autoTuneInitialRows = 1_000;
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushAutotuneInitialRows()).thenReturn(autoTuneInitialRows);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, null, "newSegmentName");

    assertEquals(threshold, autoTuneInitialRows);
  }

  @Test
  public void testUseLastSegmentSizeTimesRatioIfFirstSegmentInPartitionAndNewPartitionGroup() {
    double segmentRowsToSizeRatio = 1.5;
    long segmentSizeBytes = 20000L;
    SegmentFlushThresholdComputer computer =
        new SegmentFlushThresholdComputer(Clock.systemUTC(), segmentRowsToSizeRatio);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(segmentSizeBytes);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, null, "newSegmentName");

    // segmentSize * 1.5
    // 20000 * 1.5
    assertEquals(threshold, 30000);
  }

  @Test
  public void testUseLastSegmentSizeTimesRatioIfFirstSegmentInPartitionAndNewPartitionGroupMinimumSize10000Rows() {
    double segmentRowsToSizeRatio = 1.5;
    long segmentSizeBytes = 2000L;
    SegmentFlushThresholdComputer computer =
        new SegmentFlushThresholdComputer(Clock.systemUTC(), segmentRowsToSizeRatio);

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(segmentSizeBytes);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, null, "newSegmentName");

    assertEquals(threshold, 10000);
  }

  @Test
  public void testUseLastSegmentsThresholdIfSegmentSizeMissing() {
    long segmentSizeBytes = 0L;
    int segmentSizeThreshold = 5_000;
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdTimeMillis()).thenReturn(123L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(segmentSizeBytes);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(segmentSizeThreshold);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "newSegmentName");

    assertEquals(threshold, segmentSizeThreshold);
  }

  @Test
  public void testUseLastSegmentsThresholdIfSegmentIsCommittingDueToForceCommit() {
    long committingSegmentSizeBytes = 500_000L;
    int committingSegmentSizeThreshold = 25_000;
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(committingSegmentSizeBytes);
    when(committingSegmentDescriptor.getStopReason()).thenReturn(REASON_FORCE_COMMIT_MESSAGE_RECEIVED);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(committingSegmentSizeThreshold);

    StreamConfig streamConfig = mock(StreamConfig.class);
    
    int newSegmentSizeThreshold =
        computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
            "newSegmentName");

    assertEquals(newSegmentSizeThreshold, committingSegmentSizeThreshold);
  }

  @Test
  public void testApplyMultiplierToTotalDocsWhenTimeThresholdNotReached() {
    long currentTime = 1640216032391L;
    Clock clock = Clock.fixed(java.time.Instant.ofEpochMilli(currentTime), ZoneId.of("UTC"));

    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer(clock);

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

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // totalDocs * 1.1
    // 10000 * 1.1
    assertEquals(threshold, 11_000);
  }

  @Test
  public void testApplyMultiplierToAdjustedTotalDocsWhenTimeThresholdIsReached() {
    long currentTime = 1640216032391L;
    Clock clock = Clock.fixed(java.time.Instant.ofEpochMilli(currentTime), ZoneId.of("UTC"));

    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer(clock);

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

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // (totalDocs / 2) * 1.1
    // (30000 / 2) * 1.1
    // 15000 * 1.1
    assertEquals(threshold, 16_500);
  }

  @Test
  public void testSegmentSizeTooSmall() {
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(500_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // totalDocs / 2
    // 30000 / 2
    assertEquals(threshold, 15_000);
  }

  @Test
  public void testSegmentSizeTooBig() {
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(500_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // totalDocs + (totalDocs / 2)
    // 30000 + (30000 / 2)
    assertEquals(threshold, 45_000);
  }

  @Test
  public void testSegmentSizeJustRight() {
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(250_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(20_000);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // (totalDocs / segmentSize) * flushThresholdSegmentSize
    // (30000 / 250000) * 300000
    assertEquals(threshold, 36_000);
  }

  @Test
  public void testNoRows() {
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(250_0000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(0L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(0);

    int threshold = computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // max((totalDocs / segmentSize) * flushThresholdSegmentSize, 10000)
    // max(0, 10000)
    assertEquals(threshold, 10_000);
  }

  @Test
  public void testAdjustRowsToSizeRatio() {
    SegmentFlushThresholdComputer computer = new SegmentFlushThresholdComputer();

    StreamConfig streamConfig = mock(StreamConfig.class);
    when(streamConfig.getFlushThresholdSegmentSizeBytes()).thenReturn(300_0000L);

    CommittingSegmentDescriptor committingSegmentDescriptor = mock(CommittingSegmentDescriptor.class);
    when(committingSegmentDescriptor.getSegmentSizeBytes()).thenReturn(200_000L);

    SegmentZKMetadata committingSegmentZKMetadata = mock(SegmentZKMetadata.class);
    when(committingSegmentZKMetadata.getTotalDocs()).thenReturn(30_000L, 50_000L);
    when(committingSegmentZKMetadata.getSizeThresholdToFlushSegment()).thenReturn(60_000);

    computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // (totalDocs / segmentSize)
    // (30000 / 200000)
    assertEquals(computer.getLatestSegmentRowsToSizeRatio(), 0.15);

    computer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
        "events3__0__0__20211222T1646Z");

    // (0.1 * (totalDocs / segmentSize)) + (0.9 * lastRatio)
    // (0.1 * (50000 / 200000)) + (0.9 * 0.15)
    // (0.1 * 0.25) + (0.9 * 0.15)
    assertEquals(computer.getLatestSegmentRowsToSizeRatio(), 0.16);
  }
}
