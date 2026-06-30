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
package org.apache.pinot.controller.helix.core.retention.strategy;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for time retention.
 */
public class TimeRetentionStrategyTest {

  @Test
  public void testTimeRetention() {
    String tableNameWithType = "myTable_OFFLINE";
    TimeRetentionStrategy retentionStrategy = new TimeRetentionStrategy(TimeUnit.DAYS, 30L);

    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata("mySegment");

    // Without setting time unit or end time, should not throw exception
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
    segmentZKMetadata.setTimeUnit(TimeUnit.DAYS);
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));

    // Set end time to Jan 2nd, 1970 (not purgeable due to bogus timestamp)
    segmentZKMetadata.setEndTime(1L);
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));

    // Set end time to today
    long today = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    segmentZKMetadata.setEndTime(today);
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));

    // Set end time to two weeks ago
    segmentZKMetadata.setEndTime(today - 14);
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));

    // Set end time to two months ago (purgeable due to being past the retention period)
    segmentZKMetadata.setEndTime(today - 60);
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));

    // Set end time to 200 years in the future (not purgeable due to bogus timestamp)
    segmentZKMetadata.setEndTime(today + (365 * 200));
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata));
  }

  @Test
  public void testIncompleteSegmentRetention() {
    String tableNameWithType = "myTable_REALTIME";
    TimeRetentionStrategy retentionStrategy = new TimeRetentionStrategy(TimeUnit.DAYS, 30L);

    // Test IN_PROGRESS segment (consuming segment)
    SegmentZKMetadata consumingSegmentMetadata = new SegmentZKMetadata("myConsumingSegment");
    consumingSegmentMetadata.setStatus(Status.IN_PROGRESS);

    // Consuming segments have end time of -1, which would normally trigger the warning log
    // But with our fix, consuming segments should not be purgeable regardless of end time
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, consumingSegmentMetadata));

    // Test COMMITTING segment (pauseless ingestion)
    SegmentZKMetadata committingSegmentMetadata = new SegmentZKMetadata("myCommittingSegment");
    committingSegmentMetadata.setStatus(Status.COMMITTING);

    // Committing segments also have end time of -1 until they are fully committed
    // They should not be purgeable either
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, committingSegmentMetadata));

    // Test with completed statuses to ensure they still follow normal retention logic
    SegmentZKMetadata doneSegmentMetadata = new SegmentZKMetadata("myDoneSegment");
    doneSegmentMetadata.setStatus(Status.DONE);
    doneSegmentMetadata.setTimeUnit(TimeUnit.DAYS);

    // Set end time to two months ago (should be purgeable for completed segments)
    long today = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
    doneSegmentMetadata.setEndTime(today - 60);
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, doneSegmentMetadata));

    // Test UPLOADED status as well
    SegmentZKMetadata uploadedSegmentMetadata = new SegmentZKMetadata("myUploadedSegment");
    uploadedSegmentMetadata.setStatus(Status.UPLOADED);
    uploadedSegmentMetadata.setTimeUnit(TimeUnit.DAYS);
    uploadedSegmentMetadata.setEndTime(today - 60);
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, uploadedSegmentMetadata));
  }

  @Test
  public void testOfflineTableRetention() {
    String tableNameWithType = "myTable_OFFLINE";
    TimeRetentionStrategy retentionStrategy = new TimeRetentionStrategy(TimeUnit.DAYS, 30L);

    // Test offline segment - these don't have status field set, so getStatus() returns default UPLOADED
    // But we need to ensure they follow normal retention logic for offline tables
    SegmentZKMetadata offlineSegmentMetadata = new SegmentZKMetadata("myOfflineSegment");
    // Note: We don't set status for offline segments - it defaults to UPLOADED
    offlineSegmentMetadata.setTimeUnit(TimeUnit.DAYS);

    // Verify that offline segments have default status of UPLOADED
    assertEquals(Status.UPLOADED, offlineSegmentMetadata.getStatus());
    assertTrue(offlineSegmentMetadata.getStatus().isCompleted());

    long today = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());

    // Set end time to two weeks ago (should not be purgeable - within retention period)
    offlineSegmentMetadata.setEndTime(today - 14);
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, offlineSegmentMetadata));

    // Set end time to two months ago (should be purgeable - beyond retention period)
    offlineSegmentMetadata.setEndTime(today - 60);
    assertTrue(retentionStrategy.isPurgeable(tableNameWithType, offlineSegmentMetadata));

    // Test offline segment with invalid end time (should not be purgeable)
    offlineSegmentMetadata.setEndTime(-1);
    assertFalse(retentionStrategy.isPurgeable(tableNameWithType, offlineSegmentMetadata));
  }

  @Test
  public void testCreationTimeFallback() {
    String tableNameWithType = "myTable_REALTIME";
    long retentionDays = 30L;

    // Strategy WITHOUT fallback (default behavior)
    TimeRetentionStrategy strategyNoFallback = new TimeRetentionStrategy(TimeUnit.DAYS, retentionDays, false);

    // Strategy WITH fallback enabled
    TimeRetentionStrategy strategyWithFallback = new TimeRetentionStrategy(TimeUnit.DAYS, retentionDays, true);

    long now = System.currentTimeMillis();
    long ninetyDaysAgoMs = now - TimeUnit.DAYS.toMillis(90);
    long tenDaysAgoMs = now - TimeUnit.DAYS.toMillis(10);

    // Case 1: Invalid end time, fallback DISABLED — should NOT be purgeable (existing behavior)
    SegmentZKMetadata seg1 = new SegmentZKMetadata("seg_fallback_disabled");
    seg1.setStatus(Status.DONE);
    seg1.setCreationTime(ninetyDaysAgoMs);
    // end time not set → getEndTimeMs() returns -1
    assertFalse(strategyNoFallback.isPurgeable(tableNameWithType, seg1));

    // Case 2: Invalid end time, fallback ENABLED, old creation time → PURGEABLE
    SegmentZKMetadata seg2 = new SegmentZKMetadata("seg_fallback_old_creation");
    seg2.setStatus(Status.DONE);
    seg2.setCreationTime(ninetyDaysAgoMs);
    assertTrue(strategyWithFallback.isPurgeable(tableNameWithType, seg2));

    // Case 3: Invalid end time, fallback ENABLED, recent creation time → NOT purgeable
    SegmentZKMetadata seg3 = new SegmentZKMetadata("seg_fallback_recent_creation");
    seg3.setStatus(Status.DONE);
    seg3.setCreationTime(tenDaysAgoMs);
    assertFalse(strategyWithFallback.isPurgeable(tableNameWithType, seg3));

    // Case 4: Invalid end time, fallback ENABLED, creation time also invalid (-1) → NOT purgeable
    SegmentZKMetadata seg4 = new SegmentZKMetadata("seg_fallback_invalid_creation");
    seg4.setStatus(Status.DONE);
    // creation time defaults to -1 when not set
    assertFalse(strategyWithFallback.isPurgeable(tableNameWithType, seg4));

    // Case 4b: Invalid end time, fallback ENABLED, creation time is 0 (the motivating scenario) → NOT purgeable
    SegmentZKMetadata seg4b = new SegmentZKMetadata("seg_fallback_zero_creation");
    seg4b.setStatus(Status.DONE);
    seg4b.setCreationTime(0L);
    assertFalse(strategyWithFallback.isPurgeable(tableNameWithType, seg4b));

    // Case 5: Valid end time, fallback ENABLED — should use end time, NOT creation time
    SegmentZKMetadata seg5 = new SegmentZKMetadata("seg_valid_endtime_with_fallback");
    seg5.setStatus(Status.DONE);
    seg5.setTimeUnit(TimeUnit.MILLISECONDS);
    seg5.setEndTime(tenDaysAgoMs);
    seg5.setCreationTime(ninetyDaysAgoMs);
    // End time is 10 days ago (within 30-day retention), creation time is 90 days ago
    // Should use end time → NOT purgeable
    assertFalse(strategyWithFallback.isPurgeable(tableNameWithType, seg5));

    // Case 6: Valid end time beyond retention, fallback ENABLED — should use end time
    SegmentZKMetadata seg6 = new SegmentZKMetadata("seg_old_endtime_with_fallback");
    seg6.setStatus(Status.DONE);
    seg6.setTimeUnit(TimeUnit.MILLISECONDS);
    seg6.setEndTime(ninetyDaysAgoMs);
    seg6.setCreationTime(tenDaysAgoMs);
    // End time is 90 days ago (beyond 30-day retention), even though creation time is recent
    // Should use end time → PURGEABLE
    assertTrue(strategyWithFallback.isPurgeable(tableNameWithType, seg6));
  }
}
