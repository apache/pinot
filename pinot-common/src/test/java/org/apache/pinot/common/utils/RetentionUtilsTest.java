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
package org.apache.pinot.common.utils;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RetentionUtilsTest {

  private static final String TABLE_NAME = "testTable_REALTIME";
  private static final long ONE_DAY_MS = TimeUnit.DAYS.toMillis(1);
  private static final long RETENTION_MS = TimeUnit.DAYS.toMillis(7);

  private static SegmentZKMetadata makeSegment(long endTimeMs) {
    SegmentZKMetadata segment = new SegmentZKMetadata("seg");
    segment.setEndTime(endTimeMs);
    segment.setTimeUnit(TimeUnit.MILLISECONDS);
    return segment;
  }

  @Test
  public void testExpiredSegmentIsPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - 10 * ONE_DAY_MS);
    assertTrue(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now));
  }

  @Test
  public void testRecentSegmentIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - 2 * ONE_DAY_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now));
  }

  @Test
  public void testExactBoundaryIsNotPurgeable() {
    // strict greater-than: segment at exactly retentionMs old should NOT be purgeable
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - RETENTION_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now));
  }

  @Test
  public void testOneMsPastBoundaryIsPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - RETENTION_MS - 1);
    assertTrue(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now));
  }

  @Test
  public void testInvalidEndTimeIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = new SegmentZKMetadata("seg");
    segment.setEndTime(-1);
    segment.setTimeUnit(TimeUnit.MILLISECONDS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now));
  }

  @Test
  public void testFarFutureEndTimeIsNotPurgeable() {
    long now = System.currentTimeMillis();
    // end time 200 years in the future — outside valid range
    long farFuture = now + TimeUnit.DAYS.toMillis(365 * 200L);
    SegmentZKMetadata segment = makeSegment(farFuture);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now));
  }
}
