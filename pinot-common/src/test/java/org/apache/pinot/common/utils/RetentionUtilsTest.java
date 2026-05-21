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

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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

  private static SegmentZKMetadata makeSegmentWithCreationTime(long endTimeMs, long creationTimeMs) {
    SegmentZKMetadata segment = new SegmentZKMetadata("seg");
    segment.setEndTime(endTimeMs);
    segment.setTimeUnit(TimeUnit.MILLISECONDS);
    segment.setCreationTime(creationTimeMs);
    return segment;
  }

  private static SegmentMetadata mockSegmentMetadataMillis(long endTimeRaw, long indexCreationTimeMs) {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getName()).thenReturn("seg");
    Mockito.when(metadata.getTimeInterval()).thenReturn(null);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Mockito.when(metadata.getEndTime()).thenReturn(endTimeRaw);
    Mockito.when(metadata.getIndexCreationTime()).thenReturn(indexCreationTimeMs);
    return metadata;
  }

  @Test
  public void getSegmentMetadataEndTimeMillisFromTimeUnitAndEndTime() {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Mockito.when(metadata.getEndTime()).thenReturn(5000L);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), 5000L);
  }

  @Test
  public void getSegmentMetadataEndTimeMillisConvertsWithTimeUnit() {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.DAYS);
    Mockito.when(metadata.getEndTime()).thenReturn(2L);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), TimeUnit.DAYS.toMillis(2));
  }

  @Test
  public void getSegmentMetadataEndTimeMillisUnsetReturnsRawEndTime() {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeUnit()).thenReturn(null);
    Mockito.when(metadata.getEndTime()).thenReturn(Long.MIN_VALUE);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), Long.MIN_VALUE);
  }

  @Test
  public void getSegmentMetadataEndTimeMillisNullTimeUnitReturnsRawEndTime() {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeUnit()).thenReturn(null);
    Mockito.when(metadata.getEndTime()).thenReturn(12_345L);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), 12_345L);
  }

  @Test
  public void getSegmentMetadataEndTimeMillisMinRawEndTimeReturnsMinWithoutConversion() {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.DAYS);
    Mockito.when(metadata.getEndTime()).thenReturn(Long.MIN_VALUE);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), Long.MIN_VALUE);
  }

  @Test
  public void getSegmentMetadataEndTimeMillisDoesNotReadTimeInterval() {
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.DAYS);
    Mockito.when(metadata.getEndTime()).thenReturn(1L);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), TimeUnit.DAYS.toMillis(1));
    verify(metadata, never()).getTimeInterval();
  }

  @Test
  public void getSegmentMetadataEndTimeMillisIgnoresTimeIntervalWhenRawFieldsDiffer() {
    Interval interval = new Interval(1000L, 9000L, DateTimeZone.UTC);
    SegmentMetadata metadata = Mockito.mock(SegmentMetadata.class);
    Mockito.when(metadata.getTimeInterval()).thenReturn(interval);
    Mockito.when(metadata.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Mockito.when(metadata.getEndTime()).thenReturn(5000L);
    clearInvocations(metadata);
    Assert.assertEquals(RetentionUtils.getSegmentMetadataEndTimeMillis(metadata), 5000L);
    verify(metadata, never()).getTimeInterval();
  }

  @Test
  public void testExpiredSegmentIsPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - 10 * ONE_DAY_MS);
    assertTrue(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testRecentSegmentIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - 2 * ONE_DAY_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testExactBoundaryIsNotPurgeable() {
    // strict greater-than: segment at exactly retentionMs old should NOT be purgeable
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - RETENTION_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testOneMsPastBoundaryIsPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegment(now - RETENTION_MS - 1);
    assertTrue(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testInvalidEndTimeIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = new SegmentZKMetadata("seg");
    segment.setEndTime(-1);
    segment.setTimeUnit(TimeUnit.MILLISECONDS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testFarFutureEndTimeIsNotPurgeable() {
    long now = System.currentTimeMillis();
    long farFuture = now + TimeUnit.DAYS.toMillis(365 * 200L);
    SegmentZKMetadata segment = makeSegment(farFuture);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testInvalidEndTimeFallbackDisabledIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithCreationTime(-1, now - 10 * ONE_DAY_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, false));
  }

  @Test
  public void testInvalidEndTimeOldCreationTimeFallbackEnabledIsPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithCreationTime(-1, now - 10 * ONE_DAY_MS);
    assertTrue(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, true));
  }

  @Test
  public void testInvalidEndTimeRecentCreationTimeFallbackEnabledIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithCreationTime(-1, now - 2 * ONE_DAY_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, true));
  }

  @Test
  public void testInvalidEndTimeInvalidCreationTimeFallbackEnabledIsNotPurgeable() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithCreationTime(-1, -1);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, true));
  }

  @Test
  public void testValidEndTimeTakesPriorityOverCreationTimeFallback() {
    long now = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithCreationTime(now - 2 * ONE_DAY_MS, now - 10 * ONE_DAY_MS);
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, segment, RETENTION_MS, now, true));
  }

  @Test
  public void testSegmentMetadataPurgeableExactBoundaryNotOutside() {
    long now = System.currentTimeMillis();
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, mockSegmentMetadataMillis(now - RETENTION_MS, 0L),
        RETENTION_MS, now, false));
  }

  @Test
  public void testSegmentMetadataInvalidEndNoFallback() {
    long now = System.currentTimeMillis();
    assertFalse(RetentionUtils.isPurgeable(TABLE_NAME, mockSegmentMetadataMillis(-1, now - 10 * ONE_DAY_MS),
        RETENTION_MS, now, false));
  }

  @Test
  public void testSegmentMetadataInvalidEndWithFallback() {
    long now = System.currentTimeMillis();
    long creation = now - 10 * ONE_DAY_MS;
    assertTrue(RetentionUtils.isPurgeable(TABLE_NAME, mockSegmentMetadataMillis(-1, creation),
        RETENTION_MS, now, true));
  }

  @Test
  public void shouldManageTimeBasedDataRetentionRealtime() {
    TableConfig cfg = new TableConfigBuilder(TableType.REALTIME).setTableName("t").build();
    assertTrue(RetentionUtils.shouldManageTimeBasedDataRetention(cfg));
  }

  @Test
  public void shouldManageTimeBasedDataRetentionOfflineAppendDefault() {
    TableConfig cfg = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE").build();
    assertTrue(RetentionUtils.shouldManageTimeBasedDataRetention(cfg));
  }

  @Test
  public void shouldManageTimeBasedDataRetentionOfflineRefreshSkipped() {
    TableConfig cfg = new TableConfigBuilder(TableType.OFFLINE).setTableName("t_OFFLINE").setSegmentPushType("REFRESH")
        .build();
    assertFalse(RetentionUtils.shouldManageTimeBasedDataRetention(cfg));
  }

  @Test
  public void testParseTableDataRetentionMillisValidDays() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("DAYS");
    cfg.setRetentionTimeValue("7");
    Assert.assertTrue(RetentionUtils.parseTableDataRetentionMillis(cfg).isPresent());
    Assert.assertEquals(RetentionUtils.parseTableDataRetentionMillis(cfg).getAsLong(), TimeUnit.DAYS.toMillis(7));
  }

  @Test
  public void testParseTableDataRetentionMillisMissingReturnsEmpty() {
    assertFalse(RetentionUtils.parseTableDataRetentionMillis(new SegmentsValidationAndRetentionConfig()).isPresent());
    assertFalse(RetentionUtils.parseTableDataRetentionMillis(null).isPresent());
  }

  @Test
  public void testParseTableDataRetentionMillisInvalidUnitReturnsEmpty() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("FORTNIGHTS");
    cfg.setRetentionTimeValue("1");
    assertFalse(RetentionUtils.parseTableDataRetentionMillis(cfg).isPresent());
  }

  @Test
  public void testParseTableDataRetentionMillisEmptyUnitReturnsEmpty() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("");
    cfg.setRetentionTimeValue("7");
    assertFalse(RetentionUtils.parseTableDataRetentionMillis(cfg).isPresent());
  }

  @Test
  public void testParseTableDataRetentionMillisEmptyValueReturnsEmpty() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("DAYS");
    cfg.setRetentionTimeValue("");
    assertFalse(RetentionUtils.parseTableDataRetentionMillis(cfg).isPresent());
  }

  @Test
  public void testParseTableDataRetentionMillisInvalidValueReturnsEmpty() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("DAYS");
    cfg.setRetentionTimeValue("not-a-number");
    assertFalse(RetentionUtils.parseTableDataRetentionMillis(cfg).isPresent());
  }

  @Test
  public void testParseTableDataRetentionMillisLowerCaseUnitParses() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("days");
    cfg.setRetentionTimeValue("7");
    OptionalLong parsed = RetentionUtils.parseTableDataRetentionMillis(cfg);
    Assert.assertTrue(parsed.isPresent());
    Assert.assertEquals(parsed.getAsLong(), TimeUnit.DAYS.toMillis(7));
  }

  @Test
  public void testParseTableDataRetentionMillisValidHours() {
    SegmentsValidationAndRetentionConfig cfg = new SegmentsValidationAndRetentionConfig();
    cfg.setRetentionTimeUnit("HOURS");
    cfg.setRetentionTimeValue("12");
    OptionalLong parsed = RetentionUtils.parseTableDataRetentionMillis(cfg);
    Assert.assertTrue(parsed.isPresent());
    Assert.assertEquals(parsed.getAsLong(), TimeUnit.HOURS.toMillis(12));
  }
}
