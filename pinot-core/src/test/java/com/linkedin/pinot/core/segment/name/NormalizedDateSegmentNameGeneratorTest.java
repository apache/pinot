/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.name;

import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class NormalizedDateSegmentNameGeneratorTest {
  private static final String TABLE_NAME = "myTable";
  private static final int SEQUENCE_ID = 1;
  private static final String TIME_COLUMN_TYPE = "DAYS";
  private static final String TABLE_PUSH_FREQUENCY = "daily";
  private static final String PREFIX = "myTable_daily";
  private static final String APPEND_PUSH_TYPE = "APPEND";
  private static final String REFRESH_PUSH_TYPE = "REFRESH";

  @Test
  public void testAppend() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, TIME_COLUMN_TYPE, TABLE_PUSH_FREQUENCY,
            APPEND_PUSH_TYPE, null, null, DateTimeFieldSpec.TimeFormat.EPOCH.toString());
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass),
        "myTable_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testAppendWithPrefix() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, TIME_COLUMN_TYPE, TABLE_PUSH_FREQUENCY,
            APPEND_PUSH_TYPE, PREFIX, null, DateTimeFieldSpec.TimeFormat.EPOCH.toString());
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass),
        "myTable_daily_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testRefresh() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, null, TABLE_PUSH_FREQUENCY, REFRESH_PUSH_TYPE,
            null, null, null);
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass), "myTable_1");
  }

  @Test
  public void testNoSequenceIdWithParameter() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, null, TABLE_PUSH_FREQUENCY, REFRESH_PUSH_TYPE,
            null, "true", null);
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass), "myTable");
  }

  @Test
  public void testRefreshWithPrefixNoSequenceId() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, null, TABLE_PUSH_FREQUENCY, REFRESH_PUSH_TYPE,
            "prefix", "true", null);
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass), "prefix");
  }

  @Test
  public void testAppendWithPrefixNoSequenceId() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, TIME_COLUMN_TYPE, TABLE_PUSH_FREQUENCY,
            APPEND_PUSH_TYPE, "prefix", "true", DateTimeFieldSpec.TimeFormat.EPOCH.toString());
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass),
        "prefix_1970-01-02_1970-01-04");
  }

  @Test
  public void testMirrorShare() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, TIME_COLUMN_TYPE, TABLE_PUSH_FREQUENCY,
            APPEND_PUSH_TYPE, "mirrorShareEvents_daily", null, DateTimeFieldSpec.TimeFormat.EPOCH.toString());
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass),
        "mirrorShareEvents_daily_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testUntrimmedPrefix() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(3L);
    when(columnStatisticsClass.getMinValue()).thenReturn(1L);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, TIME_COLUMN_TYPE, TABLE_PUSH_FREQUENCY,
            APPEND_PUSH_TYPE, "mirrorShareEvents_daily  ", null, DateTimeFieldSpec.TimeFormat.EPOCH.toString());
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass),
        "mirrorShareEvents_daily_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testSimpleDateFormat() throws Exception {
    ColumnStatistics columnStatisticsClass = Mockito.mock(ColumnStatistics.class);
    when(columnStatisticsClass.getMaxValue()).thenReturn(19700104);
    when(columnStatisticsClass.getMinValue()).thenReturn(19700102);
    NormalizedDateSegmentNameGenerator normalizedDataSegmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEQUENCE_ID, TIME_COLUMN_TYPE, TABLE_PUSH_FREQUENCY,
            APPEND_PUSH_TYPE, "mirrorShareEvents_daily  ", null, "yyyyMMdd");
    Assert.assertEquals(normalizedDataSegmentNameGenerator.generateSegmentName(columnStatisticsClass),
        "mirrorShareEvents_daily_1970-01-02_1970-01-04_1");
  }
}
