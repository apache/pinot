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
package org.apache.pinot.segment.spi.creator.name;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class NormalizedDateSegmentNameGeneratorTest {
  private static final String TABLE_NAME = "myTable";
  private static final String MALFORMED_TABLE_NAME = "my/Table";
  private static final String SEGMENT_NAME_PREFIX = "myTable_daily";
  private static final String SEGMENT_NAME_POSTFIX = "myPostfix";
  private static final String MALFORMED_SEGMENT_NAME_PREFIX = "myTable\\daily";
  private static final String MALFORMED_SEGMENT_NAME_POSTFIX = "my\\postfix";
  private static final String APPEND_PUSH_TYPE = "APPEND";
  private static final String REFRESH_PUSH_TYPE = "REFRESH";
  private static final String LONG_SIMPLE_DATE_FORMAT = "yyyyMMdd";
  private static final String STRING_SIMPLE_DATE_FORMAT = "yyyy-MM-dd";
  private static final String STRING_SLASH_DATE_FORMAT = "yyyy/MM/dd";
  private static final String DAILY_PUSH_FREQUENCY = "daily";
  private static final String HOURLY_PUSH_FREQUENCY = "hourly";
  private static final int INVALID_SEQUENCE_ID = -1;
  private static final int VALID_SEQUENCE_ID = 1;

  @Test
  public void testRefresh() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, REFRESH_PUSH_TYPE, null, null, null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_1");
  }

  @Test
  public void testWithSegmentNamePrefix() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, false, REFRESH_PUSH_TYPE, null, null,
            null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, appendPushType=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable_daily");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_daily_1");
  }

  @Test
  public void testWithSegmentNamePrefixPostfix() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, false, REFRESH_PUSH_TYPE, null, null,
            SEGMENT_NAME_POSTFIX);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, segmentNamePostfix=myPostfix, "
            + "appendPushType=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable_daily_myPostfix");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_daily_myPostfix_1");
  }

  @Test
  public void testWithUntrimmedSegmentNamePrefix() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX + "  ", false, REFRESH_PUSH_TYPE, null,
            null, null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, appendPushType=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable_daily");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_daily_1");
  }

  @Test
  public void testWithUntrimmedSegmentNamePrefixPostfix() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX + "  ", false, REFRESH_PUSH_TYPE, null,
            null, SEGMENT_NAME_POSTFIX + "  ");
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, segmentNamePostfix=myPostfix, "
            + "appendPushType=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable_daily_myPostfix");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_daily_myPostfix_1");
  }

  @Test
  public void testExcludeSequenceId() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, true, REFRESH_PUSH_TYPE, null, null, null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=false, excludeSequenceId=true");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable");
  }

  @Test
  public void testWithPrefixExcludeSequenceId() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, true, REFRESH_PUSH_TYPE, null, null,
            null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, appendPushType=false, "
            + "excludeSequenceId=true");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable_daily");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_daily");
  }

  @Test
  public void testWithPrefixPostfixExcludeSequenceId() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, true, REFRESH_PUSH_TYPE, null, null,
            SEGMENT_NAME_POSTFIX);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, segmentNamePostfix=myPostfix, "
            + "appendPushType=false, excludeSequenceId=true");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "myTable_daily_myPostfix");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "myTable_daily_myPostfix");
  }

  @Test
  public void testAppend() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forEpoch(TimeUnit.DAYS.name()), null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=true, outputSDF=yyyy-MM-dd, "
            + "inputTimeUnit=DAYS");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, 1L, 3L),
        "myTable_1970-01-02_1970-01-04");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, 1L, 3L),
        "myTable_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testAppendWithSegmentNamePrefix() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, false, APPEND_PUSH_TYPE,
            DAILY_PUSH_FREQUENCY, DateTimeFormatSpec.forEpoch(TimeUnit.DAYS.name()), null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, appendPushType=true, "
            + "outputSDF=yyyy-MM-dd, inputTimeUnit=DAYS");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, 1L, 3L),
        "myTable_daily_1970-01-02_1970-01-04");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, 1L, 3L),
        "myTable_daily_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testAppendWithSegmentNamePrefixPostfix() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, false, APPEND_PUSH_TYPE,
            DAILY_PUSH_FREQUENCY, DateTimeFormatSpec.forEpoch(TimeUnit.DAYS.name()), SEGMENT_NAME_POSTFIX);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable_daily, segmentNamePostfix=myPostfix, "
            + "appendPushType=true, outputSDF=yyyy-MM-dd, inputTimeUnit=DAYS");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, 1L, 3L),
        "myTable_daily_1970-01-02_1970-01-04_myPostfix");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, 1L, 3L),
        "myTable_daily_1970-01-02_1970-01-04_myPostfix_1");
  }

  @Test
  public void testHoursTimeType() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forEpoch(TimeUnit.HOURS.name()), null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=true, outputSDF=yyyy-MM-dd, "
            + "inputTimeUnit=HOURS");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, 24L, 72L),
        "myTable_1970-01-02_1970-01-04");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, 24L, 72L),
        "myTable_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testLongSimpleDateFormat() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forSimpleDateFormat(LONG_SIMPLE_DATE_FORMAT), null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=true, outputSDF=yyyy-MM-dd, "
            + "inputSDF=yyyyMMdd");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, 19700102L, 19700104L),
        "myTable_1970-01-02_1970-01-04");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, 19700102L, 19700104L),
        "myTable_1970-01-02_1970-01-04_1");
  }

  @Test
  public void testStringSimpleDateFormat() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forSimpleDateFormat(STRING_SIMPLE_DATE_FORMAT), null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=true, outputSDF=yyyy-MM-dd, "
            + "inputSDF=yyyy-MM-dd");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, "1970-01-02", "1970-01-04"),
        "myTable_1970-01-02_1970-01-04");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, "1970-01-02", "1970-01-04"),
        "myTable_1970-01-02_1970-01-04_1");

    // The following date time format will work with Joda time library and not java.text
    segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZ"), null);
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, "2023-01-01T12:00:00.111111111Z",
        "2023-01-02T12:00:00.111111111Z"), "myTable_2023-01-01_2023-01-02_1");
  }

  @Test
  public void testMalFormedTableNameAndSegmentNamePrefixPostfix() {
    DateTimeFormatSpec dateTimeFormatSpec = DateTimeFormatSpec.forSimpleDateFormat(STRING_SLASH_DATE_FORMAT);
    try {
      new NormalizedDateSegmentNameGenerator(MALFORMED_TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
          dateTimeFormatSpec, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new NormalizedDateSegmentNameGenerator(TABLE_NAME, MALFORMED_SEGMENT_NAME_PREFIX, false, APPEND_PUSH_TYPE,
          DAILY_PUSH_FREQUENCY, dateTimeFormatSpec, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new NormalizedDateSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_PREFIX, false, APPEND_PUSH_TYPE,
          DAILY_PUSH_FREQUENCY, dateTimeFormatSpec, MALFORMED_SEGMENT_NAME_POSTFIX);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Missing minTimeValue"
      + " for NormalizedDateSegmentNameGenerator")
  public void testMalFormedDateFormatAndTimeValue() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, DAILY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forSimpleDateFormat(STRING_SLASH_DATE_FORMAT), null);
    assertEquals(segmentNameGenerator.toString(), "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, "
        + "appendPushType=true, outputSDF=yyyy-MM-dd, inputSDF=yyyy/MM/dd");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, "1970/01/02", "1970/01/04"),
        "myTable_1970-01-02_1970-01-04");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, "1970/01/02", "1970/01/04"),
        "myTable_1970-01-02_1970-01-04_1");
    segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, "1970/01/04");
  }

  @Test
  public void testHourlyPushFrequency() {
    SegmentNameGenerator segmentNameGenerator =
        new NormalizedDateSegmentNameGenerator(TABLE_NAME, null, false, APPEND_PUSH_TYPE, HOURLY_PUSH_FREQUENCY,
            DateTimeFormatSpec.forEpoch(TimeUnit.DAYS.name()), null);
    assertEquals(segmentNameGenerator.toString(),
        "NormalizedDateSegmentNameGenerator: segmentNamePrefix=myTable, appendPushType=true, outputSDF=yyyy-MM-dd-HH,"
            + " inputTimeUnit=DAYS");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, 1L, 3L),
        "myTable_1970-01-02-00_1970-01-04-00");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, 1L, 3L),
        "myTable_1970-01-02-00_1970-01-04-00_1");
  }
}
