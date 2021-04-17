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
package org.apache.pinot.spi.data;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the conversion of a {@link TimeFieldSpec} to an equivalent {@link DateTimeFieldSpec}
 */
public class DateTimeFieldSpecUtilsTest {

  @Test
  public void testConversionFromTimeToDateTimeSpec() {
    TimeFieldSpec timeFieldSpec;
    DateTimeFieldSpec expectedDateTimeFieldSpec;
    DateTimeFieldSpec actualDateTimeFieldSpec;

    /* 1] only incoming */

    // incoming epoch millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // incoming epoch hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "incoming"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("incoming", DataType.INT, "1:HOURS:EPOCH", "1:HOURS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // Simple date format
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.INT, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // simple date format STRING
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.STRING, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyy-MM-dd hh-mm-ss", "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd hh-mm-ss", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // time unit size
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "incoming"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("incoming", DataType.LONG, "5:MINUTES:EPOCH", "5:MINUTES");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // transform function
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "incoming"));
    timeFieldSpec.setTransformFunction("toEpochHours(timestamp)");
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.INT, "1:HOURS:EPOCH", "1:HOURS", null, "toEpochHours(timestamp)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    /* 2] incoming + outgoing */

    // same incoming and outgoing
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "time"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "time"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("time", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // same incoming and outgoing - simple date format
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "time"),
            new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "time"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("time", DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // millis to hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", null, "toEpochHours(incoming)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // millis to bucketed minutes
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, 10, TimeUnit.MINUTES, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "10:MINUTES:EPOCH", "10:MINUTES", null,
        "toEpochMinutesBucket(incoming, 10)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // days to millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:MILLISECONDS:EPOCH",
        "1:MILLISECONDS", null, "fromEpochDays(incoming)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // bucketed minutes to millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:MILLISECONDS:EPOCH",
        "1:MILLISECONDS", null, "fromEpochMinutesBucket(incoming, 5)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // hours to days
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "incoming"),
        new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.INT, "1:DAYS:EPOCH", "1:DAYS", null,
        "toEpochDays(fromEpochHours(incoming))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // minutes to hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", null,
        "toEpochHours(fromEpochMinutes(incoming))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // bucketed minutes to days
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 10, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:DAYS:EPOCH", "1:DAYS", null,
        "toEpochDays(fromEpochMinutesBucket(incoming, 10))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // seconds to bucketed minutes
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "5:MINUTES:EPOCH", "5:MINUTES", null,
        "toEpochMinutesBucket(fromEpochSeconds(incoming), 5)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // simple date format to millis
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    try {
      Schema.convertToDateTimeFieldSpec(timeFieldSpec);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // hours to simple date format
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "incoming"),
        new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT:yyyyMMddhh", "outgoing"));
    try {
      Schema.convertToDateTimeFieldSpec(timeFieldSpec);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }
}
