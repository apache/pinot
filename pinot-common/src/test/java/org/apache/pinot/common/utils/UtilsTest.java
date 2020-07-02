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
import org.apache.pinot.common.Utils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


/**
 * Tests for the Utils classes.
 */
public class UtilsTest {

  @Test
  public void testToCamelCase() {
    assertEquals(Utils.toCamelCase("Hello world!"), "HelloWorld");
    assertEquals(Utils.toCamelCase("blah blah blah"), "blahBlahBlah");
    assertEquals(Utils.toCamelCase("the quick __--???!!! brown   fox?"), "theQuickBrownFox");
  }

  @Test
  public void testTimeUtils() {
    // Test all time units
    for (TimeUnit timeUnit : TimeUnit.values()) {
      assertEquals(TimeUtils.timeUnitFromString(timeUnit.name()), timeUnit);
      assertEquals(TimeUtils.timeUnitFromString(timeUnit.name().toLowerCase()), timeUnit);
    }

    // Test other time unit string
    assertEquals(TimeUtils.timeUnitFromString("daysSinceEpoch"), TimeUnit.DAYS);
    assertEquals(TimeUtils.timeUnitFromString("HOURSSINCEEPOCH"), TimeUnit.HOURS);
    assertEquals(TimeUtils.timeUnitFromString("MinutesSinceEpoch"), TimeUnit.MINUTES);
    assertEquals(TimeUtils.timeUnitFromString("SeCoNdSsInCeEpOcH"), TimeUnit.SECONDS);
    assertEquals(TimeUtils.timeUnitFromString("millissinceepoch"), TimeUnit.MILLISECONDS);
    assertEquals(TimeUtils.timeUnitFromString("MILLISECONDSSINCEEPOCH"), TimeUnit.MILLISECONDS);
    assertEquals(TimeUtils.timeUnitFromString("microssinceepoch"), TimeUnit.MICROSECONDS);
    assertEquals(TimeUtils.timeUnitFromString("MICROSECONDSSINCEEPOCH"), TimeUnit.MICROSECONDS);
    assertEquals(TimeUtils.timeUnitFromString("nanossinceepoch"), TimeUnit.NANOSECONDS);
    assertEquals(TimeUtils.timeUnitFromString("NANOSECONDSSINCEEPOCH"), TimeUnit.NANOSECONDS);

    // Test null and empty time unit string
    assertNull(TimeUtils.timeUnitFromString(null));
    assertNull(TimeUtils.timeUnitFromString(""));

    // Test invalid time unit string
    try {
      TimeUtils.timeUnitFromString("unknown");
      fail();
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testFlushThresholdTimeConversion() {
    Long millis = TimeUtils.convertPeriodToMillis("8d");
    assertEquals(millis.longValue(), 8 * 24 * 60 * 60 * 1000L);
    millis = TimeUtils.convertPeriodToMillis("8d6h");
    assertEquals(millis.longValue(), 8 * 24 * 60 * 60 * 1000L + 6 * 60 * 60 * 1000L);
    millis = TimeUtils.convertPeriodToMillis("8d10m");
    assertEquals(millis.longValue(), 8 * 24 * 60 * 60 * 1000L + 10 * 60 * 1000L);
    millis = TimeUtils.convertPeriodToMillis("6h");
    assertEquals(millis.longValue(), 6 * 60 * 60 * 1000L);
    millis = TimeUtils.convertPeriodToMillis("6h30m");
    assertEquals(millis.longValue(), 6 * 60 * 60 * 1000L + 30 * 60 * 1000);
    millis = TimeUtils.convertPeriodToMillis("50m");
    assertEquals(millis.longValue(), 50 * 60 * 1000L);
    millis = TimeUtils.convertPeriodToMillis("10s");
    assertEquals(millis.longValue(), 10 * 1000L);
    millis = TimeUtils.convertPeriodToMillis(null);
    assertEquals(millis.longValue(), 0);
    millis = TimeUtils.convertPeriodToMillis("-1d");
    assertEquals(millis.longValue(), -86400000L);
    try {
      millis = TimeUtils.convertPeriodToMillis("hhh");
      Assert.fail("Expected exception to be thrown while converting an invalid input string");
    } catch (IllegalArgumentException e) {
      // expected
    }

    String periodStr = TimeUtils.convertMillisToPeriod(10 * 1000L);
    assertEquals(periodStr, "10s");
    periodStr = TimeUtils.convertMillisToPeriod(50 * 60 * 1000L);
    assertEquals(periodStr, "50m");
    periodStr = TimeUtils.convertMillisToPeriod(50 * 60 * 1000L + 30 * 1000);
    assertEquals(periodStr, "50m30s");
    periodStr = TimeUtils.convertMillisToPeriod(6 * 60 * 60 * 1000L);
    assertEquals(periodStr, "6h");
    periodStr = TimeUtils.convertMillisToPeriod(6 * 60 * 60 * 1000L + 20 * 60 * 1000 + 10 * 1000);
    assertEquals(periodStr, "6h20m10s");
    periodStr = TimeUtils.convertMillisToPeriod(0L);
    assertEquals(periodStr, "0s");
    periodStr = TimeUtils.convertMillisToPeriod(-1L);
    assertEquals(periodStr, "");
    periodStr = TimeUtils.convertMillisToPeriod(null);
    assertEquals(periodStr, null);
  }
}
