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
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Utils class.
 *
 */
public class UtilsTest {
  @Test
  public void testToCamelCase() {
    Assert.assertEquals(Utils.toCamelCase("Hello world!"), "HelloWorld");
    Assert.assertEquals(Utils.toCamelCase("blah blah blah"), "blahBlahBlah");
    Assert.assertEquals(Utils.toCamelCase("the quick __--???!!! brown   fox?"), "theQuickBrownFox");
  }

  @Test
  public void testTimeUtils() {
    Assert.assertEquals(TimeUtils.timeUnitFromString("days"), TimeUnit.DAYS);
    Assert.assertEquals(TimeUtils.timeUnitFromString("MINUTES"), TimeUnit.MINUTES);
    Assert.assertNull(TimeUtils.timeUnitFromString("daysSinceEpoch"));
    Assert.assertNull(TimeUtils.timeUnitFromString(null));
  }

  @Test
  public void testFlushThresholdTimeConversion() {

    Long millis = TimeUtils.convertPeriodToMillis("8d");
    Assert.assertEquals(millis.longValue(), 8*24*60*60*1000L);
    millis = TimeUtils.convertPeriodToMillis("8d6h");
    Assert.assertEquals(millis.longValue(), 8*24*60*60*1000L + 6*60*60*1000L);
    millis = TimeUtils.convertPeriodToMillis("8d10m");
    Assert.assertEquals(millis.longValue(), 8*24*60*60*1000L + 10*60*1000L);
    millis = TimeUtils.convertPeriodToMillis("6h");
    Assert.assertEquals(millis.longValue(), 6*60*60*1000L);
    millis = TimeUtils.convertPeriodToMillis("6h30m");
    Assert.assertEquals(millis.longValue(), 6*60*60*1000L + 30*60*1000);
    millis = TimeUtils.convertPeriodToMillis("50m");
    Assert.assertEquals(millis.longValue(), 50*60*1000L);
    millis = TimeUtils.convertPeriodToMillis("10s");
    Assert.assertEquals(millis.longValue(), 10*1000L);
    millis = TimeUtils.convertPeriodToMillis(null);
    Assert.assertEquals(millis.longValue(), 0);
    millis = TimeUtils.convertPeriodToMillis("-1d");
    Assert.assertEquals(millis.longValue(),  -86400000L);
    try {
      millis = TimeUtils.convertPeriodToMillis("hhh");
      Assert.fail("Expected exception to be thrown while converting an invalid input string");
    } catch (IllegalArgumentException e){
      // expected
    }

    String periodStr = TimeUtils.convertMillisToPeriod(10 * 1000L);
    Assert.assertEquals(periodStr, "10s");
    periodStr = TimeUtils.convertMillisToPeriod(50*60*1000L);
    Assert.assertEquals(periodStr, "50m");
    periodStr = TimeUtils.convertMillisToPeriod(50*60*1000L + 30*1000);
    Assert.assertEquals(periodStr, "50m30s");
    periodStr = TimeUtils.convertMillisToPeriod(6*60*60*1000L);
    Assert.assertEquals(periodStr, "6h");
    periodStr = TimeUtils.convertMillisToPeriod(6*60*60*1000L + 20*60*1000 + 10*1000);
    Assert.assertEquals(periodStr, "6h20m10s");
    periodStr = TimeUtils.convertMillisToPeriod(0L);
    Assert.assertEquals(periodStr, "0s");
    periodStr = TimeUtils.convertMillisToPeriod(-1L);
    Assert.assertEquals(periodStr, "");
    periodStr = TimeUtils.convertMillisToPeriod(null);
    Assert.assertEquals(periodStr, null);
  }
}
