/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.time.ThresholdPeriodUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThresholdPeriodUtilsTest {

  @Test
  public void testFlushThresholdTimeConversion() {

    Long millis = ThresholdPeriodUtils.convertToMillis("6h");
    Assert.assertEquals(millis.longValue(), 6*60*60*1000L);
    millis = ThresholdPeriodUtils.convertToMillis("6h30m");
    Assert.assertEquals(millis.longValue(), 6*60*60*1000L + 30*60*1000);
    millis = ThresholdPeriodUtils.convertToMillis("50m");
    Assert.assertEquals(millis.longValue(), 50*60*1000L);
    millis = ThresholdPeriodUtils.convertToMillis("10s");
    Assert.assertEquals(millis.longValue(), 10*1000L);
    millis = ThresholdPeriodUtils.convertToMillis(null);
    Assert.assertEquals(millis.longValue(), 0);
    boolean exception = false;
    try {
      millis = ThresholdPeriodUtils.convertToMillis("hhh");
    } catch (Exception e){
      exception = true;
      // Exception
    }
    Assert.assertTrue(exception);

    String periodStr = ThresholdPeriodUtils.convertToPeriod(10 * 1000L);
    Assert.assertEquals(periodStr, "10s");
    periodStr = ThresholdPeriodUtils.convertToPeriod(50*60*1000L);
    Assert.assertEquals(periodStr, "50m");
    periodStr = ThresholdPeriodUtils.convertToPeriod(50*60*1000L + 30*1000);
    Assert.assertEquals(periodStr, "50m30s");
    periodStr = ThresholdPeriodUtils.convertToPeriod(6*60*60*1000L);
    Assert.assertEquals(periodStr, "6h");
    periodStr = ThresholdPeriodUtils.convertToPeriod(6*60*60*1000L + 20*60*1000 + 10*1000);
    Assert.assertEquals(periodStr, "6h20m10s");
    periodStr = ThresholdPeriodUtils.convertToPeriod(0L);
    Assert.assertEquals(periodStr, "0s");
    periodStr = ThresholdPeriodUtils.convertToPeriod(null);
    Assert.assertEquals(periodStr, null);
  }

}
