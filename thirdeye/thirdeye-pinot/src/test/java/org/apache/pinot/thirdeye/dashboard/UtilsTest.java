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

package org.apache.pinot.thirdeye.dashboard;

import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {
  @Test
  public void testResizeTimeGranularity() {
    // Test if duration is not dividable by time granularity
    long duration = TimeUnit.DAYS.toMillis(1) + 36000;
    String timeGranularity = "HOURS";
    timeGranularity = Utils.resizeTimeGranularity(duration, timeGranularity, 12);
    Assert.assertEquals(timeGranularity, "3_HOURS");

    // Test if duration is dividable by time granularity, but data point is limited
    duration = TimeUnit.DAYS.toMillis(1);
    timeGranularity = "HOURS";
    timeGranularity = Utils.resizeTimeGranularity(duration, timeGranularity, 12);
    Assert.assertEquals(timeGranularity, "2_HOURS");

    // Test if duration is dividable by time granularity and data point is not reduced
    duration = TimeUnit.DAYS.toMillis(1);
    timeGranularity = "HOURS";
    timeGranularity = Utils.resizeTimeGranularity(duration, timeGranularity, 24);
    Assert.assertEquals(timeGranularity, "HOURS");

    // Test if the given time granularity is large enough
    duration = TimeUnit.DAYS.toMillis(1);
    timeGranularity = "2_HOURS";
    timeGranularity = Utils.resizeTimeGranularity(duration, timeGranularity, 24);
    Assert.assertEquals(timeGranularity, "2_HOURS");

    // Test different time unit than HOURS
    duration = TimeUnit.DAYS.toMillis(1);
    timeGranularity = "MINUTES";
    timeGranularity = Utils.resizeTimeGranularity(duration, timeGranularity, 12);
    Assert.assertEquals(timeGranularity, "120_MINUTES");
  }
}
