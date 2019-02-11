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

package org.apache.pinot.thirdeye.datasource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Range;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;


public class TestTimeRangeUtils {

  @Test(dataProvider = "computeTimeRanges")
  public void computeTimeRanges(TimeGranularity granularity, DateTime start, DateTime end,
      List<Range<DateTime>> expected) {
    List<Range<DateTime>> actual = TimeRangeUtils.computeTimeRanges(granularity, start, end);
    Assert.assertEquals(actual, expected);
  }

  @DataProvider(name = "computeTimeRanges")
  public Object[][] provideComputeTimeRanges() {
    DateTime now = DateTime.now();
    DateTime yesterday = now.minusDays(1);
    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        null, yesterday, now, Collections.singletonList(Range.closedOpen(yesterday, now))
    });
    entries.add(new Object[] {
        new TimeGranularity(1, TimeUnit.DAYS), yesterday, now,
        Collections.singletonList(Range.closedOpen(yesterday, now))
    });
    entries.add(new Object[] {
        new TimeGranularity(6, TimeUnit.HOURS), yesterday, now,
        Arrays.asList(Range.closedOpen(yesterday, yesterday.plusHours(6)),
            Range.closedOpen(yesterday.plusHours(6), yesterday.plusHours(12)),
            Range.closedOpen(yesterday.plusHours(12), yesterday.plusHours(18)),
            Range.closedOpen(yesterday.plusHours(18), yesterday.plusHours(24)))
    });
    return entries.toArray(new Object[entries.size()][]);
  }
}
