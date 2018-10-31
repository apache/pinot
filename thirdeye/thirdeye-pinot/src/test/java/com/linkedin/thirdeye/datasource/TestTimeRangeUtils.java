package com.linkedin.thirdeye.datasource;

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
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datasource.TimeRangeUtils;

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
