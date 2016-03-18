package com.linkedin.thirdeye.client.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class TestThirdEyeClientUtils {
  private static final TimeGranularity DEFAULT_TIME_GRANULARITY =
      new TimeGranularity(1, TimeUnit.HOURS);
  private static final TimeGranularity TEN_MINUTE_TIME_GRANULARITY =
      new TimeGranularity(10, TimeUnit.MINUTES);
  private static final DateTime DEFAULT_START =
      ISODateTimeFormat.dateTimeParser().parseDateTime("2016-01-01T00:00:00.000+00:00");
  private static final DateTime DEFAULT_END =
      ISODateTimeFormat.dateTimeParser().parseDateTime("2016-01-02T00:00:00.000+00:00");

  @Test(dataProvider = "timeBucketCountProvider")
  public void getTimeBucketCount(ThirdEyeRequest request, TimeGranularity dataTimeGranularity,
      long expected) {
    long timeBucketCount = ThirdEyeClientUtils.getTimeBucketCount(request, dataTimeGranularity);
    Assert.assertEquals(timeBucketCount, expected);
  }

  @Test(dataProvider = "timeBucketCountProvider")
  public void getTimestamps(ThirdEyeRequest request, TimeGranularity dataTimeGranularity,
      long expectedCount) {
    List<String> timestamps = ThirdEyeClientUtils.getTimestamps(request, dataTimeGranularity);
    Assert.assertEquals(timestamps.size(), expectedCount);
    List<String> sorted = new ArrayList<>(timestamps);
    Collections.sort(sorted);
    Assert.assertEquals(timestamps, sorted);
  }

  @DataProvider(name = "timeBucketCountProvider")
  private Object[][] timeBucketCountProvider() {
    long diffMillis = DEFAULT_END.getMillis() - DEFAULT_START.getMillis();
    return new Object[][] {
        new Object[] {
            buildRequest(DEFAULT_START, DEFAULT_END, false), DEFAULT_TIME_GRANULARITY, 1
        }, new Object[] {
            buildRequest(DEFAULT_START, DEFAULT_END, true), DEFAULT_TIME_GRANULARITY,
            DEFAULT_TIME_GRANULARITY.convertToUnit(diffMillis)
        }, new Object[] {
            buildRequest(DEFAULT_START, DEFAULT_END, true), TEN_MINUTE_TIME_GRANULARITY,
            TEN_MINUTE_TIME_GRANULARITY.convertToUnit(diffMillis)
        }
    };

  }

  private ThirdEyeRequest buildRequest(DateTime start, DateTime end, boolean shouldGroupByTime) {
    return ThirdEyeRequest.newBuilder().setStartTimeInclusive(start).setEndTime(end)
        .setShouldGroupByTime(shouldGroupByTime).build();
  }
}
