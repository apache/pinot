package com.linkedin.thirdeye.client.util;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class TestThirdEyeClientUtils {
  private static final TimeSpec DEFAULT_TIME_SPEC = new TimeSpec("timeColumn",
      new TimeGranularity(5, TimeUnit.MILLISECONDS), new TimeGranularity(1, TimeUnit.HOURS), null);
  private static final DateTime DEFAULT_START =
      ISODateTimeFormat.dateTimeParser().parseDateTime("2016-01-01T00:00:00.000+00:00");
  private static final DateTime DEFAULT_END =
      ISODateTimeFormat.dateTimeParser().parseDateTime("2016-01-02T00:00:00.000+00:00");

  @Test(dataProvider = "timeBucketCountProvider")
  public void getTimeBucketCount(ThirdEyeRequest request, TimeSpec dataTimeSpec, int expected) {
    int timeBucketCount = ThirdEyeClientUtils.getTimeBucketCount(request, dataTimeSpec);
    Assert.assertEquals(timeBucketCount, expected);
  }

  @DataProvider(name = "timeBucketCountProvider")
  private Object[][] timeBucketCountProvider() {
    int numTimeBuckets = (int) DEFAULT_TIME_SPEC.getBucket().getUnit()
        .convert(DEFAULT_END.getMillis() - DEFAULT_START.getMillis(), TimeUnit.MILLISECONDS) + 1;
    return new Object[][] {
        new Object[] {
            buildRequest(DEFAULT_START, DEFAULT_END, false), DEFAULT_TIME_SPEC, 1
        }, new Object[] {
            buildRequest(DEFAULT_START, DEFAULT_END, true), DEFAULT_TIME_SPEC, numTimeBuckets
        }
    };

  }

  private ThirdEyeRequest buildRequest(DateTime start, DateTime end, boolean shouldGroupByTime) {
    return ThirdEyeRequest.newBuilder().setStartTime(start).setEndTime(end)
        .setShouldGroupByTime(shouldGroupByTime).build();
  }
}
