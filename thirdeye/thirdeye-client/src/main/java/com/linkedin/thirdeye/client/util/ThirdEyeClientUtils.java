package com.linkedin.thirdeye.client.util;

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class ThirdEyeClientUtils {

  public static int getTimeBucketCount(ThirdEyeRequest request, TimeSpec dataTimeSpec) {
    int timeBucketCount;
    if (!request.shouldGroupByTime()) {
      timeBucketCount = 1;
    } else {
      long duration = request.getEndTime().getMillis() - request.getStartTime().getMillis();
      timeBucketCount =
          (int) dataTimeSpec.getBucket().getUnit().convert(duration, TimeUnit.MILLISECONDS) + 1;
    }
    return timeBucketCount;
  }

}
