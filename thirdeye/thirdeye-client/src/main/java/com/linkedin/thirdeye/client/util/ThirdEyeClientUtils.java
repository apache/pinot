package com.linkedin.thirdeye.client.util;

import java.util.LinkedList;
import java.util.List;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class ThirdEyeClientUtils {

  public static long getTimeBucketCount(ThirdEyeRequest request,
      TimeGranularity dataTimeGranularity) {
    long timeBucketCount;
    if (!request.shouldGroupByTime()) {
      timeBucketCount = 1;
    } else {
      long duration =
          request.getEndTimeExclusive().getMillis() - request.getStartTimeInclusive().getMillis();
      timeBucketCount = dataTimeGranularity.convertToUnit(duration);
    }
    return timeBucketCount;
  }

  public static List<String> getTimestamps(ThirdEyeRequest request,
      TimeGranularity dataTimeGranularity) {
    long bucketMillis = dataTimeGranularity.toMillis();

    long startMillis = request.getStartTimeInclusive().getMillis();
    // round up to nearest aligned bucket
    startMillis = (startMillis + bucketMillis - 1) / bucketMillis * bucketMillis;
    List<String> timestamps = new LinkedList<>();
    if (!request.shouldGroupByTime()) {
      timestamps.add(Long.toString(startMillis));
    } else {
      long currentMillis = startMillis;
      long endMillis = request.getEndTimeExclusive().getMillis() / bucketMillis * bucketMillis;
      while (currentMillis < endMillis) { // end time is exclusive
        timestamps.add(Long.toString(currentMillis));
        currentMillis += bucketMillis;
      }
    }
    return timestamps;
  }

}
