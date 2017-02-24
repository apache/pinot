package com.linkedin.thirdeye.anomalydetection;

import java.util.concurrent.TimeUnit;

public class AnomalyDetectionUtils {
  /**
   * Returns the given bucket size and bucket unit to the bucket size in milliseconds.
   * @param bucketSize the number of the units.
   * @param bucketUnit the unit for computing the bucket size in milliseconds.
   * @return the bucket size in milliseconds.
   */
  public static long getBucketInMillis(int bucketSize, TimeUnit bucketUnit) {
    return bucketUnit.toMillis(bucketSize);
  }
}
