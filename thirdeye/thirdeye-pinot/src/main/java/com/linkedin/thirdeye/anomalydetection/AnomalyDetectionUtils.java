package com.linkedin.thirdeye.anomalydetection;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AnomalyDetectionUtils {
  /**
   * Returns the bucket size of time series in millis given the keys to retrieve the values from the
   * properties.
   *
   * @param bucketSizeKey the key to retrieve the parameter for bucket size.
   * @param bucketUnitKey the key to retrieve the parameter for bucket unit.
   * @param properties the properties that contains the parameters.
   * @return the bucket size in millis.
   */
  public static long getBucketInMillis(String bucketSizeKey, String bucketUnitKey, Properties properties) {
    int bucketSize = Integer.valueOf(properties.getProperty(bucketSizeKey));
    TimeUnit bucketUnit = TimeUnit.valueOf(properties.getProperty(bucketUnitKey));
    return bucketUnit.toMillis(bucketSize);
  }
}
