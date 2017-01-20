package com.linkedin.thirdeye.anomalydetection;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Utils {
  /**
   * Parses an anomaly function specification and returns the corresponding property object.
   *
   * @param spec the anomaly function spec
   * @return the corresponding property object
   * @throws IOException
   */
  public static Properties parseSpec(AnomalyFunctionDTO spec) throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }

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
