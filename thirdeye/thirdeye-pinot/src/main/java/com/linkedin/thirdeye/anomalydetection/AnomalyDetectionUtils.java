package com.linkedin.thirdeye.anomalydetection;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

  private static Splitter SEMICOLON_SPLITTER = Splitter.on(";").omitEmptyStrings();
  private static Splitter EQUALS_SPLITTER = Splitter.on("=").omitEmptyStrings();
  private static Joiner SEMICOLON = Joiner.on(";");
  private static Joiner EQUALS = Joiner.on("=");

  /**
   * Utility class to encode properties to string in format key1=value1;key2=value2
   * @param props : the property to be encoded
   * @return String of encoded property
   */
  public static String encodeCompactedProperties(Properties props) {
    List<String> parts = new ArrayList<>();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      parts.add(EQUALS.join(entry.getKey(), entry.getValue()));
    }
    return SEMICOLON.join(parts);
  }

  /**
   * Decode properties string which is encoded by encodeCompactedProperties to into a Hashmap
   * @param propStr: property string which is encoded using encodeCompactedProperties
   * @return
   */
  public static Map<String, String> decodeCompactedPropertyStringToMap(String propStr) {
    Map<String, String> props = new HashMap<>();
    if(propStr != null) {
      for (String part : SEMICOLON_SPLITTER.split(propStr)) {
        List<String> kvPair = EQUALS_SPLITTER.splitToList(part);
        props.put(kvPair.get(0), kvPair.get(1));
      }
    }
    return props;
  }

}
