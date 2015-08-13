package com.linkedin.thirdeye.anomaly.util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;

/**
 *
 */
public class DimensionKeyUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionKeyUtils.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * @param dimensionKey
   * @return
   *  The number of star fields in the dimension key
   */
  public static int getStarCount(DimensionKey dimensionKey) {
    int count = 0;
    for (String dimension : dimensionKey.getDimensionValues()) {
      if ("*".equals(dimension)) {
        count++;
      }
    }
    return count;
  }

  /**
   * @param dimensions
   * @param dimensionKey
   * @return
   *  A map of the dimension key
   */
  public static Map<String, String>  toMap(List<DimensionSpec> dimensions, DimensionKey dimensionKey) {
    Map<String, String> dimensionMap = new TreeMap<>();
    String[] dimensionValues = dimensionKey.getDimensionValues();
    for (int i = 0; i < dimensions.size(); i++) {
      dimensionMap.put(dimensions.get(i).getName(), dimensionValues[i]);
    }
    return dimensionMap;
  }

  /**
   * @param d1
   * @param d2
   * @return
   *  Whether d2 is contained within d1.
   */
  public static boolean isContainedWithin(DimensionKey d1, DimensionKey d2) {
    String[] d1Values = d1.getDimensionValues();
    String[] d2Values = d2.getDimensionValues();
    if (d1Values.length != d2Values.length) {
      LOGGER.error("dimensions do not match in num fields");
    }
    for (int i = 0; i < d1Values.length; i++) {
      if (d1Values[i].equals(d2Values[i])) {
        continue;
      } else if (d1Values[i].equals("*")) {
        continue;
      } else {
        return false;
      }
    }
    return true;
  }

  private DimensionKeyUtils() {}

}
