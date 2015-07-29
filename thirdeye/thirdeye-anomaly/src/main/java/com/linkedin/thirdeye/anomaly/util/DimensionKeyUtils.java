package com.linkedin.thirdeye.anomaly.util;

import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;

/**
 *
 */
public class DimensionKeyUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionKeyUtils.class);

  /**
   * @param dimensionKey
   * @return
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
   */
  public static String toJsonString(List<DimensionSpec> dimensions, DimensionKey dimensionKey) {
    JSONObject json = new JSONObject();
    String[] dimensionValues = dimensionKey.getDimensionValues();
    for (int i = 0; i < dimensions.size(); i++) {
      try {
        json.put(dimensions.get(i).getName(), dimensionValues[i]);
      } catch (JSONException e) {
        LOGGER.error("json key error {}", e);
      }
    }
    return json.toString();
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

}
