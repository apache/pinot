package com.linkedin.thirdeye.anomaly.onboard.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class PropertyCheckUtils {
  /**
   * check if the list of property keys exist in the given properties
   * @param properties
   * @param propertyKeys
   */
  public static void checkNotNull(Map<String, String> properties, List<String> propertyKeys) {
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(propertyKeys);

    List<String> missingPropertyKeys = new ArrayList<>();

    for (String propertyKey : propertyKeys) {
      if (properties.get(propertyKey) == null) {
        missingPropertyKeys.add(propertyKey);
      }
    }

    if (!missingPropertyKeys.isEmpty()) {
      throw new IllegalArgumentException("Missing Property Keys: " + missingPropertyKeys);
    }
  }
}
