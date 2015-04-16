package com.linkedin.thirdeye.dashboard.util;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;

import java.util.Map;
import java.util.TreeMap;

public class ViewUtils {
  public static Map<String, String> fillDimensionValues(CollectionSchema schema, Map<String, String> dimensionValues) {
    Map<String, String> filled = new TreeMap<>();
    for (String name : schema.getDimensions()) {
      String value = dimensionValues.get(name);
      if (value == null) {
        value = "*";
      }
      filled.put(name, value);
    }
    return filled;
  }
}
