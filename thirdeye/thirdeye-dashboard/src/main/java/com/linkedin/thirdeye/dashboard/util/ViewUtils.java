package com.linkedin.thirdeye.dashboard.util;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;

import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ViewUtils {
  private static final Joiner OR_JOINER = Joiner.on(" OR ");

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

  public static Map<String, String> flattenDisjunctions(MultivaluedMap<String, String> dimensionValues) {
    Map<String, String> flattened = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : dimensionValues.entrySet()) {
      flattened.put(entry.getKey(), OR_JOINER.join(entry.getValue()));
    }
    return flattened;
  }
}
