package com.linkedin.thirdeye.reporting.util;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.reporting.api.TableSpec;

public class DimensionKeyUtils {



  public static String createQueryKey(String[] filteredDimension) {
    StringBuilder sb = new StringBuilder();
    sb = sb.append("[\"");
    Joiner joiner = Joiner.on("\",\"");
    sb.append(joiner.join(filteredDimension));
    sb.append("\"]");
    return sb.toString();
  }

  public static DimensionKey createDimensionKey(String dimension) {
    dimension = dimension.replace("[\"", "");
    dimension = dimension.replace("\"]", "");
    return new DimensionKey(dimension.split("\",\""));
  }

  public static Map<String, String> createDimensionValues(TableSpec tableSpec ) {
    Map<String, String> dimensionValues = new HashMap<String, String>();
    if (tableSpec.getGroupBy() != null) {
      dimensionValues.put(tableSpec.getGroupBy(), "!");
    }
    if (tableSpec.getFixedDimensions() != null) {
      dimensionValues.putAll(tableSpec.getFixedDimensions());
    }
    return dimensionValues;
  }

}
