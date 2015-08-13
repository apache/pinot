package com.linkedin.thirdeye.anomaly.util;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.api.DimensionSpec;

/**
 *
 */
public class DimensionSpecUtils {

  public static List<String> getDimensionNames(List<DimensionSpec> dimensionSpecs) {
    List<String> dimensionNames = new ArrayList<String>(dimensionSpecs.size());
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      dimensionNames.add(dimensionSpec.getName());
    }
    return dimensionNames;
  }

}
