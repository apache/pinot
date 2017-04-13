package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class URNUtils {
  // event: thirdeye:event:{type}:{name}:{start}
  // metric: thirdeye:metric:{dataset}:{name}
  // metricdimension: thirdeye:metricdimension:{dataset}:{name}:{dimension}

  public static boolean isEventURN(String urn) {
    return urn.startsWith("thirdeye:event:");
  }

  public static boolean isMetricURN(String urn) {
    return urn.startsWith("thirdeye:metric:");
  }

  public static String getMetricDataset(String urn) {
    if(!isMetricURN(urn))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a metric", urn));
    return urn.split(":")[2];
  }

  public static String getMetricName(String urn) {
    if(!isMetricURN(urn))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a metric", urn));
    return urn.split(":")[3];
  }

  public static <T extends Entity> Map<String, T> mapEntityURNs(Collection<T> entities) {
    Map<String, T> map = new HashMap<>();
    for(T e : entities) {
      map.put(e.getUrn(), e);
    }
    return map;
  }
}
