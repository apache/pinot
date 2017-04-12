package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


public class EntityUtils {
  // thirdeye:event:type:name:start
  // thirdeye:metric:dataset:name

  public static boolean isEventEntity(Entity e) {
    return e.getUrn().startsWith("thirdeye:event:");
  }

  public static boolean isMetricEntity(Entity e) {
    return e.getUrn().startsWith("thirdeye:metric:");
  }

  public static String getMetricEntityName(Entity e) {
    if(!isMetricEntity(e))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a metric", e.getUrn()));
    return e.getUrn().split(":")[3];
  }
}
