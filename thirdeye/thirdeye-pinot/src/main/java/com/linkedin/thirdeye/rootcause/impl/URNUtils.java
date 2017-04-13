package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.SearchContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class URNUtils {
  // event: thirdeye:event:{type}:{name}:{start}
  // metric: thirdeye:metric:{dataset}:{name}
  // metricdimension: thirdeye:metricdimension:{dataset}:{name}:{dimension}

  public enum EntityType {
    EVENT("thirdeye:event:"),
    METRIC("thirdeye:metric:"),
    METRIC_DIMENSION("thirdeye:metricdimension:");

    private final String prefix;

    EntityType(String prefix) {
      this.prefix = prefix;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  public static boolean isType(String urn, EntityType type) {
    return urn.startsWith(type.getPrefix());
  }

  public static Collection<Entity> filterType(Collection<Entity> entities, EntityType type) {
    Set<Entity> filtered = new HashSet<>();
    for(Entity e : entities) {
      if(isType(e.getUrn(), type))
        filtered.add(e);
    }
    return filtered;
  }

  public static Set<Entity> filterContext(ExecutionContext context, EntityType type) {
    return new HashSet<>(filterType(context.getSearchContext().getEntities(), type));
  }

  public static String getMetricDataset(String urn) {
    if(!isType(urn, EntityType.METRIC))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a metric", urn));
    return urn.split(":")[2];
  }

  public static String getMetricName(String urn) {
    if(!isType(urn, EntityType.METRIC))
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
