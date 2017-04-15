package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class EntityUtils {
  // event:      thirdeye:event:{type}:{name}:{start}
  // metric:     thirdeye:metric:{dataset}:{name}
  // metric dim: thirdeye:metric:{dataset}:{name}:{dimension}

  public enum EntityType {
    EVENT("thirdeye:event"),
    METRIC("thirdeye:metric"),
    TIMERANGE("thirdeye:timerange"),
    BASELINE("thirdeye:baseline"),
    UNKNOWN("");

    private final String prefix;

    EntityType(String prefix) {
      this.prefix = prefix;
    }

    public String getPrefix() {
      return prefix;
    }

    public String formatUrn(String format, Object... values) {
      return this.prefix + ":" + String.format(format, values);
    }
  }

  public static EntityType getType(String urn) {
    for(EntityType t : EntityType.values()) {
      if(isType(urn, t))
        return t;
    }
    return EntityType.UNKNOWN;
  }

  public static boolean isType(String urn, EntityType type) {
    return urn.startsWith(type.getPrefix());
  }

  public static Set<Entity> filterType(Collection<Entity> entities, EntityType type) {
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

  public static TimeRangeEntity getContextTimeRange(ExecutionContext context) {
    Set<Entity> entities = filterContext(context, EntityType.TIMERANGE);
    if(entities.size() != 1)
      return null;
    Entity range = entities.iterator().next();
    if(range instanceof TimeRangeEntity)
      return (TimeRangeEntity)range;
    return TimeRangeEntity.fromURN(range.getUrn(), range.getScore());
  }

  public static BaselineEntity getContextBaseline(ExecutionContext context) {
    List<BaselineEntity> baselines = new ArrayList<>(getContextBaselines(context));
    if(baselines.isEmpty())
      return null;
    return Collections.max(baselines, new Comparator<BaselineEntity>() {
      @Override
      public int compare(BaselineEntity o1, BaselineEntity o2) {
        return Double.compare(o1.getScore(), o2.getScore());
      }
    });
  }

  public static Set<BaselineEntity> getContextBaselines(ExecutionContext context) {
    Set<Entity> entities = filterContext(context, EntityType.BASELINE);
    Set<BaselineEntity> baselines = new HashSet<>();
    for(Entity e : entities) {
      if(e instanceof BaselineEntity) {
        baselines.add((BaselineEntity) e);
      } else {
        baselines.add(BaselineEntity.fromURN(e.getUrn(), e.getScore()));
      }
    }
    return baselines;
  }

  public static String assertType(String urn, EntityType type) {
    if(!isType(urn, EntityType.METRIC))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a '%s'", urn, type.getPrefix()));
    return urn;
  }

  public static Entity assertType(Entity entity, EntityType type) {
    assertType(entity.getUrn(), type);
    return entity;
  }
}
