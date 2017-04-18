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


/**
 * Utility class to simplify type-checking and extraction of entities
 */
public class EntityUtils {
  // timerange:  thirdeye:timerange:{start}:{end}
  // baseline:   thirdeye:baseline:{start}:{end}
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

  /**
   * Returns the entity type as encoded in a URN
   *
   * @param urn entity urn
   * @return inferred entity type
   */
  public static EntityType getType(String urn) {
    for(EntityType t : EntityType.values()) {
      if(isType(urn, t))
        return t;
    }
    return EntityType.UNKNOWN;
  }

  /**
   * Returns {@code true} if the URN encodes the specified entity type {@code type}, or
   * {@code false} otherwise.
   *
   * @param urn entity urn
   * @param type entity type
   * @return {@code true} if entity type matches, {@code false} otherwise.
   */
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

  /**
   * Returns all entities with a matching entity type contained in the search context of an
   * execution context.
   *
   * @param context execution context
   * @param type entity type
   * @return set of entities with specified type
   */
  public static Set<Entity> filterContext(ExecutionContext context, EntityType type) {
    return new HashSet<>(filterType(context.getSearchContext().getEntities(), type));
  }

  /**
   * Returns the dataset as encoded in the URN of a MetricEntity.
   *
   * @param urn MetricEntity URN
   * @throws IllegalArgumentException if the URN does not encode a MetricEntity.
   * @return metric entity dataset
   */
  public static String getMetricDataset(String urn) {
    if(!isType(urn, EntityType.METRIC))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a metric", urn));
    return urn.split(":")[2];
  }

  /**
   * Returns the metric name as encoded in the URN of a MetricEntity.
   *
   * @param urn MetricEntity URN
   * @throws IllegalArgumentException if the URN does not encode a MetricEntity.
   * @return metric entity dataset
   */
  public static String getMetricName(String urn) {
    if(!isType(urn, EntityType.METRIC))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a metric", urn));
    return urn.split(":")[3];
  }

  /**
   * Returns a mapping of URNs to entities derived from a collection of entities. In case
   * the same URN is used by multiple entities only one entity is referenced in the resulting map.
   *
   * @param entities entities
   * @param <T> (sub-)class of Entity
   * @return mapping of URNs to Entities
   */
  public static <T extends Entity> Map<String, T> mapEntityURNs(Collection<T> entities) {
    Map<String, T> map = new HashMap<>();
    for(T e : entities) {
      map.put(e.getUrn(), e);
    }
    return map;
  }

  /**
   * Returns the TimeRangeEntity contained in the search context of an execution context.
   * Expects exactly one TimeRange entity and returns {@code null} if none or multiple
   * time range entities are found. If the search context contains an instance of
   * TimeRangeEntity it returns the instance. Otherwise, constructs a new instance of
   * TimeRangeEntity from an encoding URN.
   *
   * @param context execution context
   * @return TimeRangeEntity
   */
  public static TimeRangeEntity getContextTimeRange(ExecutionContext context) {
    Set<Entity> entities = filterContext(context, EntityType.TIMERANGE);
    if(entities.size() != 1)
      return null;
    Entity range = entities.iterator().next();
    if(range instanceof TimeRangeEntity)
      return (TimeRangeEntity)range;
    return TimeRangeEntity.fromURN(range.getUrn(), range.getScore());
  }

  /**
   * Returns the BaselineEntity contained in the search context of an execution context.
   * Expects exactly one BaselineEntity entity and returns {@code null} if no
   * baseline entity found. If multiple BaselineEntities are found, the method returns the most
   * recent one. If the search context contains an instance of
   * BaselineEntity it returns the instance. Otherwise, constructs new instances of
   * BaselineEntity from any encoding URNs.
   *
   * @param context execution context
   * @return BaselineEntity
   */
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

  /**
   * Returns all BaselineEntity contained in the search context of an execution context.
   * If the search context contains instances of
   * BaselineEntity it returns the instances. It also constructs new instances of
   * BaselineEntity from any encoding URNs.
   *
   * @param context execution context
   * @return set of baseline entities
   */
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

  /**
   * Throws an IllegalArgumentException if the URN does not encode the specified entity type.
   *
   * @param urn entity URN
   * @param type entity type
   * @throws IllegalArgumentException if the URN does not encode the specified entity type
   * @return the entity urn
   */
  public static String assertType(String urn, EntityType type) {
    if(!isType(urn, EntityType.METRIC))
      throw new IllegalArgumentException(String.format("Entity '%s' is not a '%s'", urn, type.getPrefix()));
    return urn;
  }

  /**
   * Throws an IllegalArgumentException if the entity's URN does not encode the specified entity type.
   *
   * @param entity entity
   * @param type entity type
   * @throws IllegalArgumentException if the entity's URN does not encode the specified entity type
   * @return the entity
   */
  public static Entity assertType(Entity entity, EntityType type) {
    assertType(entity.getUrn(), type);
    return entity;
  }
}
