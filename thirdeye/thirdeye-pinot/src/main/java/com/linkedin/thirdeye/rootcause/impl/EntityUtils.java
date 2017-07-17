package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.PipelineContext;
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

  /**
   * Returns {@code true} if the URN encodes the specified entity type {@code type}, or
   * {@code false} otherwise.
   *
   * @param e entity
   * @param type entity type
   * @return {@code true} if entity type matches, {@code false} otherwise.
   */
  public static boolean isType(Entity e, EntityType type) {
    return e.getUrn().startsWith(type.getPrefix());
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
   * Throws an IllegalArgumentException if the URN does not encode the specified entity type.
   *
   * @param urn entity URN
   * @param type entity type
   * @throws IllegalArgumentException if the URN does not encode the specified entity type
   * @return the entity urn
   */
  public static String assertType(String urn, EntityType type) {
    if(!isType(urn, type))
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

  /**
   * Normalizes scores among a set of entities to a range between {@code 0.0} and {@code 1.0}.
   *
   * @param entities entities
   * @return entities with normalized scores
   */
  public static Set<Entity> normalizeScores(Set<? extends Entity> entities) {
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    for(Entity e : entities) {
      min = Math.min(e.getScore(), min);
      max = Math.max(e.getScore(), max);
    }

    double range = max - min;
    Set<Entity> out = new HashSet<>();

    if(range <= 0) {
      for(Entity e : entities) {
        out.add(e.withScore(1.0));
      }
      return out;
    }

    for(Entity e : entities) {
      out.add(e.withScore((e.getScore() - min) / range));
    }

    return out;
  }

  /**
   * Returns the top {@code K} entities based on score
   *
   * @param entities entities
   * @param k top k elements to return (<0 indicates all)
   * @return top k entities
   */
  public static <T extends Entity> Set<T> topk(Set<T> entities, int k) {
    if (k < 0)
      return entities;
    List<T> sorted = new ArrayList<>(entities);
    Collections.sort(sorted, Entity.HIGHEST_SCORE_FIRST);
    return new HashSet<>(sorted.subList(0, Math.min(k, sorted.size())));
  }

  /**
   * Returns the top {@code K} entities based on score after normalizing scores to the interval
   * {@code [0.0, 1.0]}.
   *
   * @param entities entities
   * @param k top k elements to return (<0 indicates all)
   * @return top k normalized entities
   */
  public static Set<Entity> topkNormalized(Set<? extends Entity> entities, int k) {
    return topk(normalizeScores(entities), k);
  }

  /**
   * Attemps to parse {@code urn} and return a specific Entity subtype with the given {@code score}
   * Supports {@code MetricEntity}, {@code DimensionEntity}, {@code TimeRangeEntity}, and
   * {@code ServiceEntity}.
   *
   * @param urn entity urn
   * @param score entity score
   * @throws IllegalArgumentException, if the urn cannot be parsed
   * @return entity subtype instance
   */
  public static Entity parseURN(String urn, double score) {
    if(DimensionEntity.TYPE.isType(urn)) {
      return DimensionEntity.fromURN(urn, score);

    } else if(MetricEntity.TYPE.isType(urn)) {
      return MetricEntity.fromURN(urn, score);

    } else if(TimeRangeEntity.TYPE.isType(urn)) {
      return TimeRangeEntity.fromURN(urn, score);

    } else if(ServiceEntity.TYPE.isType(urn)) {
      return ServiceEntity.fromURN(urn, score);

    } else if(HyperlinkEntity.TYPE.isType(urn)) {
      return HyperlinkEntity.fromURL(urn, score);
    }
    throw new IllegalArgumentException(String.format("Could not parse URN '%s'", urn));
  }

  /**
   * Attemps to parse {@code urn} and return a specific Entity subtype with the given {@code score}
   * Supports {@code MetricEntity}, {@code DimensionEntity}, {@code TimeRangeEntity}, and
   * {@code ServiceEntity}.
   * If Urn can't be parsed return raw entity
   *
   * @param urn entity urn
   * @param score entity score
   * @return entity subtype instance
   */
  public static Entity parseURNRaw(String urn, double score) {
    try {
      return parseURN(urn, score);
    } catch (IllegalArgumentException e) {
      return new Entity(urn, score);
    }
  }

}
