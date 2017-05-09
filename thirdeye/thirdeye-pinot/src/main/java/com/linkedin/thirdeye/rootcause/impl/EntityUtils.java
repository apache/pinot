package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

}
