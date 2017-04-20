package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
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

//  public static Set<Entity> filterType(Collection<Entity> entities, EntityType type) {
//    Set<Entity> filtered = new HashSet<>();
//    for(Entity e : entities) {
//      if(isType(e, type))
//        filtered.add(e);
//    }
//    return filtered;
//  }
//
//  /**
//   * Returns all entities with a matching entity type contained in the search context of an
//   * execution context.
//   *
//   * @param context execution context
//   * @param type entity type
//   * @return set of entities with specified type
//   */
//  public static Set<Entity> filterContext(ExecutionContext context, EntityType type) {
//    return new HashSet<>(filterType(context.getSearchContext().getEntities(), type));
//  }
//
  public static <T extends Entity> Set<T> filterContext(ExecutionContext context, Class<? extends T> clazz) {
    HashSet<T> filtered = new HashSet<>();
    for(Entity e : context.getSearchContext().getEntities()) {
      if(clazz.isInstance(e))
        filtered.add((T)e);
    }
    return filtered;
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
}
