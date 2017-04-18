package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Utility class for grouping linearized framework results
 */
public class FrameworkResultUtils {

  /**
   * Returns the top K (first K) results per entity type from a collection of entities.
   *
   * @param entities aggregated entities
   * @param k maximum number of entities per entity type
   * @return mapping of entity types to list of entities
   */
  public static Map<EntityUtils.EntityType, Collection<Entity>> topKPerType(Collection<Entity> entities, int k) {
    Map<EntityUtils.EntityType, Collection<Entity>> map = new HashMap<>();
    for(Entity e : entities) {
      EntityUtils.EntityType t = EntityUtils.getType(e.getUrn());

      if(!map.containsKey(t))
        map.put(t, new ArrayList<Entity>());

      Collection<Entity> current = map.get(t);
      if(current.size() < k)
        current.add(e);
    }

    return map;
  }
}
