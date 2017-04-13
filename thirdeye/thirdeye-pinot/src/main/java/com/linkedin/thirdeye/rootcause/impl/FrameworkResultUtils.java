package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class FrameworkResultUtils {
  public static Map<URNUtils.EntityType, Collection<Entity>> topKPerType(Collection<Entity> entities, int k) {
    Map<URNUtils.EntityType, Collection<Entity>> map = new HashMap<>();
    for(Entity e : entities) {
      URNUtils.EntityType t = URNUtils.getType(e.getUrn());

      if(!map.containsKey(t))
        map.put(t, new ArrayList<Entity>());

      Collection<Entity> current = map.get(t);
      if(current.size() < k)
        current.add(e);
    }

    return map;
  }
}
