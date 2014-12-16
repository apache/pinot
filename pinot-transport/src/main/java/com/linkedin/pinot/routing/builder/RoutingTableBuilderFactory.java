package com.linkedin.pinot.routing.builder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Create a RoutingTableBuilder instance from a given key.
 * 
 * @author xiafu
 *
 */
public class RoutingTableBuilderFactory {

  private static Map<String, Class<? extends RoutingTableBuilder>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends RoutingTableBuilder>>();

  static {
    keyToFunction.put("random", RandomRoutingTableBuilder.class);
    keyToFunction.put("randomroutingtablebsuilder", RandomRoutingTableBuilder.class);
  }

  @SuppressWarnings("unchecked")
  public static RoutingTableBuilder get(String routingTableBuilderKey) {
    try {
      Class<? extends RoutingTableBuilder> cls = keyToFunction.get(routingTableBuilderKey.toLowerCase());
      if (cls != null) {
        return cls.newInstance();
      }
      cls = (Class<? extends RoutingTableBuilder>) Class.forName(routingTableBuilderKey);
      keyToFunction.put(routingTableBuilderKey, cls);
      return cls.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
