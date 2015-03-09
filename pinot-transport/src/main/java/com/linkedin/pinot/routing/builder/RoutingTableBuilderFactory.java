/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
