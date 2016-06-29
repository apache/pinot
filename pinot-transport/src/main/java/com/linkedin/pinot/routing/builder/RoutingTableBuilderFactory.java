/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.Utils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Create a RoutingTableBuilder instance from a given key.
 */
public class RoutingTableBuilderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTableBuilderFactory.class);

  private static Map<String, Class<? extends RoutingTableBuilder>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends RoutingTableBuilder>>();

  static {
    keyToFunction.put("random", RandomRoutingTableBuilder.class);
    keyToFunction.put("randomroutingtablebsuilder", RandomRoutingTableBuilder.class);
    keyToFunction.put("balanced", BalancedRandomRoutingTableBuilder.class);
    keyToFunction.put("balancedrandomroutingtablebsuilder", BalancedRandomRoutingTableBuilder.class);
    keyToFunction.put("kafkahighlevelconsumerbased", KafkaHighLevelConsumerBasedRoutingTableBuilder.class);
    keyToFunction.put("kafkahighlevelconsumerbasedroutingtablebuilder", KafkaHighLevelConsumerBasedRoutingTableBuilder.class);
  }

  /**
   * Obtains an instance of a routing table builder given a routing table building strategy name.
   *
   * @param routingTableBuilderKey The type of routing table builder to obtain
   * @return A routing table builder
   */
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
      LOGGER.error("Caught exception while getting routing table builder", ex);
      Utils.rethrowException(ex);
      throw new AssertionError("Should not reach this");
    }
  }
}
