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

package com.linkedin.pinot.broker.routing;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RoutingTableSelectorFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(RoutingTableSelectorFactory.class);

  private static String CLASSNAME_KEY = "class";
  private static String defaultClasssName = PercentageBasedRoutingTableSelector.class.getName();

  /*
   * "class" indicates the class to load. If it is null, the load the default class. with the configuration as
   * constructor.
   */
  public static RoutingTableSelector getRoutingTableSelector(Configuration config,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    RoutingTableSelector routingTableSelector = new PercentageBasedRoutingTableSelector();
    if (config == null) {
      LOGGER.warn("No config for routing table selector. Using default");
    } else {
      String selectorClassName = null;
      try {
        selectorClassName = (String) config.getProperty(CLASSNAME_KEY);
      } catch (Exception e) {
        LOGGER.warn("Could not parse property '{}'. Using {}", CLASSNAME_KEY, defaultClasssName, e);
      }

      Class<? extends RoutingTableSelector> clazz;
      if (selectorClassName == null || selectorClassName.isEmpty()) {
        LOGGER.info("Null or empty selector class name. Using default");
      } else {
        try {
          clazz = (Class<? extends RoutingTableSelector>) Class.forName(selectorClassName);
          routingTableSelector = clazz.newInstance();
        } catch (ClassNotFoundException e) {
          LOGGER.warn("Could not load '{}'. Loading {}", selectorClassName, defaultClasssName, e);
        } catch (InstantiationException e) {
          LOGGER.warn("Could not instantiate '{}'. Instantiating {}", selectorClassName, defaultClasssName, e);
        } catch (IllegalAccessException e) {
          LOGGER.warn("Could not instantiate '{}'. Instantiating {}", selectorClassName, defaultClasssName, e);
        }
      }
    }

    routingTableSelector.init(config, propertyStore);

    return routingTableSelector;
  }
}
