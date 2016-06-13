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
package com.linkedin.pinot.broker.broker.helix;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;


public class DefaultHelixBrokerConfig {
  public static final String HELIX_FLAPPING_TIME_WINDOW_NAME = "pinot.broker.helix.flappingTimeWindowMs";
  public static final String DEFAULT_HELIX_FLAPPING_TIMEIWINDWOW_MS = "0";

  public static Configuration getDefaultBrokerConf() {
    Configuration brokerConf = new PropertiesConfiguration();

    // config based routing
    brokerConf.addProperty("pinot.broker.transport.routingMode", "HELIX");

    brokerConf.addProperty("pinot.broker.routing.table.builder.default.offline.class", "balanced");
    brokerConf.addProperty("pinot.broker.routing.table.builder.default.offline.numOfRoutingTables", "10");
    brokerConf.addProperty("pinot.broker.routing.table.builder.default.realtime.class", "Kafkahighlevelconsumerbased");
    brokerConf.addProperty("pinot.broker.routing.table.builder.tables", "");

    //client properties
    brokerConf.addProperty("pinot.broker.client.enableConsole", "true");
    brokerConf.addProperty("pinot.broker.client.queryPort", "8099");
    brokerConf.addProperty("pinot.broker.client.consolePath", "../webapp");

    // [PINOT-2435] setting to 0 so it doesn't disconnect from zk
    brokerConf.addProperty("pinot.broker.helix.flappingTimeWindowMs", "0");

    return brokerConf;
  }

  public static Configuration getDefaultBrokerConf(Configuration externalConfigs) {
    final Configuration defaultConfigs = getDefaultBrokerConf();
    @SuppressWarnings("unchecked")
    Iterator<String> iterable = externalConfigs.getKeys();
    while (iterable.hasNext()) {
      String key = iterable.next();
      defaultConfigs.setProperty(key, externalConfigs.getProperty(key));
    }
    return defaultConfigs;
  }
}
