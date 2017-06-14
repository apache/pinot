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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PercentageBasedRoutingTableSelector implements RoutingTableSelector {
  private static Logger LOGGER = LoggerFactory.getLogger(PercentageBasedRoutingTableSelector.class);
  private static final String TABLE_KEY = "table";

  private final Map<String, Integer> _percentMap = new HashMap<>(1);
  private final Random _random = new Random();

  public PercentageBasedRoutingTableSelector() {
  }

  public void init(Configuration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    try {
      Configuration tablesConfig = configuration.subset(TABLE_KEY);
      if (tablesConfig == null || tablesConfig.isEmpty()) {
        LOGGER.info("No specific table configuration. Using 0% LLC for all tables");
        return;
      }
      ConfigurationMap cmap = new ConfigurationMap(tablesConfig);
      Set<Map.Entry<String, Integer>> mapEntrySet = cmap.entrySet();
      for (Map.Entry<String, Integer> entry : mapEntrySet) {
        LOGGER.info("Using {} percent LLC routing for table {}", entry.getValue(), entry.getKey());
        _percentMap.put(entry.getKey(), entry.getValue());
      }
    } catch (Exception e) {
      LOGGER.warn("Could not parse get config for {}. Using no LLC routing", TABLE_KEY, e);
    }
  }

  @Override
  public void registerTable(String realtimeTableName) {
    // Nothing to do, since config is read at init time from the Pinot config
  }

  @Override
  public boolean shouldUseLLCRouting(String realtimeTableName) {
    Integer percentToLLC = _percentMap.get(realtimeTableName);
    if (percentToLLC == null || percentToLLC == 0) {
      return false;
    }
    if (_random.nextInt(100) < percentToLLC) {
      return true;
    }
    return false;
  }
}
