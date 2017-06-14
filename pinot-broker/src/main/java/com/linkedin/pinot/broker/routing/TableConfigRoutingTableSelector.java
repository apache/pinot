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

import com.google.common.base.Splitter;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routing table selector that uses the "routing.llc.percentage" (between 0-100) custom config in metadata to determine
 * the appropriate ratio of queries to route to LLC.
 */
public class TableConfigRoutingTableSelector implements RoutingTableSelector, IZkDataListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigRoutingTableSelector.class);
  private static final String LLC_ROUTING_PERCENTAGE_CONFIG_KEY = "routing.llc.percentage";
  private static final Random RANDOM = new Random();
  private static final Executor LLC_CONFIG_FETCHER = Executors.newSingleThreadExecutor();
  private static final ConcurrentHashMap<String, String> CONFIG_FETCHES_IN_PROGRESS = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, Float> _tableRoutingRatioMap = new ConcurrentHashMap<>();
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Override
  public boolean shouldUseLLCRouting(String realtimeTableName) {
    // Randomly route to LLC based on the configured ratio
    return RANDOM.nextFloat() < _tableRoutingRatioMap.get(realtimeTableName);
  }

  private float getLlcRatio(String realtimeTableName) {
    TableConfig tableConfig = ZKMetadataProvider.getRealtimeTableConfig(_propertyStore, realtimeTableName);

    if (tableConfig == null) {
      LOGGER.warn("Failed to fetch table config for table {}", realtimeTableName);
      return 0.0f;
    }

    Map<String, String> customConfigs = tableConfig.getCustomConfig().getCustomConfigs();
    if (customConfigs.containsKey(LLC_ROUTING_PERCENTAGE_CONFIG_KEY)) {
      String routingPercentageString = customConfigs.get(LLC_ROUTING_PERCENTAGE_CONFIG_KEY);
      float routingPercentage;

      try {
        routingPercentage = Float.parseFloat(routingPercentageString);
      } catch (NumberFormatException e) {
        LOGGER.warn("Couldn't parse {} as a valid LLC routing percentage, should be a number between 0-100", e);
        return 0.0f;
      }

      if (routingPercentage < 0.0f || 100.0f < routingPercentage) {
        routingPercentage = Math.min(Math.max(routingPercentage, 0.0f), 100.0f);
        LOGGER.warn("LLC routing percentage ({}) is outside of [0;100], percentage was clamped to {}.",
            routingPercentageString, routingPercentage);
      }

      return routingPercentage;
    }

    return 0.0f;
  }

  @Override
  public void init(Configuration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  @Override
  public void registerTable(String realtimeTableName) {
    String tablePropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(realtimeTableName);
    _propertyStore.subscribeDataChanges(tablePropertyStorePath, this);

    _tableRoutingRatioMap.put(realtimeTableName, getLlcRatio(realtimeTableName) / 100.0f);
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    final List<String> zkPathParts = Splitter.on('/').splitToList(dataPath);
    String realtimeTableName = zkPathParts.get(zkPathParts.size() - 1);

    // Update the ratio in place
    _tableRoutingRatioMap.put(realtimeTableName, getLlcRatio(realtimeTableName) / 100.0f);
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    // Ignore for now
  }
}
