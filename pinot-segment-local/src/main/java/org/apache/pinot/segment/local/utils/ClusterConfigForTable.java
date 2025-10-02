/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.utils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains cluster level config for certain table config attributes.
 * This is useful for enabling/disabling certain features across the cluster
 * Each individual config will have its own precedence rules.
 */
public final class ClusterConfigForTable {

  // Controls whether to use NoDictColumnStatisticsCollector for no-dictionary columns globally.
  // If unset at cluster level, table-level config is used.
  // If cluster level is set to true/false, it overrides the table-level config.
  private static final String OPTIMISE_NO_DICT_STATS_COLLECTION_CONF = "pinot.stats.optimize.no.dict.collection";

  private ClusterConfigForTable() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigForTable.class);
  // if a key isn't present, it means it is not set at cluster level
  private static final Map<String, Boolean> CLUSTER_BOOLEAN_FLAGS = new ConcurrentHashMap<>();

  /** Listener that updates the cached cluster override on config changes. */
  public static class ConfigChangeListener implements PinotClusterConfigChangeListener {
    @Override
    public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
      checkNoDictStatsCollectorConfig(changedConfigs, clusterConfigs);
    }

    private void checkNoDictStatsCollectorConfig(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
      if (!changedConfigs.contains(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF)) {
        return;
      }
      String v = clusterConfigs.get(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF);
      if (v == null) {
        CLUSTER_BOOLEAN_FLAGS.remove(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF);
        return;
      }
      // Accept only explicit true/false, ignore invalid values by resetting to null to fall back to table config
      String lower = v.trim().toLowerCase();
      if ("true".equals(lower)) {
        CLUSTER_BOOLEAN_FLAGS.put(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF, Boolean.TRUE);
      } else if ("false".equals(lower)) {
        CLUSTER_BOOLEAN_FLAGS.put(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF, Boolean.FALSE);
      } else {
        LOGGER.warn("Invalid value: {} for config: {}, ignoring and falling back to table config", v,
            OPTIMISE_NO_DICT_STATS_COLLECTION_CONF);
        CLUSTER_BOOLEAN_FLAGS.remove(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF);
      }
    }
  }

  /** Returns whether we should use the optimized collector, applying cluster-level override if set. */
  public static boolean useOptimizedNoDictCollector(TableConfig tableConfig) {
    Boolean override = CLUSTER_BOOLEAN_FLAGS.get(OPTIMISE_NO_DICT_STATS_COLLECTION_CONF);
    if (override != null) {
      return override;
    }
    return tableConfig.getIndexingConfig().isOptimiseNoDictStatsCollection();
  }
}
