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
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Utility for deciding whether to use the optimized no-dictionary stats collector.
 *
 * Precedence:
 * 1) Cluster-level override if present
 * 2) Table-level IndexingConfig.canOptimiseNoDictStatsCollection()
 */
public final class NoDictStatsCollectionUtils {

  private NoDictStatsCollectionUtils() {
  }

  // null means not set at cluster level
  private static final AtomicReference<Boolean> CLUSTER_OVERRIDE = new AtomicReference<>(null);

  /** Listener that updates the cached cluster override on config changes. */
  public static class ConfigChangeListener implements PinotClusterConfigChangeListener {
    @Override
    public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
      String v = clusterConfigs.get(CommonConstants.Stats.CONFIG_OF_OPTIMISE_NO_DICT_STATS_COLLECTION);
      if (v == null) {
        CLUSTER_OVERRIDE.set(null);
        return;
      }
      // Accept only explicit true/false, ignore invalid values by resetting to null to fall back to table config
      String lower = v.trim().toLowerCase();
      if ("true".equals(lower)) {
        CLUSTER_OVERRIDE.set(Boolean.TRUE);
      } else if ("false".equals(lower)) {
        CLUSTER_OVERRIDE.set(Boolean.FALSE);
      } else {
        CLUSTER_OVERRIDE.set(null);
      }
    }
  }

  /** Returns whether we should use the optimized collector, applying cluster-level override if set. */
  public static boolean useOptimizedNoDictCollector(TableConfig tableConfig) {
    Boolean override = CLUSTER_OVERRIDE.get();
    if (override != null) {
      return override;
    }
    return tableConfig.getIndexingConfig().isOptimiseNoDictStatsCollection();
  }

  /** Exposed for tests. */
  static void setClusterOverrideForTests(@Nullable Boolean value) {
    CLUSTER_OVERRIDE.set(value);
  }
}


