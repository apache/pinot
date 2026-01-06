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
package org.apache.pinot.core.data.manager.realtime;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton class to manage the configuration for force commit and reload on consuming segments
 * for upsert tables with inconsistent state configurations (partial upsert or dropOutOfOrderRecord=true
 * with consistency mode NONE and replication > 1).
 *
 * This configuration is dynamically updatable via ZK cluster config without requiring a server restart.
 */
public class UpsertInconsistentStateConfig implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertInconsistentStateConfig.class);
  private static final UpsertInconsistentStateConfig INSTANCE = new UpsertInconsistentStateConfig();

  // Cluster config key to control whether force commit/reload is allowed for upsert tables with inconsistent state
  public static final String FORCE_COMMIT_RELOAD_CONFIG = "pinot.server.upsert.force.commit.reload";

  // Default: true (force commit/reload is disabled for safety)
  public static final boolean DEFAULT_FORCE_COMMIT_RELOAD = true;

  private final AtomicBoolean _forceCommitReloadEnabled = new AtomicBoolean(DEFAULT_FORCE_COMMIT_RELOAD);

  private UpsertInconsistentStateConfig() {
  }

  public static UpsertInconsistentStateConfig getInstance() {
    return INSTANCE;
  }

  /**
   * Checks if force commit/reload is allowed for the given table config.
   * Returns true if force commit/reload is enabled OR the table does NOT have inconsistent state configs.
   */
  public boolean isForceCommitReloadAllowed(TableConfig tableConfig) {
    return _forceCommitReloadEnabled.get() || !TableConfigUtils.checkForInconsistentStateConfigs(tableConfig);
  }

  /**
   * Returns the current config key used for this setting.
   */
  public String getConfigKey() {
    return FORCE_COMMIT_RELOAD_CONFIG;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(FORCE_COMMIT_RELOAD_CONFIG)) {
      return;
    }

    String configValue = clusterConfigs.get(FORCE_COMMIT_RELOAD_CONFIG);
    boolean enabled = (configValue == null) ? DEFAULT_FORCE_COMMIT_RELOAD : Boolean.parseBoolean(configValue);

    boolean previousValue = _forceCommitReloadEnabled.getAndSet(enabled);
    if (previousValue != enabled) {
      LOGGER.info("Updated {} from {} to {}", FORCE_COMMIT_RELOAD_CONFIG, previousValue, enabled);
    }
  }

  // For testing
  void setForceCommitReloadEnabled(boolean enabled) {
    _forceCommitReloadEnabled.set(enabled);
  }
}
