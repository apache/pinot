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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants.ConfigChangeListenerConstants;
import org.apache.pinot.spi.utils.ConsumingSegmentCommitModeProvider;
import org.apache.pinot.spi.utils.ConsumingSegmentCommitModeProvider.Mode;
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

  private final AtomicReference<Mode> _forceCommitReloadMode = new AtomicReference<>(
      Mode.fromString(ConfigChangeListenerConstants.DEFAULT_FORCE_COMMIT_RELOAD_MODE, Mode.NONE));

  private UpsertInconsistentStateConfig() {
    // Register this instance as the provider so pinot-segment-local can access the mode directly
    ConsumingSegmentCommitModeProvider.register(this::getForceCommitReloadMode);
  }

  public static UpsertInconsistentStateConfig getInstance() {
    return INSTANCE;
  }

  /**
   * Checks if force commit/reload is allowed based on the current mode.
   *
   * @return true if force commit/reload is allowed (mode is PROTECTED or UNSAFE)
   */
  public boolean isForceCommitReloadAllowed() {
    return _forceCommitReloadMode.get().isReloadEnabled();
  }

  /**
   * Returns the current force commit/reload mode.
   */
  public Mode getForceCommitReloadMode() {
    return _forceCommitReloadMode.get();
  }

  /**
   * Returns the current config key used for this setting.
   */
  public String getConfigKey() {
    return ConfigChangeListenerConstants.FORCE_COMMIT_RELOAD_CONFIG;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(ConfigChangeListenerConstants.FORCE_COMMIT_RELOAD_CONFIG)) {
      return;
    }

    String configValue = clusterConfigs.get(ConfigChangeListenerConstants.FORCE_COMMIT_RELOAD_CONFIG);
    Mode newMode = Mode.fromString(configValue, Mode.NONE);

    Mode previousMode = _forceCommitReloadMode.getAndSet(newMode);
    if (previousMode != newMode) {
      LOGGER.info("Updated cluster config: {} from {} to {}",
          ConfigChangeListenerConstants.FORCE_COMMIT_RELOAD_CONFIG, previousMode, newMode);
    }
  }
}
