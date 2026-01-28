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
package org.apache.pinot.spi.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants.ConfigChangeListenerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton class to manage the configuration for force commit on consuming segments
 * for upsert tables with inconsistent state configurations (partial upsert or dropOutOfOrderRecord=true
 * with consistency mode NONE and replication > 1).
 *
 * This configuration is dynamically updatable via ZK cluster config without requiring a server restart.
 */
public class ConsumingSegmentConsistencyModeListener implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumingSegmentConsistencyModeListener.class);
  private static final ConsumingSegmentConsistencyModeListener INSTANCE = new ConsumingSegmentConsistencyModeListener();

  public enum Mode {
    /**
     * Force commit is disabled for tables with inconsistent state configurations.
     * Safe option that prevents potential data inconsistency issues.
     */
    DEFAULT(false),

    /**
     * Force commit is enabled but tables with partial upsert or dropOutOfOrderRecord=true (with replication > 1)
     * will have their upsert metadata reverted when inconsistencies are detected.
     */
    PROTECTED(true),

    /**
     * Force commit is enabled for all tables regardless of their configuration.
     * Use with caution as this may cause data inconsistency for partial-upsert tables
     * or upsert tables with dropOutOfOrderRecord/outOfOrderRecordColumn enabled when replication > 1.
     * Inconsistency checks and metadata revert are skipped.
     */
    UNSAFE(true);

    private final boolean _forceCommitAllowed;

    Mode(boolean forceCommitAllowed) {
      _forceCommitAllowed = forceCommitAllowed;
    }

    public boolean isForceCommitAllowed() {
      return _forceCommitAllowed;
    }

    public static Mode fromString(String value) {
      if (value == null || value.trim().isEmpty()) {
        return DEFAULT;
      }
      try {
        return Mode.valueOf(value.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        return DEFAULT;
      }
    }
  }

  private final AtomicReference<Mode> _consistencyMode = new AtomicReference<>(Mode.DEFAULT);

  private ConsumingSegmentConsistencyModeListener() {
  }

  public static ConsumingSegmentConsistencyModeListener getInstance() {
    return INSTANCE;
  }

  public boolean isForceCommitAllowed() {
    return _consistencyMode.get().isForceCommitAllowed();
  }

  public Mode getConsistencyMode() {
    return _consistencyMode.get();
  }

  public String getConfigKey() {
    return ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE)) {
      return;
    }

    String configValue = clusterConfigs.get(ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE);
    Mode newMode = Mode.fromString(configValue);

    Mode previousMode = _consistencyMode.getAndSet(newMode);
    if (previousMode != newMode) {
      LOGGER.info("Updated cluster config: {} from {} to {}",
          ConfigChangeListenerConstants.CONSUMING_SEGMENT_CONSISTENCY_MODE, previousMode, newMode);
    }
  }

  @VisibleForTesting
  public void setMode(Mode mode) {
    _consistencyMode.set(mode);
  }

  @VisibleForTesting
  public void reset() {
    _consistencyMode.set(Mode.DEFAULT);
  }
}
