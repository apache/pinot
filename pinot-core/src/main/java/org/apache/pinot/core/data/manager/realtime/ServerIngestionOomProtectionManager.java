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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Applies server-local backpressure to realtime ingestion while JVM heap usage is above a configured threshold.
public class ServerIngestionOomProtectionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerIngestionOomProtectionManager.class);
  private static final String SERVER_INSTANCE_CONFIG_PREFIX =
      CommonConstants.Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".";
  private static final Set<String> SERVER_INGESTION_OOM_PROTECTION_CONFIG_KEYS = Set.of(
      CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE,
      CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_THROTTLE_THRESHOLD,
      CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_RECOVERY_THRESHOLD,
      CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS,
      CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_GC_INTERVAL_MS);

  // Server mode has the UPSERT_DEDUP_ONLY policy; table-level override intentionally only supports ENABLE/DISABLE.
  enum ServerMode {
    ENABLE, UPSERT_DEDUP_ONLY, DISABLE
  }

  private static final ServerMode DEFAULT_SERVER_MODE =
      ServerMode.valueOf(CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_MODE);

  private final Supplier<TableConfig> _tableConfigSupplier;
  private final BooleanSupplier _isUpsertOrDedupEnabledSupplier;
  private final ServerThrottleState _serverThrottleState;

  ServerIngestionOomProtectionManager(Supplier<TableConfig> tableConfigSupplier,
      BooleanSupplier isUpsertOrDedupEnabledSupplier,
      ServerThrottleState serverThrottleState) {
    _tableConfigSupplier = tableConfigSupplier;
    _isUpsertOrDedupEnabledSupplier = isUpsertOrDedupEnabledSupplier;
    _serverThrottleState = serverThrottleState;
  }

  public boolean waitIfProtectionNeeded(BooleanSupplier stopCondition)
      throws InterruptedException {
    boolean waited = false;
    while (!stopCondition.getAsBoolean() && shouldThrottle()) {
      waited = true;
      Thread.sleep(_serverThrottleState.getCheckIntervalMs());
    }
    return waited;
  }

  @VisibleForTesting
  boolean shouldThrottle() {
    return isEnabledForTable(_tableConfigSupplier.get()) && _serverThrottleState.shouldThrottle();
  }

  @VisibleForTesting
  boolean isThrottling() {
    return _serverThrottleState.isThrottling();
  }

  @VisibleForTesting
  void resetMetrics() {
    _serverThrottleState.resetMetrics();
  }

  private boolean isEnabledForTable(@Nullable TableConfig tableConfig) {
    if (tableConfig == null || !TableNameBuilder.isRealtimeTableResource(tableConfig.getTableName())) {
      return false;
    }
    return getTableEnablement(tableConfig).isEnabled(() -> switch (_serverThrottleState.getMode()) {
      case ENABLE -> true;
      case UPSERT_DEDUP_ONLY -> _isUpsertOrDedupEnabledSupplier.getAsBoolean();
      case DISABLE -> false;
    });
  }

  private static Enablement getTableEnablement(@Nullable TableConfig tableConfig) {
    if (tableConfig == null) {
      return Enablement.DEFAULT;
    }
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      return Enablement.DEFAULT;
    }
    StreamIngestionConfig streamIngestionConfig = ingestionConfig.getStreamIngestionConfig();
    return streamIngestionConfig != null ? streamIngestionConfig.getOomProtection() : Enablement.DEFAULT;
  }

  public static ServerThrottleState createServerThrottleState(
      @Nullable PinotConfiguration instanceDataManagerConfig, ServerMetrics serverMetrics) {
    return new ServerThrottleState(instanceDataManagerConfig, serverMetrics, ResourceUsageUtils::getUsedHeapSize,
        ResourceUsageUtils::getMaxHeapSize, System::currentTimeMillis, System::gc);
  }

  private static boolean containsIngestionOomProtectionConfig(Set<String> configKeys) {
    for (String key : configKeys) {
      String unprefixedKey = key.startsWith(SERVER_INSTANCE_CONFIG_PREFIX)
          ? key.substring(SERVER_INSTANCE_CONFIG_PREFIX.length())
          : key;
      if (SERVER_INGESTION_OOM_PROTECTION_CONFIG_KEYS.contains(unprefixedKey)) {
        return true;
      }
    }
    return false;
  }

  private static ServerMode getMode(String rawMode) {
    if (rawMode == null) {
      LOGGER.warn("Invalid server ingestion OOM protection mode: null. Falling back to: {}",
          DEFAULT_SERVER_MODE);
      return DEFAULT_SERVER_MODE;
    }
    try {
      return ServerMode.valueOf(rawMode.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Invalid server ingestion OOM protection mode: {}. Falling back to: {}",
          rawMode, DEFAULT_SERVER_MODE);
      return DEFAULT_SERVER_MODE;
    }
  }

  /// Server-wide realtime ingestion OOM protection state shared by all realtime table managers in one server process.
  public static class ServerThrottleState implements PinotClusterConfigChangeListener {
    private final ServerMetrics _serverMetrics;
    private final PinotConfiguration _instanceDataManagerConfig;
    private final LongSupplier _usedHeapSupplier;
    private final LongSupplier _maxHeapSupplier;
    private final LongSupplier _currentTimeMsSupplier;
    private final Runnable _gcRunner;

    private volatile ConfigSnapshot _config;
    private volatile boolean _throttling;
    private volatile long _lastCheckTimeMs = -1L;
    private long _lastGcRequestTimeMs = -1L;
    private volatile boolean _loggedInvalidHeapSize;
    private volatile boolean _loggedInvalidThresholds;

    ServerThrottleState(@Nullable PinotConfiguration instanceDataManagerConfig, ServerMetrics serverMetrics,
        LongSupplier usedHeapSupplier, LongSupplier maxHeapSupplier, LongSupplier currentTimeMsSupplier,
        Runnable gcRunner) {
      _serverMetrics = serverMetrics;
      _instanceDataManagerConfig =
          instanceDataManagerConfig != null ? new PinotConfiguration(instanceDataManagerConfig.toMap())
              : new PinotConfiguration();
      _usedHeapSupplier = usedHeapSupplier;
      _maxHeapSupplier = maxHeapSupplier;
      _currentTimeMsSupplier = currentTimeMsSupplier;
      _gcRunner = gcRunner;
      _config = ConfigSnapshot.fromConfig(_instanceDataManagerConfig, Map.of());
    }

    @Override
    public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
      if (containsIngestionOomProtectionConfig(changedConfigs)) {
        updateConfigFromClusterConfigs(Map.copyOf(clusterConfigs));
      }
    }

    boolean shouldThrottle() {
      long nowMs = _currentTimeMsSupplier.getAsLong();
      if (isWithinCheckInterval(nowMs)) {
        return _throttling;
      }
      synchronized (this) {
        nowMs = _currentTimeMsSupplier.getAsLong();
        if (isWithinCheckInterval(nowMs)) {
          return _throttling;
        }
        _lastCheckTimeMs = nowMs;
        return sampleAndUpdate(nowMs);
      }
    }

    boolean isThrottling() {
      return _throttling;
    }

    long getCheckIntervalMs() {
      return _config._checkIntervalMs;
    }

    ServerMode getMode() {
      return _config._mode;
    }

    void resetMetrics() {
      updateThrottling(false, 0.0);
    }

    private void updateConfigFromClusterConfigs(Map<String, String> clusterConfigs) {
      try {
        ConfigSnapshot oldConfig = _config;
        ConfigSnapshot config = ConfigSnapshot.fromConfig(_instanceDataManagerConfig, clusterConfigs);
        _config = config;
        _lastCheckTimeMs = -1L;
        _loggedInvalidThresholds = false;
        if (_throttling && (oldConfig._mode != config._mode || !config.isValidThresholdConfig())) {
          updateThrottling(false, 0.0, config);
        }
        LOGGER.info("Updated server ingestion OOM protection config: mode={}, heapUsageThrottleThreshold={}%, "
                + "heapUsageRecoveryThreshold={}%, checkIntervalMs={}, gcIntervalMs={}", config._mode,
            Math.round(config._heapUsageThrottleThreshold * 100), Math.round(config._heapUsageRecoveryThreshold * 100),
            config._checkIntervalMs, config._gcIntervalMs);
      } catch (RuntimeException e) {
        LOGGER.warn("Ignoring invalid server ingestion OOM protection cluster config update", e);
      }
    }

    private boolean sampleAndUpdate(long nowMs) {
      ConfigSnapshot config = _config;
      if (!config.isValidThresholdConfig()) {
        if (!_loggedInvalidThresholds) {
          LOGGER.warn("Disabling server ingestion OOM protection because thresholds are invalid. "
                  + "heapUsageThrottleThreshold: {}, heapUsageRecoveryThreshold: {}",
              config._heapUsageThrottleThreshold, config._heapUsageRecoveryThreshold);
          _loggedInvalidThresholds = true;
        }
        updateThrottling(false, 0.0);
        return false;
      }

      long maxHeapBytes = _maxHeapSupplier.getAsLong();
      if (maxHeapBytes <= 0) {
        if (!_loggedInvalidHeapSize) {
          LOGGER.warn("Disabling server ingestion OOM protection because max heap size is not available: {}",
              maxHeapBytes);
          _loggedInvalidHeapSize = true;
        }
        updateThrottling(false, 0.0);
        return false;
      }

      double heapUsageRatio = Math.min(1.0, Math.max(0.0, (double) _usedHeapSupplier.getAsLong() / maxHeapBytes));
      boolean shouldThrottle = _throttling ? heapUsageRatio > config._heapUsageRecoveryThreshold
          : heapUsageRatio >= config._heapUsageThrottleThreshold;
      updateThrottling(shouldThrottle, heapUsageRatio, config);
      if (shouldThrottle) {
        requestGcIfNeeded(heapUsageRatio, nowMs, config._gcIntervalMs);
      }
      return shouldThrottle;
    }

    private void updateThrottling(boolean shouldThrottle, double heapUsageRatio) {
      updateThrottling(shouldThrottle, heapUsageRatio, _config);
    }

    private void updateThrottling(boolean shouldThrottle, double heapUsageRatio, ConfigSnapshot config) {
      boolean wasThrottling = _throttling;
      _throttling = shouldThrottle;
      _serverMetrics.setValueOfGlobalGauge(ServerGauge.REALTIME_INGESTION_OOM_PROTECTION_ACTIVE,
          shouldThrottle ? 1L : 0L);
      if (shouldThrottle && !wasThrottling) {
        LOGGER.warn("Server ingestion OOM protection activated. Heap usage: {}%, threshold: {}%, "
                + "recovery threshold: {}%", Math.round(heapUsageRatio * 100),
            Math.round(config._heapUsageThrottleThreshold * 100),
            Math.round(config._heapUsageRecoveryThreshold * 100));
      } else if (shouldThrottle) {
        LOGGER.warn("Server ingestion OOM protection remains active. Heap usage: {}%, recovery threshold: {}%",
            Math.round(heapUsageRatio * 100), Math.round(config._heapUsageRecoveryThreshold * 100));
      } else if (!shouldThrottle && wasThrottling) {
        _lastGcRequestTimeMs = -1L;
        LOGGER.info("Server ingestion OOM protection released. Heap usage: {}%, recovery threshold: {}%",
            Math.round(heapUsageRatio * 100), Math.round(config._heapUsageRecoveryThreshold * 100));
      }
    }

    private void requestGcIfNeeded(double heapUsageRatio, long nowMs, long gcIntervalMs) {
      if (gcIntervalMs <= 0) {
        return;
      }
      if (_lastGcRequestTimeMs >= 0 && nowMs >= _lastGcRequestTimeMs
          && nowMs - _lastGcRequestTimeMs < gcIntervalMs) {
        return;
      }
      _lastGcRequestTimeMs = nowMs;
      LOGGER.warn("Requesting JVM GC while server ingestion OOM protection is active. Heap usage: {}%",
          Math.round(heapUsageRatio * 100));
      _gcRunner.run();
    }

    private boolean isWithinCheckInterval(long nowMs) {
      return _lastCheckTimeMs >= 0 && nowMs - _lastCheckTimeMs < _config._checkIntervalMs;
    }
  }

  private static class ConfigSnapshot {
    private final ServerMode _mode;
    private final double _heapUsageThrottleThreshold;
    private final double _heapUsageRecoveryThreshold;
    private final long _checkIntervalMs;
    private final long _gcIntervalMs;

    private ConfigSnapshot(PinotConfiguration config) {
      _mode = getMode(config.getProperty(
          CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_MODE,
          CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_MODE));
      _heapUsageThrottleThreshold = config.getProperty(
          CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_THROTTLE_THRESHOLD,
          CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_THROTTLE_THRESHOLD);
      _heapUsageRecoveryThreshold = config.getProperty(
          CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_RECOVERY_THRESHOLD,
          CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_HEAP_USAGE_RECOVERY_THRESHOLD);
      long checkIntervalMs = config.getProperty(
          CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS,
          CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS);
      _checkIntervalMs = checkIntervalMs > 0 ? checkIntervalMs
          : CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_CHECK_INTERVAL_MS;
      _gcIntervalMs = config.getProperty(
          CommonConstants.Server.CONFIG_OF_SERVER_INGESTION_OOM_PROTECTION_GC_INTERVAL_MS,
          CommonConstants.Server.DEFAULT_SERVER_INGESTION_OOM_PROTECTION_GC_INTERVAL_MS);
    }

    private static ConfigSnapshot fromConfig(PinotConfiguration instanceDataManagerConfig,
        Map<String, String> clusterConfigs) {
      Map<String, Object> mergedConfig = new HashMap<>(instanceDataManagerConfig.toMap());
      for (String configKey : SERVER_INGESTION_OOM_PROTECTION_CONFIG_KEYS) {
        String clusterConfigValue = getClusterConfigValue(clusterConfigs, configKey);
        if (clusterConfigValue != null) {
          mergedConfig.put(configKey, clusterConfigValue);
        }
      }
      return new ConfigSnapshot(new PinotConfiguration(mergedConfig));
    }

    private static String getClusterConfigValue(Map<String, String> clusterConfigs, String configKey) {
      String value = clusterConfigs.get(configKey);
      if (value != null) {
        return value;
      }
      return clusterConfigs.get(SERVER_INSTANCE_CONFIG_PREFIX + configKey);
    }

    private boolean isValidThresholdConfig() {
      return _heapUsageThrottleThreshold > 0.0 && _heapUsageThrottleThreshold < 1.0
          && _heapUsageRecoveryThreshold > 0.0 && _heapUsageRecoveryThreshold < _heapUsageThrottleThreshold;
    }
  }
}
