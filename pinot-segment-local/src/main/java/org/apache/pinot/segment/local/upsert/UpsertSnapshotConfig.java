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
package org.apache.pinot.segment.local.upsert;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Holds server-scoped, live-updatable configuration for the upsert snapshot path.
///
/// The value is read at server startup from [PinotConfiguration] and refreshed at runtime through the
/// [PinotClusterConfigChangeListener] SPI when the corresponding cluster config key
/// ([CommonConstants.Server#CONFIG_OF_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS]) is PATCHed via the cluster-config API.
/// Callers should read the value directly with [#getSegmentLockTimeoutMs()] on every use so a change in cluster
/// config takes effect on the next call.
///
/// Thread-safety: all reads and writes to the timeout value are atomic (backed by a `volatile long`). Concurrent
/// readers on the snapshot path may observe a stale value across an in-flight `onChange()`; callers that require a
/// consistent value within a critical section should snapshot the value into a local variable up-front.
public final class UpsertSnapshotConfig implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertSnapshotConfig.class);

  private static volatile long _segmentLockTimeoutMs =
      CommonConstants.Server.DEFAULT_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS;

  private UpsertSnapshotConfig() {
  }

  private static final UpsertSnapshotConfig INSTANCE = new UpsertSnapshotConfig();

  public static UpsertSnapshotConfig getInstance() {
    return INSTANCE;
  }

  public static long getSegmentLockTimeoutMs() {
    return _segmentLockTimeoutMs;
  }

  /// Initialize the holder from the server startup configuration. Safe to call multiple times; only the last value
  /// wins, matching the behavior of a cluster-config PATCH.
  public static void init(PinotConfiguration serverConfig) {
    long configured = serverConfig.getProperty(CommonConstants.Server.CONFIG_OF_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS,
        CommonConstants.Server.DEFAULT_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS);
    setSegmentLockTimeoutMs(configured);
  }

  private static void setSegmentLockTimeoutMs(long value) {
    if (value < 0) {
      LOGGER.warn("Ignoring negative upsert snapshot segment lock timeout: {} ms", value);
      return;
    }
    long previous = _segmentLockTimeoutMs;
    if (previous != value) {
      _segmentLockTimeoutMs = value;
      LOGGER.info("Updated upsert snapshot segment lock timeout: {} ms -> {} ms", previous, value);
    }
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs == null
        || !changedConfigs.contains(CommonConstants.Server.CONFIG_OF_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS)) {
      return;
    }
    String raw = clusterConfigs.get(CommonConstants.Server.CONFIG_OF_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS);
    if (raw == null) {
      // Cluster config was cleared; revert to default.
      setSegmentLockTimeoutMs(CommonConstants.Server.DEFAULT_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS);
      return;
    }
    try {
      setSegmentLockTimeoutMs(Long.parseLong(raw.trim()));
    } catch (NumberFormatException e) {
      LOGGER.warn("Cluster config {} has non-numeric value: '{}', keeping current: {} ms",
          CommonConstants.Server.CONFIG_OF_UPSERT_SNAPSHOT_SEGMENT_LOCK_TIMEOUT_MS, raw, _segmentLockTimeoutMs);
    }
  }
}
