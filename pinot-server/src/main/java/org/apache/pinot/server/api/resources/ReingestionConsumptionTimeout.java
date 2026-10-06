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
package org.apache.pinot.server.api.resources;

import com.google.common.primitives.Longs;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_REINGESTION_CONSUMPTION_TIMEOUT_MS;


/// Holds the consumption timeout of the pauseless segment re-ingestion jobs run by [ReingestionResource]. The cluster
/// config takes precedence over the server config, which takes precedence over the default. Cluster config changes are
/// applied as they arrive. A non-numeric or non-positive value is ignored with a warning when the config is loaded or
/// changed.
///
/// Thread-safe: cluster config changes are delivered one at a time, and the latest value is visible to all readers.
@ThreadSafe
public class ReingestionConsumptionTimeout implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReingestionConsumptionTimeout.class);

  // Timeout from the server config or the default, used when the cluster config is absent or invalid
  private final long _serverTimeoutMs;
  private volatile long _timeoutMs;

  public ReingestionConsumptionTimeout(PinotConfiguration serverConf) {
    Long serverTimeoutMs =
        parseTimeoutMs(serverConf.getProperty(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS), "server");
    _serverTimeoutMs = serverTimeoutMs != null ? serverTimeoutMs : DEFAULT_REINGESTION_CONSUMPTION_TIMEOUT_MS;
    _timeoutMs = _serverTimeoutMs;
    LOGGER.info("Initialized re-ingestion consumption timeout to: {}ms", _timeoutMs);
  }

  public long getTimeoutMs() {
    return _timeoutMs;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS)) {
      return;
    }
    Long clusterTimeoutMs = parseTimeoutMs(clusterConfigs.get(CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS), "cluster");
    _timeoutMs = clusterTimeoutMs != null ? clusterTimeoutMs : _serverTimeoutMs;
    LOGGER.info("Updated re-ingestion consumption timeout to: {}ms", _timeoutMs);
  }

  @Nullable
  private static Long parseTimeoutMs(@Nullable String value, String configSource) {
    if (value == null) {
      return null;
    }
    Long timeoutMs = Longs.tryParse(value.trim());
    if (timeoutMs == null || timeoutMs <= 0) {
      LOGGER.warn("Ignoring invalid {} config: {}={}, expecting a positive number of milliseconds", configSource,
          CONFIG_OF_REINGESTION_CONSUMPTION_TIMEOUT_MS, value);
      return null;
    }
    return timeoutMs;
  }
}
