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
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerRateLimitConfigChangeListener implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRateLimitConfigChangeListener.class);
  private final ServerMetrics _serverMetrics;

  public ServerRateLimitConfigChangeListener(ServerMetrics serverMetrics) {
    _serverMetrics = serverMetrics;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT)) {
      LOGGER.info("ChangedConfigs: {} does not contain: {}. Skipping updates", changedConfigs,
          CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT);
      return;
    }
    // Init serverRateLimit as default rate limit in-case serverRateLimit config is deleted/removed from cluster
    // configs.
    double serverRateLimit = CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT;
    if (clusterConfigs.containsKey(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT)) {
      try {
        serverRateLimit =
            Double.parseDouble(clusterConfigs.get(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT));
      } catch (NumberFormatException e) {
        LOGGER.error("Invalid rate limit config value: {}. Ignoring the config change",
            clusterConfigs.get(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT), e);
        return;
      }
    }
    RealtimeConsumptionRateManager.getInstance().updateServerRateLimiter(serverRateLimit, _serverMetrics);
  }
}
