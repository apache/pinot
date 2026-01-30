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
package org.apache.pinot.server.starter.helix;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KeepPipelineBreakerStatsPredicate implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeepPipelineBreakerStatsPredicate.class);

  private volatile boolean _enabled;

  public KeepPipelineBreakerStatsPredicate(boolean enabled) {
    _enabled = enabled;
  }

  // NOTE: When this method is called, the helix manager is not yet connected.
  public static KeepPipelineBreakerStatsPredicate create(PinotConfiguration serverConf) {
    boolean enabled = serverConf.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_KEEP_PIPELINE_BREAKER_STATS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_KEEP_PIPELINE_BREAKER_STATS);
    return new KeepPipelineBreakerStatsPredicate(enabled);
  }

  public boolean isEnabled() {
    return _enabled;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    String key = CommonConstants.MultiStageQueryRunner.KEY_OF_KEEP_PIPELINE_BREAKER_STATS;
    if (!changedConfigs.contains(key)) {
      LOGGER.debug("No change for key: {}, keeping its value as {}", key, _enabled);
      return;
    }
    String value = clusterConfigs.get(key);
    if (value == null || value.isEmpty()) {
      LOGGER.info("Empty or null value for key: {}, reset to default: {}",
          key,
          CommonConstants.MultiStageQueryRunner.DEFAULT_KEEP_PIPELINE_BREAKER_STATS);
      _enabled = CommonConstants.MultiStageQueryRunner.DEFAULT_KEEP_PIPELINE_BREAKER_STATS;
    } else {
      boolean oldEnabled = _enabled;
      String valueStr = value.trim();
      _enabled = Boolean.parseBoolean(valueStr.toLowerCase(Locale.ENGLISH));
      if (oldEnabled != _enabled) {
        LOGGER.warn("Updated {} from: {} to: {}, parsed as {}", key, valueStr, oldEnabled, _enabled);
      } else {
        LOGGER.info("{} kept as {}", key, value);
      }
    }
  }
}
