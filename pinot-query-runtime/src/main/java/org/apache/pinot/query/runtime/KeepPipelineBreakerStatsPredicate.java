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
package org.apache.pinot.query.runtime;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KeepPipelineBreakerStatsPredicate implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeepPipelineBreakerStatsPredicate.class);

  private volatile boolean _skip;

  public KeepPipelineBreakerStatsPredicate(boolean skip) {
    _skip = skip;
  }

  // NOTE: When this method is called, the helix manager is not yet connected.
  public static KeepPipelineBreakerStatsPredicate create(PinotConfiguration serverConf) {
    boolean skip = serverConf.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_SKIP_PIPELINE_BREAKER_STATS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_SKIP_PIPELINE_BREAKER_STATS);
    LOGGER.info("Initialized {} with value: {}",
        CommonConstants.MultiStageQueryRunner.KEY_OF_SKIP_PIPELINE_BREAKER_STATS, skip);
    return new KeepPipelineBreakerStatsPredicate(skip);
  }

  public boolean isEnabled() {
    return !_skip;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    String key = CommonConstants.MultiStageQueryRunner.KEY_OF_SKIP_PIPELINE_BREAKER_STATS;
    if (!changedConfigs.contains(key)) {
      LOGGER.debug("No change for key: {}, keeping its value as {}", key, _skip);
      return;
    }
    String value = clusterConfigs.get(key);
    if (value == null || value.isEmpty()) {
      LOGGER.info("Empty or null value for key: {}, reset to default: {}",
          key,
          CommonConstants.MultiStageQueryRunner.DEFAULT_SKIP_PIPELINE_BREAKER_STATS);
      _skip = CommonConstants.MultiStageQueryRunner.DEFAULT_SKIP_PIPELINE_BREAKER_STATS;
    } else {
      boolean oldSkip = _skip;
      String valueStr = value.trim();
      _skip = Boolean.parseBoolean(valueStr);
      if (oldSkip != _skip) {
        LOGGER.info("Updated {} from: {} to: {}, parsed as {}", key, oldSkip, valueStr, _skip);
      } else {
        LOGGER.info("{} kept as {}", key, value);
      }
    }
  }
}
