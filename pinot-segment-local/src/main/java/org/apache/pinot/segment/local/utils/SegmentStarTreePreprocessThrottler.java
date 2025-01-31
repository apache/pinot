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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to throttle the total concurrent startree index rebuilds that can happen on a given Pinot server.
 */
public class SegmentStarTreePreprocessThrottler extends BaseSegmentPreprocessThrottler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStarTreePreprocessThrottler.class);

  /**
   * _maxStarTreePreprocessConcurrency must be > 0. To effectively disable throttling, this can be set to a very high
   * value
   */
  private int _maxStarTreePreprocessConcurrency;

  /**
   * @param maxStarTreePreprocessConcurrency configured StarTree index preprocessing concurrency
   */
  public SegmentStarTreePreprocessThrottler(int maxStarTreePreprocessConcurrency) {
    super(maxStarTreePreprocessConcurrency, LOGGER);
    LOGGER.info("Initializing SegmentStarTreePreprocessThrottler, maxStarTreePreprocessConcurrency: {}",
        maxStarTreePreprocessConcurrency);
    _maxStarTreePreprocessConcurrency = maxStarTreePreprocessConcurrency;
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentStarTreePreprocessThrottler configs with unchanged clusterConfigs");
      return;
    }

    LOGGER.info("Updating SegmentStarTreePreprocessThrottler configs with latest clusterConfigs");
    handleMaxStarTreePreprocessConcurrencyChange(changedConfigs, clusterConfigs);
    LOGGER.info("Updated SegmentStarTreePreprocessThrottler configs with latest clusterConfigs");
  }

  private void handleMaxStarTreePreprocessConcurrencyChange(Set<String> changedConfigs, Map<String,
      String> clusterConfigs) {
    if (!changedConfigs.contains(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM)) {
      LOGGER.info("changedConfigs list indicates maxStarTreePreprocessConcurrency was not updated, skipping updates");
      return;
    }

    String configName = CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM;
    String defaultConfigValue = CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM;
    String maxParallelSegmentStarTreePreprocessesStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxStarTreePreprocessConcurrency;
    try {
      maxStarTreePreprocessConcurrency = Integer.parseInt(maxParallelSegmentStarTreePreprocessesStr);
    } catch (Exception e) {
      LOGGER.warn("Invalid maxStarTreePreprocessConcurrency set: {}, not making change, fix config and try again",
          maxParallelSegmentStarTreePreprocessesStr);
      return;
    }

    if (maxStarTreePreprocessConcurrency <= 0) {
      LOGGER.warn("maxStarTreePreprocessConcurrency: {} must be > 0, not making change, fix config and try again",
          maxStarTreePreprocessConcurrency);
      return;
    }

    if (maxStarTreePreprocessConcurrency == _maxStarTreePreprocessConcurrency) {
      LOGGER.info("No ZK update for maxStarTreePreprocessConcurrency {}, total permits: {}",
          _maxStarTreePreprocessConcurrency, totalPermits());
      return;
    }

    LOGGER.info("Updated maxStarTreePreprocessConcurrency from: {} to: {}", _maxStarTreePreprocessConcurrency,
        maxStarTreePreprocessConcurrency);
    _maxStarTreePreprocessConcurrency = maxStarTreePreprocessConcurrency;

    _semaphore.setPermits(_maxStarTreePreprocessConcurrency);
    LOGGER.info("Updated total StarTree index rebuild permits: {}", totalPermits());
  }
}
