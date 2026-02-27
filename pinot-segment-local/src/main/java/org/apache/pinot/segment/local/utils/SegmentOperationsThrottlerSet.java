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
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains all the segment operation throttlers used to control the total parallel segment related operations that can
 * happen on a given Pinot server. For now this class supports download throttling and index rebuild throttling at the
 * following levels:
 * - All index rebuild throttling
 * - StarTree index rebuild throttling
 * - Server level segment download throttling (this is taken after the table level download semaphore is taken)
 * Code paths that do not need to download or rebuild the index or which don't happen on the server need not utilize
 * this throttler. The throttlers passed in for now cannot be 'null', instead for code paths that do not need
 * throttling, this object itself will be passed in as 'null'.
 */
public class SegmentOperationsThrottlerSet implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOperationsThrottlerSet.class);

  private final BaseSegmentOperationsThrottler _segmentAllIndexPreprocessThrottler;
  private final BaseSegmentOperationsThrottler _segmentStarTreePreprocessThrottler;
  private final BaseSegmentOperationsThrottler _segmentMultiColTextIndexPreprocessThrottler;
  private final BaseSegmentOperationsThrottler _segmentDownloadThrottler;

  /**
   * Constructor for SegmentOperationsThrottlerSet
   * @param segmentAllIndexPreprocessThrottler segment preprocess throttler to use for all indexes
   * @param segmentStarTreePreprocessThrottler segment preprocess throttler to use for StarTree index
   * @param segmentDownloadThrottler segment download throttler to throttle download at server level
   * @param segmentMultiColTextIndexPreprocessThrottler segment preprocess throttler for multi-col text index
   */
  public SegmentOperationsThrottlerSet(BaseSegmentOperationsThrottler segmentAllIndexPreprocessThrottler,
      BaseSegmentOperationsThrottler segmentStarTreePreprocessThrottler,
      BaseSegmentOperationsThrottler segmentDownloadThrottler,
      BaseSegmentOperationsThrottler segmentMultiColTextIndexPreprocessThrottler) {
    _segmentAllIndexPreprocessThrottler = segmentAllIndexPreprocessThrottler;
    _segmentStarTreePreprocessThrottler = segmentStarTreePreprocessThrottler;
    _segmentDownloadThrottler = segmentDownloadThrottler;
    _segmentMultiColTextIndexPreprocessThrottler = segmentMultiColTextIndexPreprocessThrottler;
  }

  public BaseSegmentOperationsThrottler getSegmentAllIndexPreprocessThrottler() {
    return _segmentAllIndexPreprocessThrottler;
  }

  public BaseSegmentOperationsThrottler getSegmentStarTreePreprocessThrottler() {
    return _segmentStarTreePreprocessThrottler;
  }

  public BaseSegmentOperationsThrottler getSegmentMultiColTextIndexPreprocessThrottler() {
    return _segmentMultiColTextIndexPreprocessThrottler;
  }

  public BaseSegmentOperationsThrottler getSegmentDownloadThrottler() {
    return _segmentDownloadThrottler;
  }

  /**
   * The ServerMetrics may be created after these objects are created. In that case, the initialization that happens
   * in the constructor may have occurred on the NOOP metrics. This should be called after the server metrics are
   * created and registered
   */
  public void initializeMetrics() {
    _segmentAllIndexPreprocessThrottler.initializeMetrics();
    _segmentStarTreePreprocessThrottler.initializeMetrics();
    _segmentDownloadThrottler.initializeMetrics();
    _segmentMultiColTextIndexPreprocessThrottler.initializeMetrics();
  }

  public synchronized void startServingQueries() {
    _segmentAllIndexPreprocessThrottler.startServingQueries();
    _segmentStarTreePreprocessThrottler.startServingQueries();
    _segmentDownloadThrottler.startServingQueries();
    _segmentMultiColTextIndexPreprocessThrottler.startServingQueries();
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentOperationsThrottlerSet configs with unchanged clusterConfigs");
      return;
    }

    LOGGER.info("Updating SegmentOperationsThrottlerSet configs with latest clusterConfigs");

    // Update all index preprocess throttler
    updateThrottlerIfConfigChanged(changedConfigs, clusterConfigs,
        _segmentAllIndexPreprocessThrottler,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);

    // Update startree preprocess throttler
    updateThrottlerIfConfigChanged(changedConfigs, clusterConfigs,
        _segmentStarTreePreprocessThrottler,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);

    // Update download throttler
    updateThrottlerIfConfigChanged(changedConfigs, clusterConfigs,
        _segmentDownloadThrottler,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);

    // Update multi-col text index preprocess throttler
    updateThrottlerIfConfigChanged(changedConfigs, clusterConfigs,
        _segmentMultiColTextIndexPreprocessThrottler,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);

    LOGGER.info("Updated SegmentOperationsThrottlerSet configs with latest clusterConfigs");
  }

  protected void updateThrottlerIfConfigChanged(Set<String> changedConfigs, Map<String, String> clusterConfigs,
      BaseSegmentOperationsThrottler throttler, String maxConcurrencyConfigKey, String maxConcurrencyDefault,
      String maxConcurrencyBeforeServingQueriesConfigKey, String maxConcurrencyBeforeServingQueriesDefault) {
    String throttlerName = throttler.getThrottlerName();
    boolean maxConcurrencyChanged = changedConfigs.contains(maxConcurrencyConfigKey);
    boolean maxConcurrencyBeforeServingQueriesChanged =
        changedConfigs.contains(maxConcurrencyBeforeServingQueriesConfigKey);

    if (!maxConcurrencyChanged && !maxConcurrencyBeforeServingQueriesChanged) {
      LOGGER.debug("No config changes for {}", throttlerName);
      return;
    }

    // Parse maxConcurrency
    int maxConcurrency = parseConfigValue(clusterConfigs, maxConcurrencyConfigKey, maxConcurrencyDefault);
    if (maxConcurrency <= 0) {
      return;
    }

    // Parse maxConcurrencyBeforeServingQueries
    int maxConcurrencyBeforeServingQueries =
        parseConfigValue(clusterConfigs, maxConcurrencyBeforeServingQueriesConfigKey,
            maxConcurrencyBeforeServingQueriesDefault);
    if (maxConcurrencyBeforeServingQueries <= 0) {
      return;
    }

    // Update throttler
    LOGGER.info("Updating {} throttler with maxConcurrency: {}, maxConcurrencyBeforeServingQueries: {}",
        throttlerName, maxConcurrency, maxConcurrencyBeforeServingQueries);
    throttler.updatePermits(maxConcurrency, maxConcurrencyBeforeServingQueries);
  }

  protected int parseConfigValue(Map<String, String> clusterConfigs, String configKey, String defaultValue) {
    String valueStr = clusterConfigs == null ? defaultValue : clusterConfigs.getOrDefault(configKey, defaultValue);
    try {
      return Integer.parseInt(valueStr);
    } catch (Exception e) {
      LOGGER.warn("Invalid config {} set to: {}, not making change, fix config and try again", configKey, valueStr);
      return -1;
    }
  }
}
