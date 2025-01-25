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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to throttle the total concurrent index rebuilds that can happen on a given Pinot server.
 * Code paths that do no need to rebuild the index or which don't happen on the server need not utilize this throttler.
 */
public class SegmentPreprocessThrottler implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreprocessThrottler.class);

  /**
   * _maxPreprocessConcurrency and _maxConcurrentPreprocessesBeforeServingQueries must be >= 0. To effectively disable
   * throttling, this can be set to a very high value
   */
  private int _maxPreprocessConcurrency;
  private int _maxPreprocessConcurrencyBeforeServingQueries;
  private boolean _isServingQueries;
  private final AdjustableSemaphore _semaphore;

  /**
   * @param maxPreprocessConcurrency configured preprocessing concurrency
   * @param maxPreprocessConcurrencyBeforeServingQueries configured preprocessing concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   */
  public SegmentPreprocessThrottler(int maxPreprocessConcurrency, int maxPreprocessConcurrencyBeforeServingQueries,
      boolean isServingQueries) {
    LOGGER.info("Initializing SegmentPreprocessThrottler, maxPreprocessConcurrency: {}, "
            + "maxPreprocessConcurrencyBeforeServingQueries: {}, isServingQueries: {}",
        maxPreprocessConcurrency, maxPreprocessConcurrencyBeforeServingQueries, isServingQueries);
    Preconditions.checkArgument(maxPreprocessConcurrency > 0,
        "Max preprocess parallelism must be > 0, but found to be: " + maxPreprocessConcurrency);
    Preconditions.checkArgument(maxPreprocessConcurrencyBeforeServingQueries > 0,
        "Max preprocess parallelism before serving queries must be > 0, but found to be: "
            + maxPreprocessConcurrencyBeforeServingQueries);

    _maxPreprocessConcurrency = maxPreprocessConcurrency;
    _maxPreprocessConcurrencyBeforeServingQueries = maxPreprocessConcurrencyBeforeServingQueries;
    _isServingQueries = isServingQueries;

    // maxConcurrentPreprocessesBeforeServingQueries is only used prior to serving queries and once the server is
    // ready to serve queries this is not used again. Thus, it is safe to only pick up this configuration during
    // server startup. There is no need to allow updates to this via the ZK CLUSTER config handler
    int preServeQueryParallelism = Math.max(_maxPreprocessConcurrency, _maxPreprocessConcurrencyBeforeServingQueries);
    int preprocessConcurrency = _maxPreprocessConcurrency;
    if (!isServingQueries) {
      preprocessConcurrency = preServeQueryParallelism;
      LOGGER.info("Serving queries is disabled, setting preprocess concurrency to: {}", preprocessConcurrency);
    }
    _semaphore = new AdjustableSemaphore(preprocessConcurrency, true);
    LOGGER.info("Created semaphore with total permits: {}, available permits: {}", totalPermits(),
        availablePermits());
  }

  public synchronized void startServingQueries() {
    LOGGER.info("Serving queries is to be enabled, reset throttling threshold for segment preprocess concurrency, "
        + "total permits: {}, available permits: {}", totalPermits(), availablePermits());
    _isServingQueries = true;
    _semaphore.setPermits(_maxPreprocessConcurrency);
    LOGGER.info("Reset throttling completed, new concurrency: {}, total permits: {}, available permits: {}",
        _maxPreprocessConcurrency, totalPermits(), availablePermits());
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentPreprocessThrottler configs with unchanged clusterConfigs");
      return;
    }

    LOGGER.info("Updating SegmentPreprocessThrottler configs with latest clusterConfigs");
    handleMaxPreprocessConcurrencyChange(changedConfigs, clusterConfigs);
    handleMaxPreprocessConcurrencyBeforeServingQueriesChange(changedConfigs, clusterConfigs);
    LOGGER.info("Updated SegmentPreprocessThrottler configs with latest clusterConfigs");
  }

  private void handleMaxPreprocessConcurrencyChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM)) {
      LOGGER.info("changedConfigs list indicates maxPreprocessConcurrency was not updated, skipping updates");
      return;
    }

    String configName = CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM;
    String defaultConfigValue = CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM;
    String maxParallelSegmentPreprocessesStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxPreprocessConcurrency;
    try {
      maxPreprocessConcurrency = Integer.parseInt(maxParallelSegmentPreprocessesStr);
    } catch (Exception e) {
      LOGGER.warn("Invalid maxPreprocessConcurrency set: {}, not making change, fix config and try again",
          maxParallelSegmentPreprocessesStr);
      return;
    }

    if (maxPreprocessConcurrency <= 0) {
      LOGGER.warn("maxPreprocessConcurrency: {} must be > 0, not making change, fix config and try again",
          maxPreprocessConcurrency);
      return;
    }

    if (maxPreprocessConcurrency == _maxPreprocessConcurrency) {
      LOGGER.info("No ZK update for maxPreprocessConcurrency {}, total permits: {}", _maxPreprocessConcurrency,
          totalPermits());
      return;
    }

    LOGGER.info("Updated maxPreprocessConcurrency from: {} to: {}", _maxPreprocessConcurrency,
        maxPreprocessConcurrency);
    _maxPreprocessConcurrency = maxPreprocessConcurrency;

    if (!_isServingQueries) {
      LOGGER.info("Serving queries hasn't been enabled yet, not updating the permits with maxPreprocessConcurrency");
      return;
    }
    _semaphore.setPermits(_maxPreprocessConcurrency);
    LOGGER.info("Updated total permits: {}", totalPermits());
  }

  private void handleMaxPreprocessConcurrencyBeforeServingQueriesChange(Set<String> changedConfigs,
      Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES)) {
      LOGGER.info("changedConfigs list indicates maxPreprocessConcurrencyBeforeServingQueries was not updated, "
          + "skipping updates");
      return;
    }

    String configName = CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES;
    String defaultConfigValue = CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES;
    String maxParallelSegmentPreprocessesBeforeServingQueriesStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxPreprocessConcurrencyBeforeServingQueries;
    try {
      maxPreprocessConcurrencyBeforeServingQueries =
          Integer.parseInt(maxParallelSegmentPreprocessesBeforeServingQueriesStr);
    } catch (Exception e) {
      LOGGER.warn("Invalid maxPreprocessConcurrencyBeforeServingQueries set: {}, not making change, fix config and "
              + "try again", maxParallelSegmentPreprocessesBeforeServingQueriesStr);
      return;
    }

    if (maxPreprocessConcurrencyBeforeServingQueries <= 0) {
      LOGGER.warn("maxPreprocessConcurrencyBeforeServingQueries: {} must be > 0, not making change, fix config "
          + "and try again", maxPreprocessConcurrencyBeforeServingQueries);
      return;
    }

    if (maxPreprocessConcurrencyBeforeServingQueries == _maxPreprocessConcurrencyBeforeServingQueries) {
      LOGGER.info("No ZK update for maxPreprocessConcurrencyBeforeServingQueries {}, total permits: {}",
          _maxPreprocessConcurrencyBeforeServingQueries, totalPermits());
      return;
    }

    LOGGER.info("Updated maxPreprocessConcurrencyBeforeServingQueries from: {} to: {}",
        _maxPreprocessConcurrencyBeforeServingQueries, maxPreprocessConcurrencyBeforeServingQueries);
    _maxPreprocessConcurrencyBeforeServingQueries = maxPreprocessConcurrencyBeforeServingQueries;
    if (!_isServingQueries) {
      LOGGER.info("maxPreprocessConcurrencyBeforeServingQueries was updated before serving queries was enabled, "
          + "updating the permits");
      _semaphore.setPermits(_maxPreprocessConcurrencyBeforeServingQueries);
      LOGGER.info("Updated total permits: {}", totalPermits());
    }
  }

  /**
   * Block trying to acquire the semaphore to perform the segment index rebuild steps unless interrupted.
   * <p>
   * {@link #release()} should be called after the segment preprocess completes. It is the responsibility of the caller
   * to ensure that {@link #release()} is called exactly once for each call to this method.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public void acquire()
      throws InterruptedException {
    _semaphore.acquire();
  }

  /**
   * Should be called after the segment index build completes. It is the responsibility of the caller to ensure that
   * this method is called exactly once for each call to {@link #acquire()}.
   */
  public void release() {
    _semaphore.release();
  }

  @VisibleForTesting
  int availablePermits() {
    return _semaphore.availablePermits();
  }

  @VisibleForTesting
  int totalPermits() {
    return _semaphore.getTotalPermits();
  }
}
