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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to throttle the total concurrent deep store downloads that can happen on a given Pinot server.
 */
public class SegmentDownloadThrottler implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDownloadThrottler.class);

  /**
   * _maxDownloadConcurrency must be > 0. To effectively disable throttling, this can be set to a very high value
   */
  private int _maxDownloadConcurrency;
  private final String _tableNameWithType;
  private final AdjustableSemaphore _semaphore;

  /**
   * @param maxDownloadConcurrency configured download concurrency
   */
  public SegmentDownloadThrottler(int maxDownloadConcurrency, String tableNameWithType) {
    LOGGER.info("Initializing SegmentDownloadThrottler, maxDownloadConcurrency: {}", maxDownloadConcurrency);
    Preconditions.checkArgument(maxDownloadConcurrency > 0,
        "Max download parallelism must be > 0, but found to be: " + maxDownloadConcurrency);

    _maxDownloadConcurrency = maxDownloadConcurrency;
    _tableNameWithType = tableNameWithType;

    _semaphore = new AdjustableSemaphore(_maxDownloadConcurrency, true);
    LOGGER.info("Created download semaphore for table: {} with total permits: {}, available permits: {}",
        _tableNameWithType, totalPermits(), availablePermits());
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentDownloadThrottler configs for table: {} with unchanged clusterConfigs",
          _tableNameWithType);
      return;
    }

    LOGGER.info("Updating SegmentDownloadThrottler configs for table: {} with latest clusterConfigs",
        _tableNameWithType);
    if (!changedConfigs.contains(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS)) {
      LOGGER.info("changedConfigs list indicates config: {} for table: {} was not updated, skipping updates",
          CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, _tableNameWithType);
      return;
    }

    String configName = CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS;
    String defaultConfigValue = CommonConstants.Server.DEFAULT_MAX_PARALLEL_SEGMENT_DOWNLOADS;
    String maxParallelSegmentDownloadsStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxDownloadConcurrency;
    try {
      maxDownloadConcurrency = Integer.parseInt(maxParallelSegmentDownloadsStr);
    } catch (Exception e) {
      LOGGER.warn("Invalid config: {} for table: {} set to: {}, not making change, fix config and try again",
          CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, _tableNameWithType,
          maxParallelSegmentDownloadsStr);
      return;
    }

    if (maxDownloadConcurrency <= 0) {
      LOGGER.warn("config {} for table: {}: {} must be > 0, not making change, fix config and try again",
          CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, _tableNameWithType, maxDownloadConcurrency);
      return;
    }

    if (maxDownloadConcurrency == _maxDownloadConcurrency) {
      LOGGER.info("No ZK update for config: {} for table: {} value: {}, total permits: {}",
          CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, _tableNameWithType, _maxDownloadConcurrency,
          totalPermits());
      return;
    }

    LOGGER.info("Updated config: {} for table: {} from: {} to: {}",
        CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, _tableNameWithType, _maxDownloadConcurrency,
        maxDownloadConcurrency);
    _maxDownloadConcurrency = maxDownloadConcurrency;

    _semaphore.setPermits(_maxDownloadConcurrency);
    LOGGER.info("Updated total table download permits: {}", totalPermits());
    LOGGER.info("Updated SegmentDownloadThrottler configs for table: {} with latest clusterConfigs",
        _tableNameWithType);
  }

  /**
   * Block trying to acquire the semaphore to perform the segment index rebuild steps unless interrupted.
   * <p>
   * {@link #release()} should be called after the segment download completes. It is the responsibility of the caller
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

  /**
   * Get the estimated number of threads waiting for the semaphore
   * @return the estimated number of threads waiting for the semaphore
   */
  public int getQueueLength() {
    return _semaphore.getQueueLength();
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
