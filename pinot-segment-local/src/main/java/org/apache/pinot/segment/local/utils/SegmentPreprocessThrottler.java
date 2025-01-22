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
import java.util.Map;
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
   * If _maxPreprocessConcurrency is <= 0, it means that the cluster is not configured to limit the number of
   * index rebuilds that can be executed concurrently. This class sets the semaphore permit to be a large value
   * in this scenario.
   */
  private int _maxPreprocessConcurrency;
  private boolean _relaxThrottling;
  private volatile AdjustableSemaphore _semaphore;

  public SegmentPreprocessThrottler(int maxPreprocessConcurrency, boolean relaxThrottling) {
    LOGGER.info("Initializing SegmentPreprocessThrottler, maxPreprocessConcurrency: {}, relaxThrottling: {}",
        maxPreprocessConcurrency, relaxThrottling);
    if (maxPreprocessConcurrency <= 0) {
      int defaultSegmentPreprocessParallelism =
          Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM);
      LOGGER.warn("Max preprocess concurrency is negative: {}, resetting to {}", maxPreprocessConcurrency,
          defaultSegmentPreprocessParallelism);
      maxPreprocessConcurrency = defaultSegmentPreprocessParallelism;
    }
    _maxPreprocessConcurrency = maxPreprocessConcurrency;
    _relaxThrottling = relaxThrottling;

    int relaxThrottlingThreshold = Math.max(_maxPreprocessConcurrency,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    int preprocessConcurrency = _maxPreprocessConcurrency;
    if (relaxThrottling) {
      preprocessConcurrency = relaxThrottlingThreshold;
      LOGGER.info("Relax throttling enabled, setting preprocess concurrency to: {}", preprocessConcurrency);
    }
    _semaphore = new AdjustableSemaphore(preprocessConcurrency, true);
    LOGGER.info("Created semaphore with available permits: {}", _semaphore.availablePermits());
  }

  public void resetThrottling() {
    LOGGER.info("Reset throttling threshold for segment preprocess concurrency, available permits: {}",
        _semaphore.availablePermits());
    _relaxThrottling = false;
    _semaphore.setPermits(_maxPreprocessConcurrency);
    LOGGER.info("Reset throttling completed, new concurrency: {}, available permits: {}", _maxPreprocessConcurrency,
        _semaphore.availablePermits());
  }

  @Override
  public void onChange(Map<String, String> clusterConfigs) {
    if (clusterConfigs == null || clusterConfigs.isEmpty()) {
      LOGGER.info("Skip updating SegmentPreprocessThrottler configs with empty clusterConfigs");
      return;
    }
    LOGGER.info("Updating SegmentPreprocessThrottler configs with latest clusterConfigs");
    String configName = CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM;
    String defaultConfigValue = CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM;
    String maxParallelSegmentPreprocessesStr = clusterConfigs.getOrDefault(configName, defaultConfigValue);
    int maxPreprocessConcurrency = Integer.parseInt(maxParallelSegmentPreprocessesStr);

    if (maxPreprocessConcurrency == _maxPreprocessConcurrency) {
      LOGGER.info("No ZK update for SegmentPreprocessThrottler, available permits: {}", _semaphore.availablePermits());
      return;
    }

    LOGGER.info("Updated maxPreprocessConcurrency: {} to: {}", _maxPreprocessConcurrency, maxPreprocessConcurrency);

    if (maxPreprocessConcurrency <= 0) {
      LOGGER.warn("Max preprocess concurrency set to value <= 0: {}, resetting to {}", maxPreprocessConcurrency,
          Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM));
      maxPreprocessConcurrency = Integer.parseInt(CommonConstants.Helix.DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM);
    }

    _maxPreprocessConcurrency = maxPreprocessConcurrency;
    if (_relaxThrottling) {
      LOGGER.warn("Reset throttling has not yet been called, not updating the permits");
      return;
    }

    _semaphore.setPermits(_maxPreprocessConcurrency);
    LOGGER.info("Updated SegmentPreprocessThrottler configs with latest clusterConfigs");
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
}
