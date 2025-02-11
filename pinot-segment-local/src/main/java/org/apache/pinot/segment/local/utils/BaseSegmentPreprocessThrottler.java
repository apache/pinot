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
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.slf4j.Logger;


/**
 * Base class for segment preprocess throttlers, contains the common logic for the semaphore and handling the pre and
 * post query serving values. The semaphore cannot be null and must contain > 0 total permits
 */
public abstract class BaseSegmentPreprocessThrottler implements PinotClusterConfigChangeListener {

  protected AdjustableSemaphore _semaphore;
  /**
   * _maxPreprocessConcurrency and _maxPreprocessConcurrencyBeforeServingQueries must be > 0. To effectively disable
   * throttling, this can be set to a very high value
   */
  protected int _maxPreprocessConcurrency;
  protected int _maxPreprocessConcurrencyBeforeServingQueries;
  protected boolean _isServingQueries;
  private final Logger _logger;

  /**
   * Base segment preprocess throttler constructor
   * @param maxPreprocessConcurrency configured index preprocessing concurrency
   * @param maxPreprocessConcurrencyBeforeServingQueries configured preprocessing concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   * @param logger logger to use
   */
  public BaseSegmentPreprocessThrottler(int maxPreprocessConcurrency, int maxPreprocessConcurrencyBeforeServingQueries,
      boolean isServingQueries, Logger logger) {
    _logger = logger;
    _logger.info("Initializing SegmentPreprocessThrottler, maxPreprocessConcurrency: {}, "
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

    // maxPreprocessConcurrencyBeforeServingQueries is only used prior to serving queries and once the server is
    // ready to serve queries this is not used again. This too is configurable via ZK CLUSTER config updates while the
    // server is starting up.
    if (!isServingQueries) {
      logger.info("Serving queries is disabled, using preprocess concurrency as: {}",
          _maxPreprocessConcurrencyBeforeServingQueries);
    }

    _semaphore = new AdjustableSemaphore(
        _isServingQueries ? _maxPreprocessConcurrency : _maxPreprocessConcurrencyBeforeServingQueries, true);
    _logger.info("Created semaphore with total permits: {}, available permits: {}", totalPermits(),
        availablePermits());
  }

  public synchronized void startServingQueries() {
    _logger.info("Serving queries is to be enabled, reset throttling threshold for segment preprocess concurrency, "
        + "total permits: {}, available permits: {}", totalPermits(), availablePermits());
    _isServingQueries = true;
    _semaphore.setPermits(_maxPreprocessConcurrency);
    _logger.info("Reset throttling completed, new concurrency: {}, total permits: {}, available permits: {}",
        _maxPreprocessConcurrency, totalPermits(), availablePermits());
  }

  protected void handleMaxPreprocessConcurrencyChange(Set<String> changedConfigs, Map<String, String> clusterConfigs,
      String configName, String defaultConfigValue) {
    if (!changedConfigs.contains(configName)) {
      _logger.info("changedConfigs list indicates config: {} was not updated, skipping updates", configName);
      return;
    }

    String maxParallelSegmentPreprocessesStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxPreprocessConcurrency;
    try {
      maxPreprocessConcurrency = Integer.parseInt(maxParallelSegmentPreprocessesStr);
    } catch (Exception e) {
      _logger.warn("Invalid config {} set to: {}, not making change, fix config and try again", configName,
          maxParallelSegmentPreprocessesStr);
      return;
    }

    if (maxPreprocessConcurrency <= 0) {
      _logger.warn("config {}: {} must be > 0, not making change, fix config and try again", configName,
          maxPreprocessConcurrency);
      return;
    }

    if (maxPreprocessConcurrency == _maxPreprocessConcurrency) {
      _logger.info("No ZK update for config {}, value: {}, total permits: {}", configName, _maxPreprocessConcurrency,
          totalPermits());
      return;
    }

    _logger.info("Updated config: {} from: {} to: {}", configName, _maxPreprocessConcurrency,
        maxPreprocessConcurrency);
    _maxPreprocessConcurrency = maxPreprocessConcurrency;

    if (!_isServingQueries) {
      _logger.info("Serving queries hasn't been enabled yet, not updating the permits with config {}", configName);
      return;
    }
    _semaphore.setPermits(_maxPreprocessConcurrency);
    _logger.info("Updated total permits: {}", totalPermits());
  }

  protected void handleMaxPreprocessConcurrencyBeforeServingQueriesChange(Set<String> changedConfigs,
      Map<String, String> clusterConfigs, String configName, String defaultConfigValue) {
    if (!changedConfigs.contains(configName)) {
      _logger.info("changedConfigs list indicates config: {} was not updated, skipping updates", configName);
      return;
    }

    String maxParallelSegmentPreprocessesBeforeServingQueriesStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxPreprocessConcurrencyBeforeServingQueries;
    try {
      maxPreprocessConcurrencyBeforeServingQueries =
          Integer.parseInt(maxParallelSegmentPreprocessesBeforeServingQueriesStr);
    } catch (Exception e) {
      _logger.warn("Invalid config {} set to: {}, not making change, fix config and try again", configName,
          maxParallelSegmentPreprocessesBeforeServingQueriesStr);
      return;
    }

    if (maxPreprocessConcurrencyBeforeServingQueries <= 0) {
      _logger.warn("config {}: {} must be > 0, not making change, fix config and try again", configName,
          maxPreprocessConcurrencyBeforeServingQueries);
      return;
    }

    if (maxPreprocessConcurrencyBeforeServingQueries == _maxPreprocessConcurrencyBeforeServingQueries) {
      _logger.info("No ZK update for config: {} value: {}, total permits: {}", configName,
          _maxPreprocessConcurrencyBeforeServingQueries, totalPermits());
      return;
    }

    _logger.info("Updated config: {} from: {} to: {}", configName, _maxPreprocessConcurrencyBeforeServingQueries,
        maxPreprocessConcurrencyBeforeServingQueries);
    _maxPreprocessConcurrencyBeforeServingQueries = maxPreprocessConcurrencyBeforeServingQueries;
    if (!_isServingQueries) {
      _logger.info("config: {} was updated before serving queries was enabled, updating the permits", configName);
      _semaphore.setPermits(_maxPreprocessConcurrencyBeforeServingQueries);
      _logger.info("Updated total permits: {}", totalPermits());
    }
  }

  /**
   * Block trying to acquire the semaphore to perform the segment StarTree index rebuild steps unless interrupted.
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
   * Should be called after the segment StarTree index build completes. It is the responsibility of the caller to
   * ensure that this method is called exactly once for each call to {@link #acquire()}.
   */
  public void release() {
    _semaphore.release();
  }

  /**
   * Get the number of available permits
   * @return number of available permits
   */
  @VisibleForTesting
  protected int availablePermits() {
    return _semaphore.availablePermits();
  }

  /**
   * Get the total number of permits
   * @return total number of permits
   */
  @VisibleForTesting
  protected int totalPermits() {
    return _semaphore.getTotalPermits();
  }
}
