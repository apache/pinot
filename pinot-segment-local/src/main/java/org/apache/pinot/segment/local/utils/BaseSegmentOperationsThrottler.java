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
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.slf4j.Logger;


/**
 * Base class for segment operation throttlers, contains the common logic for the semaphore and handling the pre and
 * post query serving values. The semaphore cannot be null and must contain > 0 total permits
 */
public abstract class BaseSegmentOperationsThrottler implements PinotClusterConfigChangeListener {

  protected final ServerMetrics _serverMetrics = ServerMetrics.get();

  protected AdjustableSemaphore _semaphore;
  /**
   * _maxConcurrency and _maxConcurrencyBeforeServingQueries must be > 0. To effectively disable throttling, this can
   * be set to a very high value
   */
  protected int _maxConcurrency;
  protected int _maxConcurrencyBeforeServingQueries;
  protected boolean _isServingQueries;
  protected ServerGauge _thresholdGauge;
  protected ServerGauge _countGauge;
  private final Logger _logger;

  /**
   * Base segment operations throttler constructor
   * @param maxConcurrency configured concurrency
   * @param maxConcurrencyBeforeServingQueries configured concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   * @param thresholdGauge gauge metric to track the throttle thresholds
   * @param countGauge gauge metric to track the number of segments undergoing the given operation
   * @param logger logger to use
   */
  public BaseSegmentOperationsThrottler(int maxConcurrency, int maxConcurrencyBeforeServingQueries,
      boolean isServingQueries, ServerGauge thresholdGauge, ServerGauge countGauge, Logger logger) {
    _logger = logger;
    _logger.info("Initializing SegmentOperationsThrottler, maxConcurrency: {}, maxConcurrencyBeforeServingQueries: {}, "
            + "isServingQueries: {}",
        maxConcurrency, maxConcurrencyBeforeServingQueries, isServingQueries);
    Preconditions.checkArgument(maxConcurrency > 0, "Max parallelism must be > 0, but found to be: " + maxConcurrency);
    Preconditions.checkArgument(maxConcurrencyBeforeServingQueries > 0,
        "Max parallelism before serving queries must be > 0, but found to be: " + maxConcurrencyBeforeServingQueries);

    _maxConcurrency = maxConcurrency;
    _maxConcurrencyBeforeServingQueries = maxConcurrencyBeforeServingQueries;
    _isServingQueries = isServingQueries;
    _thresholdGauge = thresholdGauge;
    _countGauge = countGauge;

    // maxConcurrencyBeforeServingQueries is only used prior to serving queries and once the server is
    // ready to serve queries this is not used again. This too is configurable via ZK CLUSTER config updates while the
    // server is starting up.
    if (!isServingQueries) {
      logger.info("Serving queries is disabled, using concurrency as: {}", _maxConcurrencyBeforeServingQueries);
    }

    int concurrency = _isServingQueries ? _maxConcurrency : _maxConcurrencyBeforeServingQueries;
    _semaphore = new AdjustableSemaphore(concurrency, true);
    _serverMetrics.setValueOfGlobalGauge(_thresholdGauge, concurrency);
    _serverMetrics.setValueOfGlobalGauge(_countGauge, 0);
    _logger.info("Created semaphore with total permits: {}, available permits: {}", totalPermits(),
        availablePermits());
  }

  public synchronized void startServingQueries() {
    _logger.info("Serving queries is to be enabled, reset throttling threshold for segment operations concurrency, "
        + "total permits: {}, available permits: {}", totalPermits(), availablePermits());
    _isServingQueries = true;
    _semaphore.setPermits(_maxConcurrency);
    _serverMetrics.setValueOfGlobalGauge(_thresholdGauge, _maxConcurrency);
    _logger.info("Reset throttling completed, new concurrency: {}, total permits: {}, available permits: {}",
        _maxConcurrency, totalPermits(), availablePermits());
  }

  protected void handleMaxConcurrencyChange(Set<String> changedConfigs, Map<String, String> clusterConfigs,
      String configName, String defaultConfigValue) {
    if (!changedConfigs.contains(configName)) {
      _logger.info("changedConfigs list indicates config: {} was not updated, skipping updates", configName);
      return;
    }

    String maxParallelSegmentOperationsStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxConcurrency;
    try {
      maxConcurrency = Integer.parseInt(maxParallelSegmentOperationsStr);
    } catch (Exception e) {
      _logger.warn("Invalid config {} set to: {}, not making change, fix config and try again", configName,
          maxParallelSegmentOperationsStr);
      return;
    }

    if (maxConcurrency <= 0) {
      _logger.warn("config {}: {} must be > 0, not making change, fix config and try again", configName,
          maxConcurrency);
      return;
    }

    if (maxConcurrency == _maxConcurrency) {
      _logger.info("No ZK update for config {}, value: {}, total permits: {}", configName, _maxConcurrency,
          totalPermits());
      return;
    }

    _logger.info("Updated config: {} from: {} to: {}", configName, _maxConcurrency, maxConcurrency);
    _maxConcurrency = maxConcurrency;

    if (!_isServingQueries) {
      _logger.info("Serving queries hasn't been enabled yet, not updating the permits with config {}", configName);
      return;
    }
    _semaphore.setPermits(_maxConcurrency);
    _serverMetrics.setValueOfGlobalGauge(_thresholdGauge, _maxConcurrency);
    _logger.info("Updated total permits: {}", totalPermits());
  }

  protected void handleMaxConcurrencyBeforeServingQueriesChange(Set<String> changedConfigs,
      Map<String, String> clusterConfigs, String configName, String defaultConfigValue) {
    if (!changedConfigs.contains(configName)) {
      _logger.info("changedConfigs list indicates config: {} was not updated, skipping updates", configName);
      return;
    }

    String maxParallelSegmentOperationsBeforeServingQueriesStr =
        clusterConfigs == null ? defaultConfigValue : clusterConfigs.getOrDefault(configName, defaultConfigValue);

    int maxConcurrencyBeforeServingQueries;
    try {
      maxConcurrencyBeforeServingQueries = Integer.parseInt(maxParallelSegmentOperationsBeforeServingQueriesStr);
    } catch (Exception e) {
      _logger.warn("Invalid config {} set to: {}, not making change, fix config and try again", configName,
          maxParallelSegmentOperationsBeforeServingQueriesStr);
      return;
    }

    if (maxConcurrencyBeforeServingQueries <= 0) {
      _logger.warn("config {}: {} must be > 0, not making change, fix config and try again", configName,
          maxConcurrencyBeforeServingQueries);
      return;
    }

    if (maxConcurrencyBeforeServingQueries == _maxConcurrencyBeforeServingQueries) {
      _logger.info("No ZK update for config: {} value: {}, total permits: {}", configName,
          _maxConcurrencyBeforeServingQueries, totalPermits());
      return;
    }

    _logger.info("Updated config: {} from: {} to: {}", configName, _maxConcurrencyBeforeServingQueries,
        maxConcurrencyBeforeServingQueries);
    _maxConcurrencyBeforeServingQueries = maxConcurrencyBeforeServingQueries;
    if (!_isServingQueries) {
      _logger.info("config: {} was updated before serving queries was enabled, updating the permits", configName);
      _semaphore.setPermits(_maxConcurrencyBeforeServingQueries);
      _serverMetrics.setValueOfGlobalGauge(_thresholdGauge, _maxConcurrencyBeforeServingQueries);
      _logger.info("Updated total permits: {}", totalPermits());
    }
  }

  /**
   * Block trying to acquire the semaphore to perform the segment StarTree index rebuild steps unless interrupted.
   * <p>
   * {@link #release()} should be called after the segment operation completes. It is the responsibility of the caller
   * to ensure that {@link #release()} is called exactly once for each call to this method.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public void acquire()
      throws InterruptedException {
    _semaphore.acquire();
    _serverMetrics.addValueToGlobalGauge(_countGauge, 1L);
  }

  /**
   * Should be called after the segment StarTree index build completes. It is the responsibility of the caller to
   * ensure that this method is called exactly once for each call to {@link #acquire()}.
   */
  public void release() {
    _semaphore.release();
    _serverMetrics.addValueToGlobalGauge(_countGauge, -1L);
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
