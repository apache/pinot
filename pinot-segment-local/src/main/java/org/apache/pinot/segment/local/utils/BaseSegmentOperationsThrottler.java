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
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.concurrency.AdjustableSemaphore;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for segment operation throttlers, contains the common logic for the semaphore and handling the pre and
 * post query serving values. The semaphore cannot be null and must contain > 0 total permits
 */
public class BaseSegmentOperationsThrottler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSegmentOperationsThrottler.class);
  protected ServerMetrics _serverMetrics;
  protected AdjustableSemaphore _semaphore;
  /**
   * _maxConcurrency and _maxConcurrencyBeforeServingQueries must be > 0. To effectively disable throttling, this can
   * be set to a very high value
   */
  protected int _maxConcurrency;
  protected int _maxConcurrencyBeforeServingQueries;
  protected boolean _isServingQueries;
  private AtomicInteger _numSegmentsAcquiredSemaphore;
  private String _throttlerName;
  @Nullable
  private final ServerGauge _thresholdGauge;
  @Nullable
  private final ServerGauge _countGauge;

  /**
   * Base segment operations throttler constructor
   * @param maxConcurrency configured concurrency
   * @param maxConcurrencyBeforeServingQueries configured concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   */
  @VisibleForTesting
  public BaseSegmentOperationsThrottler(int maxConcurrency, int maxConcurrencyBeforeServingQueries,
      boolean isServingQueries) {
    this(maxConcurrency, maxConcurrencyBeforeServingQueries, isServingQueries, null, null, "");
  }

  /**
   * Base segment operations throttler constructor
   * @param maxConcurrency configured concurrency
   * @param maxConcurrencyBeforeServingQueries configured concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   * @param throttlerName name of the throttler to be used in logging
   */
  public BaseSegmentOperationsThrottler(int maxConcurrency, int maxConcurrencyBeforeServingQueries,
      boolean isServingQueries, String throttlerName) {
    this(maxConcurrency, maxConcurrencyBeforeServingQueries, isServingQueries, null, null, throttlerName);
  }

  /**
   * Base segment operations throttler constructor with gauge parameters
   * @param maxConcurrency configured concurrency
   * @param maxConcurrencyBeforeServingQueries configured concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   * @param thresholdGauge gauge for tracking the throttle threshold metric, or null to skip
   * @param countGauge gauge for tracking the count metric, or null to skip
   * @param throttlerName name of the throttler to be used in logging
   */
  public BaseSegmentOperationsThrottler(int maxConcurrency, int maxConcurrencyBeforeServingQueries,
      boolean isServingQueries, @Nullable ServerGauge thresholdGauge, @Nullable ServerGauge countGauge,
      String throttlerName) {
    _throttlerName = throttlerName;
    _thresholdGauge = thresholdGauge;
    _countGauge = countGauge;
    LOGGER.info("Initializing SegmentOperationsThrottlerSet {}, maxConcurrency: {}, "
            + "maxConcurrencyBeforeServingQueries: {}, isServingQueries: {}",
        throttlerName, maxConcurrency, maxConcurrencyBeforeServingQueries,
        isServingQueries);
    Preconditions.checkArgument(maxConcurrency > 0, "Max parallelism must be > 0, but found to be: " + maxConcurrency);
    Preconditions.checkArgument(maxConcurrencyBeforeServingQueries > 0,
        "Max parallelism before serving queries must be > 0, but found to be: " + maxConcurrencyBeforeServingQueries);

    _maxConcurrency = maxConcurrency;
    _maxConcurrencyBeforeServingQueries = maxConcurrencyBeforeServingQueries;
    _isServingQueries = isServingQueries;

    // maxConcurrencyBeforeServingQueries is only used prior to serving queries and once the server is
    // ready to serve queries this is not used again. This too is configurable via ZK CLUSTER config updates while the
    // server is starting up.
    if (!isServingQueries) {
      LOGGER.info("Throttler {}: Serving queries is disabled, using concurrency as: {}",
          _throttlerName, _maxConcurrencyBeforeServingQueries);
    }

    int concurrency = _isServingQueries ? _maxConcurrency : _maxConcurrencyBeforeServingQueries;
    _semaphore = new AdjustableSemaphore(concurrency, true);
    _numSegmentsAcquiredSemaphore = new AtomicInteger(0);
    initializeMetrics();
    LOGGER.info("Throttler {}: Created semaphore with total permits: {}, available permits: {}",
        _throttlerName, totalPermits(), availablePermits());
  }

  /**
   * Updates the throttle threshold metric
   * @param value value to update the metric to
   */
  public void updateThresholdMetric(int value) {
    if (_thresholdGauge != null) {
      _serverMetrics.setValueOfGlobalGauge(_thresholdGauge, value);
    }
  }

  /**
   * Updates the throttle count metric
   * @param value value to update the metric to
   */
  public void updateCountMetric(int value) {
    if (_countGauge != null) {
      _serverMetrics.setValueOfGlobalGauge(_countGauge, value);
    }
  }

  /**
   * The ServerMetrics may be created after these throttle objects are created. In that case, the initialization that
   * happens in the constructor may have occurred on the NOOP metrics. This should be called after the server metrics
   * are created and registered to ensure the correct metrics object is used and the metrics are updated correctly
   *
   * This is called in the same thread as the constructor so there is no need to make _serverMetrics volatile here
   */
  public void initializeMetrics() {
    _serverMetrics = ServerMetrics.get();
    updateThresholdMetric(_semaphore.getTotalPermits());
    updateCountMetric(0);
  }

  public synchronized void startServingQueries() {
    LOGGER.info("Throttler {}: Serving queries is to be enabled, reset throttling threshold "
        + "for segment operations concurrency, total permits: {}, available permits: {}",
        _throttlerName, totalPermits(), availablePermits());
    _isServingQueries = true;
    _semaphore.setPermits(_maxConcurrency);
    updateThresholdMetric(_maxConcurrency);
    LOGGER.info("Throttler {}: Reset throttling completed, new concurrency: {}, "
        + "total permits: {}, available permits: {}",
        _throttlerName, _maxConcurrency, totalPermits(),
        availablePermits());
  }

  /**
   * Updates the throttler permits based on new configuration values.
   * This is called by the parent SegmentOperationsThrottlerSet when config changes are detected.
   *
   * @param maxConcurrency new max concurrency value
   * @param maxConcurrencyBeforeServingQueries new max concurrency before serving queries value
   */
  public synchronized void updatePermits(int maxConcurrency, int maxConcurrencyBeforeServingQueries) {
    Preconditions.checkArgument(maxConcurrency > 0, "Max concurrency must be > 0, but found: " + maxConcurrency);
    Preconditions.checkArgument(maxConcurrencyBeforeServingQueries > 0,
        "Max concurrency before serving queries must be > 0, but found: " + maxConcurrencyBeforeServingQueries);

    LOGGER.info("Throttler {}: Updating permits - maxConcurrency: {} -> {}, "
        + "maxConcurrencyBeforeServingQueries: {} -> {}",
        _throttlerName, _maxConcurrency, maxConcurrency,
        _maxConcurrencyBeforeServingQueries, maxConcurrencyBeforeServingQueries);

    _maxConcurrency = maxConcurrency;
    _maxConcurrencyBeforeServingQueries = maxConcurrencyBeforeServingQueries;

    // Update semaphore based on current state
    int newPermits = _isServingQueries ? _maxConcurrency : _maxConcurrencyBeforeServingQueries;
    _semaphore.setPermits(newPermits);
    updateThresholdMetric(newPermits);
    LOGGER.info("Throttler {}: Updated total permits: {}", _throttlerName, totalPermits());
  }

  /**
   * Block trying to acquire the semaphore to perform the segment operation steps unless interrupted.
   * <p>
   * {@link #release()} should be called after the segment operation completes. It is the responsibility of the caller
   * to ensure that {@link #release()} is called exactly once for each call to this method.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public void acquire()
      throws InterruptedException {
    _semaphore.acquire();
    updateCountMetric(_numSegmentsAcquiredSemaphore.incrementAndGet());
  }

  /**
   * Should be called after the segment operation completes. It is the responsibility of the caller to
   * ensure that this method is called exactly once for each call to {@link #acquire()}.
   */
  public void release() {
    _semaphore.release();
    updateCountMetric(_numSegmentsAcquiredSemaphore.decrementAndGet());
  }

  /**
   * Get the estimated number of threads waiting for the semaphore
   * @return the estimated queue length
   */
  public int getQueueLength() {
    return _semaphore.getQueueLength();
  }

  /**
   * Get the number of available permits
   * @return number of available permits
   */
  @VisibleForTesting
  public int availablePermits() {
    return _semaphore.availablePermits();
  }

  /**
   * Get the total number of permits
   * @return total number of permits
   */
  @VisibleForTesting
  public int totalPermits() {
    return _semaphore.getTotalPermits();
  }

  public String getThrottlerName() {
    return _throttlerName;
  }
}
