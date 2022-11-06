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

package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The {@code ExponentialMovingAverage} is the implementation of the utility Exponential Weighted Moving Average
 * which is a statistical measure used to model time series data. Refer https://en.wikipedia.org/wiki/Moving_average
 * for more details.
 */
@ThreadSafe
public class ExponentialMovingAverage {
  private final double _alpha;
  private final long _autoDecayWindowMs;

  private final long _warmUpDurationMs;
  private final long _initializationTimeMs;

  private volatile double _average;
  private long _lastUpdatedTimeMs;

  /**
   * Constructor
   *
   * @param alpha                Determines how much weightage should be given to the new value. Can only take a value
   *                             between 0 and 1.
   * @param autoDecayWindowMs    Time interval to periodically decay the average if no updates are received. For example
   *                             if autoDecayWindowMs = 30s, if average is not updated for a period of 30 seconds, we
   *                             automatically update the average to 0.0 with a weightage of alpha.
   * @param warmUpDurationMs     The initial duration after initialization during which new incoming values are ignored
   *                             in the average calculation.
   * @param avgInitializationVal The default value to initialize for average.
   * @param periodicTaskExecutor Executor to schedule periodic tasks like autoDecay.
   */
  public ExponentialMovingAverage(double alpha, long autoDecayWindowMs, long warmUpDurationMs,
      double avgInitializationVal, @Nullable ScheduledExecutorService periodicTaskExecutor) {
    Preconditions.checkState(alpha >= 0.0 && alpha <= 1.0, "Alpha should be between 0 and 1");
    _alpha = alpha;
    Preconditions.checkState(warmUpDurationMs >= 0, "warmUpDurationMs is negative.");
    _warmUpDurationMs = warmUpDurationMs;
    Preconditions.checkState(avgInitializationVal >= 0.0, "avgInitializationVal is negative.");
    _average = avgInitializationVal;

    _initializationTimeMs = System.currentTimeMillis();
    _lastUpdatedTimeMs = 0;
    _autoDecayWindowMs = autoDecayWindowMs;

    if (_autoDecayWindowMs > 0) {
      // Schedule a task to automatically decay the average if updates are not performed in the last _autoDecayWindowMs.
      Preconditions.checkState(periodicTaskExecutor != null);
      periodicTaskExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          if (_lastUpdatedTimeMs > 0 && (System.currentTimeMillis() - _lastUpdatedTimeMs) > _autoDecayWindowMs) {
            compute(0.0);
          }
        }
      }, 0, _autoDecayWindowMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Returns the exponentially weighted moving average.
   */
  public double getAverage() {
    return _average;
  }

  /**
   * Adds a value to the exponentially weighted moving average. If warmUpDurationMs is not reached yet, this value is
   * ignored.
   * @param value incoming value
   * @return the updated exponentially weighted moving average.
   */
  public synchronized double compute(double value) {
    long currTime = System.currentTimeMillis();
    _lastUpdatedTimeMs = currTime;

    if (_initializationTimeMs + _warmUpDurationMs > currTime) {
      return _average;
    }

    _average = value * _alpha + _average * (1 - _alpha);
    return _average;
  }
}
