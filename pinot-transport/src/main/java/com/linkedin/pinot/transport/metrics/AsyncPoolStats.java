/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.transport.pool.AsyncPool;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;


public class AsyncPoolStats<T extends Sampling & Summarizable> implements PoolStats<T> {
  private final int _totalCreated;
  private final int _totalDestroyed;
  private final int _totalCreateErrors;
  private final int _totalDestroyErrors;
  private final int _totalBadDestroyed;
  private final int _totalTimedOut;

  private final int _checkedOut;
  private final int _maxPoolSize;
  private final int _minPoolSize;
  private final int _poolSize;

  private final int _sampleMaxCheckedOut;
  private final int _sampleMaxPoolSize;

  private final int _idleCount;
  private final LatencyMetric<T> _waitTime;
  private final LifecycleStats<T> _lifecycleStats;

  /**
   * This class should be instantiated through a call to
   * getStats() on an AsyncPool.
   */
  public AsyncPoolStats(int totalCreated, int totalDestroyed, int totalCreateErrors, int totalDestroyErrors,
      int totalBadDestroyed, int totalTimedOut,

      int checkedOut, int maxPoolSize, int minPoolSize, int poolSize,

      int sampleMaxCheckedOut, int sampleMaxPoolSize,

      int idleCount, LatencyMetric<T> waitTime, LifecycleStats<T> lifecycleStats) {
    _totalCreated = totalCreated;
    _totalDestroyed = totalDestroyed;
    _totalCreateErrors = totalCreateErrors;
    _totalDestroyErrors = totalDestroyErrors;
    _totalBadDestroyed = totalBadDestroyed;
    _totalTimedOut = totalTimedOut;

    _checkedOut = checkedOut;
    _maxPoolSize = maxPoolSize;
    _minPoolSize = minPoolSize;
    _poolSize = poolSize;

    _sampleMaxCheckedOut = sampleMaxCheckedOut;
    _sampleMaxPoolSize = sampleMaxPoolSize;

    _idleCount = idleCount;
    _waitTime = waitTime;
    _lifecycleStats = lifecycleStats;
  }

  /**
   * Get the total number of pool objects created between
   * the starting of the AsyncPool and the call to getStats().
   * Does not include create errors.
   * @return The total number of pool objects created
   */
  @Override
  public int getTotalCreated() {
    return _totalCreated;
  }

  /**
   * Get the total number of pool objects destroyed between
   * the starting of the AsyncPool and the call to getStats().
   * Includes lifecycle validation failures, disposes,
   * and timed-out objects, but does not include destroy errors.
   * @return The total number of pool objects destroyed
   */
  @Override
  public int getTotalDestroyed() {
    return _totalDestroyed;
  }

  /**
   * Get the total number of lifecycle create errors between
   * the starting of the AsyncPool and the call to getStats().
   * @return The total number of create errors
   */
  @Override
  public int getTotalCreateErrors() {
    return _totalCreateErrors;
  }

  /**
   * Get the total number of lifecycle destroy errors between
   * the starting of the AsyncPool and the call to getStats().
   * @return The total number of destroy errors
   */
  @Override
  public int getTotalDestroyErrors() {
    return _totalDestroyErrors;
  }

  /**
   * Get the total number of pool objects destroyed (or failed to
   * to destroy because of an error) because of disposes or failed
   * lifecycle validations between the starting of the AsyncPool
   * and the call to getStats().
   * @return The total number of destroyed "bad" objects
   */
  @Override
  public int getTotalBadDestroyed() {
    return _totalBadDestroyed;
  }

  /**
   * Get the total number of timed out pool objects between the
   * starting of the AsyncPool and the call to getStats().
   * @return The total number of timed out objects
   */
  @Override
  public int getTotalTimedOut() {
    return _totalTimedOut;
  }

  /**
   * Get the number of pool objects checked out at the time of
   * the call to getStats().
   * @return The number of checked out pool objects
   */
  @Override
  public int getCheckedOut() {
    return _checkedOut;
  }

  /**
   * Get the configured maximum pool size.
   * @return The maximum pool size
   */
  @Override
  public int getMaxPoolSize() {
    return _maxPoolSize;
  }

  /**
   * Get the configured minimum pool size.
   * @return The minimum pool size
   */
  @Override
  public int getMinPoolSize() {
    return _minPoolSize;
  }

  /**
   * Get the pool size at the time of the call to getStats().
   * @return The pool size
   */
  @Override
  public int getPoolSize() {
    return _poolSize;
  }

  /**
   * Get the maximum number of checked out objects. Reset
   * after each call to getStats().
   * @return The maximum number of checked out objects
   */
  @Override
  public int getSampleMaxCheckedOut() {
    return _sampleMaxCheckedOut;
  }

  /**
   * Get the maximum pool size. Reset after each call to
   * getStats().
   * @return The maximum pool size
   */
  @Override
  public int getSampleMaxPoolSize() {
    return _sampleMaxPoolSize;
  }

  /**
   * Get the number of objects that are idle(not checked out)
   * in the pool.
   * @return The number of idle objects
   */
  @Override
  public int getIdleCount() {
    return _idleCount;
  }

  /**
   * Get stats collected from {@link AsyncPool.Lifecycle}
   * @return Lifecycle stats
   */
  @Override
  public LifecycleStats<T> getLifecycleStats() {
    return _lifecycleStats;
  }

  @Override
  public String toString() {
    return "\ntotalCreated: " + _totalCreated + "\ntotalDestroyed: " + _totalDestroyed + "\ntotalCreateErrors: "
        + _totalCreateErrors + "\ntotalDestroyErrors: " + _totalDestroyErrors + "\ntotalBadDestroyed: "
        + _totalBadDestroyed + "\ntotalTimeOut: " + _totalTimedOut + "\ncheckedOut: " + _totalTimedOut
        + "\nmaxPoolSize: " + _maxPoolSize + "\npoolSize: " + _poolSize + "\nsampleMaxCheckedOut: "
        + _sampleMaxCheckedOut + "\nsampleMaxPoolSize: " + _sampleMaxPoolSize + "\nidleCount: " + _idleCount
        + "\nwaitTime: " + _waitTime + (_lifecycleStats != null ? _lifecycleStats.toString() : "");
  }

  @Override
  public LatencyMetric<T> getWaitTime() {
    return _waitTime;
  }

  @Override
  public void refresh() {

  }
}
