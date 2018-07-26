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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.linkedin.pinot.common.metrics.AggregatedHistogram;
import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;


public class AggregatedPoolStats<T extends Sampling & Summarizable> implements PoolStats<T>, PoolStatsProvider {

  private final long DEFAULT_REFRESH_MS = 60 * 1000; // 1 minute

  // Refresh Delay config
  private final long _refreshMs;

  // Last Refreshed timestamp
  private volatile long _lastRefreshedTime;

  private volatile int _totalCreated;
  private volatile int _totalDestroyed;
  private volatile int _totalCreateErrors;
  private volatile int _totalDestroyErrors;
  private volatile int _totalBadDestroyed;
  private volatile int _totalTimedOut;

  private volatile int _checkedOut;
  private volatile int _maxPoolSize;
  private volatile int _minPoolSize;
  private volatile int _poolSize;

  private volatile int _sampleMaxCheckedOut;
  private volatile int _sampleMaxPoolSize;

  private volatile int _idleCount;
  private volatile LatencyMetric<T> _waitTime;
  private volatile LifecycleStats<T> _lifecycleStats;

  private final List<PoolStatsProvider> _poolStatsProvider = new CopyOnWriteArrayList<PoolStatsProvider>();

  public AggregatedPoolStats(long refreshMs) {
    _refreshMs = refreshMs;
  }

  public AggregatedPoolStats() {
    _refreshMs = DEFAULT_REFRESH_MS;
  }

  public AggregatedPoolStats<T> addAll(Collection<PoolStatsProvider> poolStats) {
    _poolStatsProvider.addAll(poolStats);
    return this;
  }

  public AggregatedPoolStats<T> add(PoolStatsProvider poolStat) {
    _poolStatsProvider.add(poolStat);
    return this;
  }

  public void refreshIfElapsed() {
    long currentTime = System.currentTimeMillis();
    if (currentTime - _lastRefreshedTime > _refreshMs && !_poolStatsProvider.isEmpty()) {
      refresh();
      _lastRefreshedTime = currentTime;
    }
  }

  public void refresh() {
    int totalCreated = 0;
    int totalDestroyed = 0;
    int totalCreateErrors = 0;
    int totalDestroyErrors = 0;
    int totalBadDestroyed = 0;
    int totalTimedOut = 0;

    int checkedOut = 0;
    int maxPoolSize = 0;
    int minPoolSize = 0;
    int poolSize = 0;

    int sampleMaxCheckedOut = 0;
    int sampleMaxPoolSize = 0;

    int idleCount = 0;
    AggregatedHistogram<Sampling> waitTimeHist = new AggregatedHistogram<Sampling>();
    AggregatedHistogram<Sampling> createTimeHist = new AggregatedHistogram<Sampling>();

    for (PoolStatsProvider p : _poolStatsProvider) {
      PoolStats<T> s = p.getStats();
      totalCreated += s.getTotalCreated();
      totalDestroyed += s.getTotalBadDestroyed();
      totalCreateErrors += s.getTotalCreateErrors();
      totalDestroyErrors += s.getTotalDestroyErrors();
      totalBadDestroyed += s.getTotalBadDestroyed();
      totalTimedOut += s.getTotalTimedOut();
      checkedOut += s.getCheckedOut();
      maxPoolSize += s.getMaxPoolSize();
      minPoolSize += s.getMinPoolSize();
      poolSize += s.getPoolSize();
      sampleMaxCheckedOut += s.getSampleMaxCheckedOut();
      sampleMaxPoolSize += s.getSampleMaxPoolSize();
      idleCount += s.getIdleCount();
      waitTimeHist.add(s.getWaitTime().getHistogram());
      createTimeHist.add(s.getLifecycleStats().getCreateTime().getHistogram());
    }

    _totalCreated = totalCreated;
    _totalDestroyed = totalDestroyed;
    _totalBadDestroyed = totalBadDestroyed;
    _totalCreateErrors = totalCreateErrors;
    _totalDestroyErrors = totalDestroyErrors;
    _totalTimedOut = totalTimedOut;
    _checkedOut = checkedOut;
    _maxPoolSize = maxPoolSize;
    _minPoolSize = minPoolSize;
    _poolSize = poolSize;
    _sampleMaxCheckedOut = sampleMaxCheckedOut;
    _sampleMaxPoolSize = sampleMaxPoolSize;
    _idleCount = idleCount;
    _waitTime = new LatencyMetric(waitTimeHist);
    _lifecycleStats = new LifecycleStats(new LatencyMetric<AggregatedHistogram<Sampling>>(createTimeHist));
  }

  @Override
  public int getTotalCreated() {
    refreshIfElapsed();
    return _totalCreated;
  }

  @Override
  public int getTotalDestroyed() {
    refreshIfElapsed();
    return _totalDestroyed;
  }

  @Override
  public int getTotalCreateErrors() {
    refreshIfElapsed();
    return _totalCreateErrors;
  }

  @Override
  public int getTotalDestroyErrors() {
    refreshIfElapsed();
    return _totalDestroyErrors;
  }

  @Override
  public int getTotalBadDestroyed() {
    refreshIfElapsed();
    return _totalBadDestroyed;
  }

  @Override
  public int getTotalTimedOut() {
    refreshIfElapsed();
    return _totalTimedOut;
  }

  @Override
  public int getCheckedOut() {
    refreshIfElapsed();
    return _checkedOut;
  }

  @Override
  public int getMaxPoolSize() {
    refreshIfElapsed();
    return _maxPoolSize;
  }

  @Override
  public int getMinPoolSize() {
    refreshIfElapsed();
    return _minPoolSize;
  }

  @Override
  public int getPoolSize() {
    refreshIfElapsed();
    return _poolSize;
  }

  @Override
  public int getSampleMaxCheckedOut() {
    refreshIfElapsed();
    return _sampleMaxCheckedOut;
  }

  @Override
  public int getSampleMaxPoolSize() {
    refreshIfElapsed();
    return _sampleMaxPoolSize;
  }

  @Override
  public int getIdleCount() {
    refreshIfElapsed();
    return _idleCount;
  }

  @Override
  public LatencyMetric<T> getWaitTime() {
    refreshIfElapsed();
    return _waitTime;
  }

  @Override
  public LifecycleStats<T> getLifecycleStats() {
    refreshIfElapsed();
    return _lifecycleStats;
  }

  @Override
  public PoolStats<T> getStats() {
    return this;
  }

  @Override
  public String toString() {
    return "AggregatedPoolStats [_refreshMs=" + _refreshMs + ", _lastRefreshedTime=" + _lastRefreshedTime
        + ", _totalCreated=" + _totalCreated + ", _totalDestroyed=" + _totalDestroyed + ", _totalCreateErrors="
        + _totalCreateErrors + ", _totalDestroyErrors=" + _totalDestroyErrors + ", _totalBadDestroyed="
        + _totalBadDestroyed + ", _totalTimedOut=" + _totalTimedOut + ", _checkedOut=" + _checkedOut
        + ", _maxPoolSize=" + _maxPoolSize + ", _minPoolSize=" + _minPoolSize + ", _poolSize=" + _poolSize
        + ", _sampleMaxCheckedOut=" + _sampleMaxCheckedOut + ", _sampleMaxPoolSize=" + _sampleMaxPoolSize
        + ", _idleCount=" + _idleCount + ", _waitTime=" + _waitTime + ", _lifecycleStats=" + _lifecycleStats
        + ", _poolStatsProvider=" + _poolStatsProvider + "]";
  }
}
