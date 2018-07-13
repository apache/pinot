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
package com.linkedin.pinot.transport.pool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.AsyncResponseFuture;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.Cancellable;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.CompositeFuture.GatherModeOnError;
import com.linkedin.pinot.transport.common.ServerResponseFuture;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.metrics.AggregatedPoolStats;
import com.linkedin.pinot.transport.metrics.PoolStats;
import com.linkedin.pinot.transport.pool.AsyncPoolImpl.Strategy;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;


public class KeyedPoolImpl<T> implements KeyedPool<T> {

  protected static Logger LOGGER = LoggerFactory.getLogger(KeyedPoolImpl.class);

  /**
   * State of the pool
   */
  private enum State {
    INIT,
    RUNNING,
    SHUTTING_DOWN,
    SHUTDOWN
  }

  // State of this connection pool
  private State _state;

  // Maximum number of resources allowed per key in the pool
  private final int _maxResourcesPerKey;
  // Minimum number of resources allowed per key in the pool
  private final int _minResourcesPerKey;
  // Idle Timeout (ms) for reaping idle resources
  private final long _idleTimeoutMs;
  // Maximum number of pending checkout requests before requests starts getting rejected
  private final int _maxPendingCheckoutRequests;
  // Executor Service to remove idle objects on timeout.
  private final ScheduledExecutorService _timeoutExecutor;

  //Resource Manager Callback
  private final PooledResourceManager<T> _resourceManager;

  // Executor Service for creating/removing resources
  private final ExecutorService _executorService;

  // Shutdown Future
  private CompositeFuture<NoneType> _shutdownFuture;

  private final MetricsRegistry _metricRegistry;

  private final AggregatedPoolStats<Histogram> _poolStats;

  /**
   * Map of AsyncPools
   */
  private final ConcurrentMap<ServerInstance, AsyncPool<T>> _keyedPool;

  private final ConcurrentMap<ServerInstance, AsyncPoolResourceManagerAdapter<T>> _pooledResourceManagerMap;

  /**
   * All modifications of _keyedPool and all access to _state must be locked on _mutex.
   * READS of _pool are allowed without synchronization
   */
  private final Object _mutex = new Object();

  public KeyedPoolImpl(int minResourcesPerKey, int maxResourcesPerKey, long idleTimeoutMs,
      int maxPendingCheckoutRequests, PooledResourceManager<T> resourceManager,
      ScheduledExecutorService timeoutExecutorService, ExecutorService executorService, MetricsRegistry registry) {
    _keyedPool = new ConcurrentHashMap<ServerInstance, AsyncPool<T>>();
    _pooledResourceManagerMap = new ConcurrentHashMap<ServerInstance, AsyncPoolResourceManagerAdapter<T>>();
    _minResourcesPerKey = minResourcesPerKey;
    _maxResourcesPerKey = maxResourcesPerKey;
    _idleTimeoutMs = idleTimeoutMs;
    _maxPendingCheckoutRequests = maxPendingCheckoutRequests;
    _timeoutExecutor = timeoutExecutorService;
    _executorService = executorService;
    _resourceManager = resourceManager;
    _state = State.INIT;
    _metricRegistry = registry;
    _poolStats = new AggregatedPoolStats<Histogram>();
  }

  @Override
  public void start() {
    _state = State.RUNNING;
  }

  @Override
  public ServerResponseFuture<T> checkoutObject(ServerInstance key, String context) {
    AsyncPool<T> pool = _keyedPool.get(key);

    if (null == pool) {
      synchronized (_mutex) {
        pool = _keyedPool.get(key);
        if (null == pool) {
          String poolName = "Pool for (" + key + ")";
          AsyncPoolResourceManagerAdapter<T> rmAdapter =
              new AsyncPoolResourceManagerAdapter<T>(key, _resourceManager, _executorService, _metricRegistry);
          pool = new AsyncPoolImpl<T>(poolName, rmAdapter, _maxResourcesPerKey, _idleTimeoutMs, _timeoutExecutor,
              _executorService, _maxPendingCheckoutRequests, Strategy.LRU, _minResourcesPerKey, _metricRegistry);
          _keyedPool.put(key, pool);
          _poolStats.add(pool);
          _pooledResourceManagerMap.put(key, rmAdapter);
          pool.start();
        }
      }
    }

    AsyncResponseFuture<T> future = new AsyncResponseFuture<T>(key, "ConnPool checkout future for key " + key + "(" + context + ")");
    Cancellable cancellable = pool.get(future);
    future.setCancellable(cancellable);
    return future;
  }

  @Override
  public void checkinObject(ServerInstance key, T object) {
    AsyncPool<T> pool = _keyedPool.get(key);
    if (null != pool) {
      pool.put(object);
    } else {
      throw new IllegalStateException(
          "Trying to checkin an object from a pool which does not exist. No pool available for key (" + key + ") !!");
    }
  }

  @Override
  public void destroyObject(ServerInstance key, T object) {
    AsyncPool<T> pool = _keyedPool.get(key);
    LOGGER.info("Destroying object for the key (" + key + ") object :" + object);
    if (null != pool) {
      pool.dispose(object);
    } else {
      throw new IllegalStateException(
          "Trying to destroy an object from a pool which does not exist. No pool available for key (" + key + ") !!");
    }
  }

  @Override
  public ServerResponseFuture<NoneType> shutdown() {
    synchronized (_mutex) {
      if ((_state == State.SHUTTING_DOWN) || (_state == State.SHUTDOWN)) {
        return _shutdownFuture;
      }

      _state = State.SHUTTING_DOWN;

      List<ServerResponseFuture< NoneType>> futureList = new ArrayList<ServerResponseFuture<NoneType>>();
      for (Entry<ServerInstance, AsyncPool<T>> poolEntry : _keyedPool.entrySet()) {
        AsyncResponseFuture<NoneType> shutdownFuture = new AsyncResponseFuture<NoneType>(poolEntry.getKey(),
            "ConnPool shutdown future for pool entry " + poolEntry.getKey());
        poolEntry.getValue().shutdown(shutdownFuture);
        futureList.add(shutdownFuture);
        //Cancel waiters and notify them
        Collection<Callback<T>> waiters = poolEntry.getValue().cancelWaiters();
        if ((null != waiters) && !waiters.isEmpty()) {
          Exception ex = new Exception("Pool is shutting down !!");
          for (Callback<T> w : waiters) {
            w.onError(ex);
          }
          poolEntry.getValue().shutdown(shutdownFuture);
        }
      }
      _shutdownFuture = new CompositeFuture<NoneType>("Shutdown For Pool", GatherModeOnError.AND);
      _shutdownFuture.start(futureList);
      _shutdownFuture.addListener(new Runnable() {

        @Override
        public void run() {
          synchronized (_mutex) {
            _state = State.SHUTDOWN;
          }
        }
      }, null);
    }
    return _shutdownFuture;
  }

  @Override
  public PoolStats<Histogram> getStats() {
    return _poolStats;
  }

  @Override
  public boolean validatePool(ServerInstance key, boolean recreate) {
    AsyncPool<T> pool = _keyedPool.get(key);
    if (pool != null) {
      return pool.validate(recreate);
    }
    return true;
  }
}
