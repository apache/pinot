/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.metrics.PoolStats.LifecycleStats;
import com.linkedin.pinot.transport.pool.AsyncPool.Lifecycle;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;


// The create() and destoy() methods in this class are ONLY called from
// AsyncPoolImpl when a connection needs to be created or destroyed.
public class AsyncPoolResourceManagerAdapter<T> implements Lifecycle<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncPoolResourceManagerAdapter.class);

  private final PooledResourceManager<T> _resourceManager;
  private final ExecutorService _executor;
  private final ServerInstance _key;
  private final Histogram _histogram;
  private boolean _isShuttingDown = false;

  public AsyncPoolResourceManagerAdapter(ServerInstance key, PooledResourceManager<T> resourceManager,
      ExecutorService executorService, MetricsRegistry registry) {
    _resourceManager = resourceManager;
    _executor = executorService;
    _key = key;
    _histogram =
        MetricsHelper.newHistogram(registry, new MetricName(AsyncPoolResourceManagerAdapter.class, key.toString()),
            false);
  }

  @Override
  public void shutdown() {
    _isShuttingDown = true;
  }

  @Override
  public void create(final Callback<T> callback) {
    final long startTime = System.currentTimeMillis();
    _executor.submit(new Runnable() {

      @Override
      public void run() {
        T resource = _resourceManager.create(_key);
        _histogram.update(System.currentTimeMillis() - startTime);
        if (null != resource) {
          callback.onSuccess(resource);
        } else {
          callback.onError(new Exception("Unable to create resource for key " + _key));
        }
      }
    });
  }

  @Override
  public boolean validate(T obj) {
    return _resourceManager.validate(_key, obj);
  }

  @Override
  public boolean validateGet(T obj) {
    return _resourceManager.validate(_key, obj);
  }

  @Override
  public boolean validatePut(T obj) {
    return _resourceManager.validate(_key, obj);
  }

  @Override
  public void destroy(final T obj, final boolean error, final Callback<T> callback) {
    try {
      _executor.submit(new Runnable() {

        @Override
        public void run() {
          LOGGER.info("Running teardown for the client connection " + obj + " Error is : " + error);
          boolean success = _resourceManager.destroy(_key, error, obj);
          if (success) {
            callback.onSuccess(obj);
          } else {
            callback.onError(new Exception("Unable to destroy resource for key " + _key));
          }
        }
      });
    } catch (Exception e) {
      // During a broker shutdown, it is possible that we get RejectedExecutionException since we are tyring to
      // destroy all resources. Ignore it (only during shutdown).
      if (_isShuttingDown) {
        LOGGER.info("Could not destroy resource for key {}: {}", _key.toString(), e.getMessage());
      } else {
        LOGGER.error("Could not destroy resource for key {}: {}", _key.toString(), e.getMessage());
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public LifecycleStats<Histogram> getStats() {
    return new LifecycleStats<Histogram>(new LatencyMetric<Histogram>(_histogram));
  }
}
