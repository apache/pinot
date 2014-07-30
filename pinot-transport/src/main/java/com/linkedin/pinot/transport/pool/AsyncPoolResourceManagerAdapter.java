package com.linkedin.pinot.transport.pool;

import java.util.concurrent.ExecutorService;

import com.linkedin.pinot.metrics.common.LatencyMetric;
import com.linkedin.pinot.metrics.common.MetricsHelper;
import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.metrics.PoolStats.LifecycleStats;
import com.linkedin.pinot.transport.pool.AsyncPool.Lifecycle;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public class AsyncPoolResourceManagerAdapter<K, T> implements Lifecycle<T> {
  //private static final Logger LOG = LoggerFactory.getLogger(AsyncPoolResourceManagerAdapter.class);

  private final PooledResourceManager<K,T> _resourceManager;
  private final ExecutorService _executor;
  private final K _key;
  private final Histogram _histogram;
  public AsyncPoolResourceManagerAdapter(K key, PooledResourceManager<K,T> resourceManager, ExecutorService executorService, MetricsRegistry registry)
  {
    _resourceManager = resourceManager;
    _executor = executorService;
    _key = key;
    _histogram = MetricsHelper.newHistogram(registry, new MetricName(AsyncPoolResourceManagerAdapter.class, key.toString()), false);
  }

  @Override
  public void create(final Callback<T> callback) {
    final long startTime = System.currentTimeMillis();
    _executor.submit( new Runnable() {

      @Override
      public void run() {
        T resource =  _resourceManager.create(_key);
        _histogram.update(System.currentTimeMillis() - startTime);
        if ( null != resource)
        {
          callback.onSuccess(resource);
        } else {
          callback.onError(new Exception("Unable to create resource for key " + _key));
        }
      }
    });
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
    _executor.submit( new Runnable() {

      @Override
      public void run() {
        boolean success = _resourceManager.destroy(_key, error, obj);
        if (success )
        {
          callback.onSuccess(obj);
        } else {
          callback.onError(new Exception("Unable to destroy resource for key " + _key));
        }
      }
    });

  }

  @Override
  public LifecycleStats<Histogram> getStats() {
    return new LifecycleStats<Histogram>(new LatencyMetric<Histogram>(_histogram));
  }
}
