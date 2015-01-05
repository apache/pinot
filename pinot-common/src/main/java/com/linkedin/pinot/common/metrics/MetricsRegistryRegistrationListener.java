package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;


/**
 * Interface to implement operations that occur whenever a new MetricsRegistry is registered with the MetricsHelper.
 *
 * @author jfim
 */
public interface MetricsRegistryRegistrationListener {
  public void onMetricsRegistryRegistered(MetricsRegistry metricsRegistry);
}
