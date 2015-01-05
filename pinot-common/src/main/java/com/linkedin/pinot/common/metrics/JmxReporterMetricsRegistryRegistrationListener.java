package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;


/**
 * Adapter that causes metrics from a metric registry to be published to JMX.
 *
 * @author jfim
 */
public class JmxReporterMetricsRegistryRegistrationListener implements MetricsRegistryRegistrationListener {
  @Override
  public void onMetricsRegistryRegistered(MetricsRegistry metricsRegistry) {
    new JmxReporter(metricsRegistry).start();
  }
}
