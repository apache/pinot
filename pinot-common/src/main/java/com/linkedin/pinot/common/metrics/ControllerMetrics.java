package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;


/**
 * Metrics for the controller.
 */
public class ControllerMetrics extends AbstractMetrics<AbstractMetrics.QueryPhase, AbstractMetrics.Meter> {
  public ControllerMetrics(MetricsRegistry metricsRegistry) {
    super("pinot.controller", metricsRegistry, ControllerMetrics.class);
  }

  @Override
  protected QueryPhase[] getQueryPhases() {
    return new QueryPhase[0];
  }

  @Override
  protected Meter[] getMeters() {
    return new Meter[0];
  }
}
