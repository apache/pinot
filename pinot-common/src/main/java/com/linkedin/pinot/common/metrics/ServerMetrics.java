package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;


/**
 * Utility class to centralize all metrics reporting for the Pinot server.
 *
 * @author jfim
 */
public class ServerMetrics extends AbstractMetrics<ServerQueryPhase, ServerMeter> {
  @Override
  protected ServerQueryPhase[] getQueryPhases() {
    return ServerQueryPhase.values();
  }

  @Override
  protected ServerMeter[] getMeters() {
    return ServerMeter.values();
  }

  public ServerMetrics(MetricsRegistry metricsRegistry) {
    super("pinot.server.", metricsRegistry, ServerMetrics.class);
  }
}
