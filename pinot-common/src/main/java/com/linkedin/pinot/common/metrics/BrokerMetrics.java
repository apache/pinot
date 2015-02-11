package com.linkedin.pinot.common.metrics;

import com.yammer.metrics.core.MetricsRegistry;


/**
 * Broker metrics utility class, which provides facilities to log the execution performance of queries.
 *
 * @author jfim
 */
public class BrokerMetrics extends AbstractMetrics<BrokerQueryPhase, BrokerMeter> {
  /**
   * Constructs the broker metrics.
   *
   * @param metricsRegistry The metric registry used to register timers and meters.
   */
  public BrokerMetrics(MetricsRegistry metricsRegistry) {
    super("pinot.broker.", metricsRegistry, BrokerMetrics.class);
  }

  @Override
  protected BrokerQueryPhase[] getQueryPhases() {
    return BrokerQueryPhase.values();
  }

  @Override
  protected BrokerMeter[] getMeters() {
    return BrokerMeter.values();
  }
}
