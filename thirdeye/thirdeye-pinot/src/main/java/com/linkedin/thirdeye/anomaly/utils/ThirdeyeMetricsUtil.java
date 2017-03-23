package com.linkedin.thirdeye.anomaly.utils;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;


public class ThirdeyeMetricsUtil {
  private static MetricsRegistry metricsRegistry = new MetricsRegistry();
  private static JmxReporter jmxReporter = new JmxReporter(metricsRegistry);

  static {
    jmxReporter.start();
  }

  private ThirdeyeMetricsUtil() {
  }

  public static final Counter detectionTaskCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskCounter");

  public static final Counter detectionTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskSuccessCounter");

  public static final Counter alertTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "alertTaskSuccessCounter");

  public static final Counter dbCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbCallCounter");

  public static MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}
