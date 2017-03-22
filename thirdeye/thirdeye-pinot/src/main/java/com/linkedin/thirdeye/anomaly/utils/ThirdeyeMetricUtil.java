package com.linkedin.thirdeye.anomaly.utils;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;


public class ThirdeyeMetricUtil {
  private static MetricsRegistry metricsRegistry = new MetricsRegistry();
  private static JmxReporter jmxReporter = new JmxReporter(metricsRegistry);

  static {
    jmxReporter.start();
  }

  private ThirdeyeMetricUtil() {
  }

  public static final Counter detectionTaskCounter =
      metricsRegistry.newCounter(ThirdeyeMetricUtil.class, "detectionTaskCounter");

  public static final Counter detectionTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricUtil.class, "detectionTaskSuccessCounter");

  public static final Counter alertTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricUtil.class, "alertTaskSuccessCounter");

  public static final Counter dbCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricUtil.class, "dbCallCounter");
}
