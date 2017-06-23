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

  public static final Counter dbReadCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbReadCallCounter");

  public static final Counter dbReadByteCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbReadByteCounter");

  public static final Counter dbReadDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbReadDurationCounter");

  public static final Counter dbWriteCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbWriteCallCounter");

  public static final Counter dbWriteByteCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbWriteByteCounter");

  public static final Counter dbWriteDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbWriteDurationCounter");

  public static final Counter datasourceCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "datasourceCallCounter");

  public static final Counter datasourceDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "datasourceDurationCounter");

  public static final Counter pinotCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotCallCounter");

  public static final Counter pinotDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotDurationCounter");

  public static final Counter rcaPipelineCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineCallCounter");

  public static final Counter rcaPipelineDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineDurationCounter");

  public static final Counter rcaFrameworkCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkCallCounter");

  public static final Counter rcaFrameworkDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkDurationCounter");

  public static final Counter cubeCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeCallCounter");

  public static final Counter cubeDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeDurationCounter");

  public static MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}
