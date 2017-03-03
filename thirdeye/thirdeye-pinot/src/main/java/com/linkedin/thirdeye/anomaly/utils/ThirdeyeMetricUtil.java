package com.linkedin.thirdeye.anomaly.utils;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyApplication;

import static com.linkedin.thirdeye.common.BaseThirdEyeApplication.metricsRegistry;

import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.yammer.metrics.core.Counter;

public abstract class ThirdeyeMetricUtil {
  private ThirdeyeMetricUtil() {
  }

  public static final Counter detectionTaskCounter =
      metricsRegistry.newCounter(ThirdEyeAnomalyApplication.class, "detectionTaskCounter");

  public static final Counter detectionTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdEyeAnomalyApplication.class, "detectionTaskSuccessCounter");

  public static final Counter alertTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdEyeAnomalyApplication.class, "alertTaskSuccessCounter");

  public static final Counter dbCallCounter =
      metricsRegistry.newCounter(BaseThirdEyeApplication.class, "dbCallCounter");
}
