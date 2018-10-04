/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.utils;

import com.linkedin.thirdeye.tracking.RequestLog;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;


public class ThirdeyeMetricsUtil {
  private static final MetricsRegistry metricsRegistry = new MetricsRegistry();
  private static final JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
  private static final RequestLog requestLog = new RequestLog(1000000);

  static {
    jmxReporter.start();
  }

  private ThirdeyeMetricsUtil() {
  }

  public static final Counter detectionTaskCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskCounter");

  public static final Counter detectionTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskSuccessCounter");

  public static final Counter detectionTaskExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskExceptionCounter");

  public static final Counter alertTaskCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "alertTaskCounter");

  public static final Counter alertTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "alertTaskSuccessCounter");

  public static final Counter alertTaskExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "alertTaskExceptionCounter");

  public static final Counter dbCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbCallCounter");

  public static final Counter dbExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbExceptionCounter");

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

  public static final Counter datasourceExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "datasourceExceptionCounter");

  public static final Counter pinotCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotCallCounter");

  public static final Counter pinotDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotDurationCounter");

  public static final Counter pinotExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotExceptionCounter");

  public static final Counter rcaPipelineCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineCallCounter");

  public static final Counter rcaPipelineDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineDurationCounter");

  public static final Counter rcaPipelineExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineExceptionCounter");

  public static final Counter rcaFrameworkCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkCallCounter");

  public static final Counter rcaFrameworkDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkDurationCounter");

  public static final Counter rcaFrameworkExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkExceptionCounter");

  public static final Counter cubeCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeCallCounter");

  public static final Counter cubeDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeDurationCounter");

  public static final Counter cubeExceptionCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeExceptionCounter");

  public static MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  public static RequestLog getRequestLog() {
    return requestLog;
  }
}
