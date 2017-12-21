package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

public class DetectionOnboardTaskContext {
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private AlertFilterAutotuneFactory alertFilterAutotuneFactory;
  private Configuration configuration = new MapConfiguration(Collections.emptyMap());
  private DetectionOnboardExecutionContext executionContext = new DetectionOnboardExecutionContext();

  public DetectionOnboardTaskContext() {
  }

  public DetectionOnboardTaskContext(AnomalyFunctionFactory anomalyFunctionFactory,
      AlertFilterFactory alertFilterFactory, AlertFilterAutotuneFactory alertFilterAutotuneFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
    this.alertFilterAutotuneFactory = alertFilterAutotuneFactory;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    Preconditions.checkNotNull(configuration);
    this.configuration = configuration;
  }

  public DetectionOnboardExecutionContext getExecutionContext() {
    return executionContext;
  }

  public void setExecutionContext(DetectionOnboardExecutionContext executionContext) {
    Preconditions.checkNotNull(executionContext);
    this.executionContext = executionContext;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  private void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public AlertFilterFactory getAlertFilterFactory() {
    return alertFilterFactory;
  }

  private void setAlertFilterFactory(AlertFilterFactory alertFilterFactory) {
    this.alertFilterFactory = alertFilterFactory;
  }

  public AlertFilterAutotuneFactory getAlertFilterAutotuneFactory() {
    return alertFilterAutotuneFactory;
  }

  private void setAlertFilterAutotuneFactory(AlertFilterAutotuneFactory alertFilterAutotuneFactory) {
    this.alertFilterAutotuneFactory = alertFilterAutotuneFactory;
  }
}
