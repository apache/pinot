package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExFactory;


public class TaskContext {

  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private AnomalyFunctionExFactory anomalyFunctionExFactory;
  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;

  public ThirdEyeAnomalyConfiguration getThirdEyeAnomalyConfiguration() {
    return thirdEyeAnomalyConfiguration;
  }

  public void setThirdEyeAnomalyConfiguration(
      ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration) {
    this.thirdEyeAnomalyConfiguration = thirdEyeAnomalyConfiguration;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public AlertFilterFactory getAlertFilterFactory(){ return  alertFilterFactory; }

  public void setAlertFilterFactory(AlertFilterFactory alertFilterFactory) {
    this.alertFilterFactory = alertFilterFactory;
  }

  public AnomalyFunctionExFactory getAnomalyFunctionExFactory() {
    return anomalyFunctionExFactory;
  }

  public void setAnomalyFunctionExFactory(AnomalyFunctionExFactory anomalyFunctionExFactory) {
    this.anomalyFunctionExFactory = anomalyFunctionExFactory;
  }
}
