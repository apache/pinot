package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.classification.classifier.AnomalyClassifierFactory;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private AnomalyClassifierFactory anomalyClassifierFactory;

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

  public void setAlertFilterFactory(AlertFilterFactory alertFilterFactory){
    this.alertFilterFactory = alertFilterFactory;
  }

  public AnomalyClassifierFactory getAnomalyClassifierFactory() {
    return anomalyClassifierFactory;
  }

  public void setAnomalyClassifierFactory(AnomalyClassifierFactory anomalyClassifierFactory) {
    this.anomalyClassifierFactory = anomalyClassifierFactory;
  }
}
