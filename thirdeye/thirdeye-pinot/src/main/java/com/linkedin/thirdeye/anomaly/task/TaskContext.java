package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private AnomalyJobDAO anomalyJobDAO;
  private AnomalyTaskDAO anomalyTaskDAO;
  private AnomalyResultDAO resultDAO;
  private AnomalyMergedResultDAO mergedResultDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;

  public ThirdEyeAnomalyConfiguration getThirdEyeAnomalyConfiguration() {
    return thirdEyeAnomalyConfiguration;
  }

  public void setThirdEyeAnomalyConfiguration(
      ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration) {
    this.thirdEyeAnomalyConfiguration = thirdEyeAnomalyConfiguration;
  }

  public AnomalyJobDAO getAnomalyJobDAO() {
    return anomalyJobDAO;
  }

  public void setAnomalyJobDAO(AnomalyJobDAO anomalyJobDAO) {
    this.anomalyJobDAO = anomalyJobDAO;
  }

  public AnomalyTaskDAO getAnomalyTaskDAO() {
    return anomalyTaskDAO;
  }

  public void setAnomalyTaskDAO(AnomalyTaskDAO anomalyTaskDAO) {
    this.anomalyTaskDAO = anomalyTaskDAO;
  }

  public AnomalyResultDAO getResultDAO() {
    return resultDAO;
  }

  public void setResultDAO(AnomalyResultDAO resultDAO) {
    this.resultDAO = resultDAO;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public AnomalyMergedResultDAO getMergedResultDAO() {
    return mergedResultDAO;
  }

  public void setMergedResultDAO(AnomalyMergedResultDAO mergedResultDAO) {
    this.mergedResultDAO = mergedResultDAO;
  }
}
