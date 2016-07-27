package com.linkedin.thirdeye.anomaly.job;


import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;

import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public abstract class JobContext {

  private AnomalyJobDAO anomalyJobDAO;
  private AnomalyTaskDAO anomalyTaskDAO;
  private AnomalyFunctionDAO anomalyFunctionDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;

  private String jobName;
  private long jobExecutionId;


  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
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

  public AnomalyFunctionDAO getAnomalyFunctionDAO() {
    return anomalyFunctionDAO;
  }

  public void setAnomalyFunctionDAO(AnomalyFunctionDAO anomalyFunctionDAO) {
    this.anomalyFunctionDAO = anomalyFunctionDAO;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

}
