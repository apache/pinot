package com.linkedin.thirdeye.anomaly.job;


import org.hibernate.SessionFactory;

import com.linkedin.thirdeye.detector.db.dao.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public abstract class JobContext {

  private AnomalyJobSpecDAO anomalyJobSpecDAO;
  private AnomalyTaskSpecDAO anomalyTaskSpecDAO;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private SessionFactory sessionFactory;

  private String jobName;
  private long jobExecutionId;


  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public AnomalyJobSpecDAO getAnomalyJobSpecDAO() {
    return anomalyJobSpecDAO;
  }

  public void setAnomalyJobSpecDAO(AnomalyJobSpecDAO anomalyJobSpecDAO) {
    this.anomalyJobSpecDAO = anomalyJobSpecDAO;
  }

  public AnomalyTaskSpecDAO getAnomalyTaskSpecDAO() {
    return anomalyTaskSpecDAO;
  }

  public void setAnomalyTaskSpecDAO(AnomalyTaskSpecDAO anomalyTaskSpecDAO) {
    this.anomalyTaskSpecDAO = anomalyTaskSpecDAO;
  }

  public AnomalyFunctionSpecDAO getAnomalyFunctionSpecDAO() {
    return anomalyFunctionSpecDAO;
  }

  public void setAnomalyFunctionSpecDAO(AnomalyFunctionSpecDAO anomalyFunctionSpecDAO) {
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
  }

  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
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
