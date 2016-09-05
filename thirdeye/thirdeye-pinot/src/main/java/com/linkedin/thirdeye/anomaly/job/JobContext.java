package com.linkedin.thirdeye.anomaly.job;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public abstract class JobContext {

  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private EmailConfigurationManager emailConfigurationDAO;

  private String jobName;
  private long jobExecutionId;


  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public JobManager getAnomalyJobDAO() {
    return anomalyJobDAO;
  }

  public void setAnomalyJobDAO(JobManager anomalyJobDAO) {
    this.anomalyJobDAO = anomalyJobDAO;
  }

  public TaskManager getAnomalyTaskDAO() {
    return anomalyTaskDAO;
  }

  public void setAnomalyTaskDAO(TaskManager anomalyTaskDAO) {
    this.anomalyTaskDAO = anomalyTaskDAO;
  }

  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return anomalyFunctionDAO;
  }

  public void setAnomalyFunctionDAO(AnomalyFunctionManager anomalyFunctionDAO) {
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

  public EmailConfigurationManager getEmailConfigurationDAO() {
    return emailConfigurationDAO;
  }

  public void setEmailConfigurationDAO(EmailConfigurationManager emailConfigurationDAO) {
    this.emailConfigurationDAO = emailConfigurationDAO;
  }

}
