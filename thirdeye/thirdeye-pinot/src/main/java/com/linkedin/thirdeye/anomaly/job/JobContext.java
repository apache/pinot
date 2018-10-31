package com.linkedin.thirdeye.anomaly.job;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;

public abstract class JobContext {

  // Todo : remove DAOs from here as we can inject these wherever needed.
  private JobManager jobDAO;
  private TaskManager taskDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;

  private String jobName;
  private long jobExecutionId;


  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public JobManager getJobDAO() {
    return jobDAO;
  }

  public void setJobDAO(JobManager jobDAO) {
    this.jobDAO = jobDAO;
  }

  public TaskManager getTaskDAO() {
    return taskDAO;
  }

  public void setTaskDAO(TaskManager taskDAO) {
    this.taskDAO = taskDAO;
  }

  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return anomalyFunctionDAO;
  }

  public void setAnomalyFunctionDAO(AnomalyFunctionManager anomalyFunctionDAO2) {
    this.anomalyFunctionDAO = anomalyFunctionDAO2;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public DatasetConfigManager getDatasetConfigDAO() {
    return datasetConfigDAO;
  }

  public void setDatasetConfigDAO(DatasetConfigManager datasetConfigDAO) {
    this.datasetConfigDAO = datasetConfigDAO;
  }

  public MetricConfigManager getMetricConfigDAO() {
    return metricConfigDAO;
  }

  public void setMetricConfigDAO(MetricConfigManager metricConfigDAO) {
    this.metricConfigDAO = metricConfigDAO;
  }

}
