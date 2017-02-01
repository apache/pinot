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
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private JobManager jobDAO;
  private TaskManager taskDAO;
  private RawAnomalyResultManager resultDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;
  private OverrideConfigManager overrideConfigDAO;
  private DataCompletenessConfigManager dataCompletenessConfigDAO;
  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;

  public ThirdEyeAnomalyConfiguration getThirdEyeAnomalyConfiguration() {
    return thirdEyeAnomalyConfiguration;
  }

  public void setThirdEyeAnomalyConfiguration(
      ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration) {
    this.thirdEyeAnomalyConfiguration = thirdEyeAnomalyConfiguration;
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

  public RawAnomalyResultManager getResultDAO() {
    return resultDAO;
  }

  public void setResultDAO(RawAnomalyResultManager anomalyResultDAO) {
    this.resultDAO = anomalyResultDAO;
  }

  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return anomalyFunctionFactory;
  }

  public void setAnomalyFunctionFactory(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public MergedAnomalyResultManager getMergedResultDAO() {
    return mergedResultDAO;
  }

  public void setMergedResultDAO(MergedAnomalyResultManager mergedResultDAO) {
    this.mergedResultDAO = mergedResultDAO;
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

  public OverrideConfigManager getOverrideConfigDAO() {
    return overrideConfigDAO;
  }

  public void setOverrideConfigDAO(OverrideConfigManager overrideConfigDAO) {
    this.overrideConfigDAO = overrideConfigDAO;
  }

  public DataCompletenessConfigManager getDataCompletenessConfigDAO() { return dataCompletenessConfigDAO; }

  public void setDataCompletenessConfigDAO(DataCompletenessConfigManager dataCompletenessConfigDAO) { this.dataCompletenessConfigDAO = dataCompletenessConfigDAO; }
}
