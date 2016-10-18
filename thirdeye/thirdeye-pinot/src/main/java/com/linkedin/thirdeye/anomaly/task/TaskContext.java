package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class TaskContext {

  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private RawAnomalyResultManager resultDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;
  private ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration;

  public ThirdEyeAnomalyConfiguration getThirdEyeAnomalyConfiguration() {
    return thirdEyeAnomalyConfiguration;
  }

  public void setThirdEyeAnomalyConfiguration(
      ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration) {
    this.thirdEyeAnomalyConfiguration = thirdEyeAnomalyConfiguration;
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

}
