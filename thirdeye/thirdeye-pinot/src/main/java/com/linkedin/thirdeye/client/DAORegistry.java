package com.linkedin.thirdeye.client;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.IngraphDashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;

public class DAORegistry {
  private static AnomalyFunctionManager anomalyFunctionDAO;
  private static EmailConfigurationManager emailConfigurationDAO;
  private static AlertConfigManager alertConfigDAO;
  private static RawAnomalyResultManager rawAnomalyResultDAO;
  private static MergedAnomalyResultManager mergedAnomalyResultDAO;
  private static JobManager jobDAO;
  private static TaskManager taskDAO;
  private static DatasetConfigManager datasetConfigDAO;
  private static MetricConfigManager metricConfigDAO;
  private static DashboardConfigManager dashboardConfigDAO;
  private static IngraphDashboardConfigManager ingraphDashboardConfigDAO;
  private static IngraphMetricConfigManager ingraphMetricConfigDAO;
  private static OverrideConfigManager overrideConfigDAO;
  private static DataCompletenessConfigManager dataCompletenessConfigDAO;

  private static class Holder {
    static final DAORegistry INSTANCE = new DAORegistry();
  }

  public static DAORegistry getInstance() {
    return Holder.INSTANCE;
  }

  public static void registerDAOs(AnomalyFunctionManager anomalyFunctionDAO,
      EmailConfigurationManager emailConfigurationDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, JobManager jobDAO,  TaskManager taskDAO,
      DatasetConfigManager datasetConfigDAO, MetricConfigManager metricConfigDAO,
      DashboardConfigManager dashboardConfigDAO, IngraphMetricConfigManager ingraphMetricConfigDAO,
      IngraphDashboardConfigManager ingraphDashboardConfigDAO,
      OverrideConfigManager overrideConfigDAO, AlertConfigManager alertConfigDAO,
      DataCompletenessConfigManager dataCompletenessConfigDAO) {

    DAORegistry daoRegistry = DAORegistry.getInstance();
    daoRegistry.registerAnomalyFunctionDAO(anomalyFunctionDAO);
    daoRegistry.registerEmailConfigurationDAO(emailConfigurationDAO);
    daoRegistry.registerRawAnomalyResultDAO(rawAnomalyResultDAO);
    daoRegistry.registerMergedAnomalyResultDAO(mergedAnomalyResultDAO);
    daoRegistry.registerJobDAO(jobDAO);
    daoRegistry.registerTaskDAO(taskDAO);
    daoRegistry.registerDatasetConfigDAO(datasetConfigDAO);
    daoRegistry.registerDashboardConfigDAO(dashboardConfigDAO);
    daoRegistry.registerMetricConfigDAO(metricConfigDAO);
    daoRegistry.registerIngraphDashboardConfigDAO(ingraphDashboardConfigDAO);
    daoRegistry.registerIngraphMetricConfigDAO(ingraphMetricConfigDAO);
    daoRegistry.registerOverrideConfigDAO(overrideConfigDAO);
    daoRegistry.registerAlertConfigDAO(alertConfigDAO);
    daoRegistry.registerDataCompletenessConfigDAO(dataCompletenessConfigDAO);
  }

  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return anomalyFunctionDAO;
  }

  private void registerAnomalyFunctionDAO(AnomalyFunctionManager anomalyFunctionDAO) {
    DAORegistry.anomalyFunctionDAO = anomalyFunctionDAO;
  }

  public EmailConfigurationManager getEmailConfigurationDAO() {
    return emailConfigurationDAO;
  }

  private void registerEmailConfigurationDAO(EmailConfigurationManager emailConfigurationDAO) {
    DAORegistry.emailConfigurationDAO = emailConfigurationDAO;
  }

  public RawAnomalyResultManager getRawAnomalyResultDAO() {
    return rawAnomalyResultDAO;
  }

  private void registerRawAnomalyResultDAO(RawAnomalyResultManager rawAnomalyResultDAO) {
    DAORegistry.rawAnomalyResultDAO = rawAnomalyResultDAO;
  }

  public MergedAnomalyResultManager getMergedAnomalyResultDAO() {
    return mergedAnomalyResultDAO;
  }

  private void registerMergedAnomalyResultDAO(MergedAnomalyResultManager mergedAnomalyResultDAO) {
    DAORegistry.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
  }

  public JobManager getJobDAO() {
    return jobDAO;
  }

  private void registerJobDAO(JobManager jobDAO) {
    DAORegistry.jobDAO = jobDAO;
  }

  public TaskManager getTaskDAO() {
    return taskDAO;
  }

  private void registerTaskDAO(TaskManager taskDAO) {
    DAORegistry.taskDAO = taskDAO;
  }

  public DatasetConfigManager getDatasetConfigDAO() {
    return datasetConfigDAO;
  }

  private void registerDatasetConfigDAO(DatasetConfigManager datasetConfigDAO) {
    DAORegistry.datasetConfigDAO = datasetConfigDAO;
  }

  public MetricConfigManager getMetricConfigDAO() {
    return metricConfigDAO;
  }

  private void registerMetricConfigDAO(MetricConfigManager metricConfigDAO) {
    DAORegistry.metricConfigDAO = metricConfigDAO;
  }

  public DashboardConfigManager getDashboardConfigDAO() {
    return dashboardConfigDAO;
  }

  private void registerDashboardConfigDAO(DashboardConfigManager dashboardConfigDAO) {
    DAORegistry.dashboardConfigDAO = dashboardConfigDAO;
  }

  public IngraphMetricConfigManager getIngraphMetricConfigDAO() {
    return ingraphMetricConfigDAO;
  }

  private void registerIngraphMetricConfigDAO(IngraphMetricConfigManager ingraphMetricConfigDAO) {
    DAORegistry.ingraphMetricConfigDAO = ingraphMetricConfigDAO;
  }

  public IngraphDashboardConfigManager getIngraphDashboardConfigDAO() {
    return ingraphDashboardConfigDAO;
  }

  private void registerIngraphDashboardConfigDAO(IngraphDashboardConfigManager ingraphDashboardConfigDAO) {
    DAORegistry.ingraphDashboardConfigDAO = ingraphDashboardConfigDAO;
  }

  private void registerOverrideConfigDAO(OverrideConfigManager overrideConfigDAO) {
    DAORegistry.overrideConfigDAO = overrideConfigDAO;
  }

  public OverrideConfigManager getOverrideConfigDAO() {
    return DAORegistry.overrideConfigDAO;
  }

  public AlertConfigManager getAlertConfigDAO() {
    return alertConfigDAO;
  }

  private void registerAlertConfigDAO(AlertConfigManager alertConfigDAO) {
    DAORegistry.alertConfigDAO = alertConfigDAO;
  }

  public DataCompletenessConfigManager getDataCompletenessConfigDAO() {
    return dataCompletenessConfigDAO;
  }

  private void registerDataCompletenessConfigDAO(DataCompletenessConfigManager dataCompletenessConfigDAO) {
    DAORegistry.dataCompletenessConfigDAO = dataCompletenessConfigDAO;
  }
}
