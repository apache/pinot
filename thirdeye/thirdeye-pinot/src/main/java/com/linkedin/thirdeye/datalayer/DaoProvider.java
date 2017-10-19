package com.linkedin.thirdeye.datalayer;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.ConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionStatusManager;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.GroupedAnomalyResultsManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OnboardDatasetMetricManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;


public interface DaoProvider {

  /**
   * This is for unit test usage. The restart is to clean up the DB so that the previous tests don't affect the following
   * unit tests.
   */
  void cleanup();

  AnomalyFunctionManager getAnomalyFunctionDAO();

  AlertConfigManager getAlertConfigDAO();

  RawAnomalyResultManager getRawAnomalyResultDAO();

  MergedAnomalyResultManager getMergedAnomalyResultDAO();

  JobManager getJobDAO();

  TaskManager getTaskDAO();

  DatasetConfigManager getDatasetConfigDAO();

  MetricConfigManager getMetricConfigDAO();

  DashboardConfigManager getDashboardConfigDAO();

  OverrideConfigManager getOverrideConfigDAO();

  DataCompletenessConfigManager getDataCompletenessConfigDAO();

  EventManager getEventDAO();

  DetectionStatusManager getDetectionStatusDAO();

  AutotuneConfigManager getAutotuneConfigDAO();

  ClassificationConfigManager getClassificationConfigDAO();

  EntityToEntityMappingManager getEntityToEntityMappingDAO();

  GroupedAnomalyResultsManager getGroupedAnomalyResultsDAO();

  OnboardDatasetMetricManager getOnboardDatasetMetricDAO();

  ConfigManager getConfigDAO();

  ApplicationManager getApplicationDAO();
}
