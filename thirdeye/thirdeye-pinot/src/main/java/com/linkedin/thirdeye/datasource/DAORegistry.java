package com.linkedin.thirdeye.datasource;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AlertSnapshotManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.ConfigManager;
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
import com.linkedin.thirdeye.datalayer.bao.RootcauseSessionManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertSnapshotManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.ApplicationManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AutotuneConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.ClassificationConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.ConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EntityToEntityMappingManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.GroupedAnomalyResultsManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.OnboardDatasetMetricManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.RootcauseSessionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

/**
 * Singleton service registry for Data Access Objects (DAOs)
 */
public class DAORegistry {

  private static final DAORegistry INSTANCE = new DAORegistry();

  /****************************************************************************
   * SINGLETON
   */

  public static DAORegistry getInstance() {
    return INSTANCE;
  }

  /**
   * **USE FOR TESTING ONLY**
   * Return a DAO registry for testing purpose, which may be performed in arbitrary order and
   * hence need independent registry for each test.
   *
   * @return an independent DAO registry to the global singleton registry.
   */
  public static DAORegistry getTestInstance() {
    return new DAORegistry();
  }



  /**
   * internal constructor.
   */
  private DAORegistry() {}

  /****************************************************************************
   * GETTERS/SETTERS
   */

  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return DaoProviderUtil.getInstance(AnomalyFunctionManagerImpl.class);
  }

  public AlertConfigManager getAlertConfigDAO() {
    return DaoProviderUtil.getInstance(AlertConfigManagerImpl.class);
  }

  public RawAnomalyResultManager getRawAnomalyResultDAO() {
    return DaoProviderUtil.getInstance(RawAnomalyResultManagerImpl.class);
  }

  public MergedAnomalyResultManager getMergedAnomalyResultDAO() {
    return DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
  }

  public JobManager getJobDAO() {
    return DaoProviderUtil.getInstance(JobManagerImpl.class);
  }

  public TaskManager getTaskDAO() {
    return DaoProviderUtil.getInstance(TaskManagerImpl.class);
  }

  public DatasetConfigManager getDatasetConfigDAO() {
    return DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
  }

  public MetricConfigManager getMetricConfigDAO() {
    return DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);
  }

  public OverrideConfigManager getOverrideConfigDAO() {
    return DaoProviderUtil.getInstance(OverrideConfigManagerImpl.class);
  }

  public DataCompletenessConfigManager getDataCompletenessConfigDAO() {
    return DaoProviderUtil.getInstance(DataCompletenessConfigManagerImpl.class);
  }

  public EventManager getEventDAO() {
    return DaoProviderUtil.getInstance(EventManagerImpl.class);
  }

  public DetectionStatusManager getDetectionStatusDAO() {
    return DaoProviderUtil.getInstance(DetectionStatusManagerImpl.class);
  }

  public AutotuneConfigManager getAutotuneConfigDAO() {
    return DaoProviderUtil.getInstance(AutotuneConfigManagerImpl.class);
  }

  public ClassificationConfigManager getClassificationConfigDAO() {
    return DaoProviderUtil.getInstance(ClassificationConfigManagerImpl.class);
  }

  public EntityToEntityMappingManager getEntityToEntityMappingDAO() {
    return DaoProviderUtil.getInstance(EntityToEntityMappingManagerImpl.class);
  }

  public GroupedAnomalyResultsManager getGroupedAnomalyResultsDAO() {
    return DaoProviderUtil.getInstance(GroupedAnomalyResultsManagerImpl.class);
  }

  public OnboardDatasetMetricManager getOnboardDatasetMetricDAO() {
    return DaoProviderUtil.getInstance(OnboardDatasetMetricManagerImpl.class);
  }

  public ConfigManager getConfigDAO() {
    return DaoProviderUtil.getInstance(ConfigManagerImpl.class);
  }

  public ApplicationManager getApplicationDAO() {
    return DaoProviderUtil.getInstance(ApplicationManagerImpl.class);
  }

  public AlertSnapshotManager getAlertSnapshotDAO() {
    return DaoProviderUtil.getInstance(AlertSnapshotManagerImpl.class);
  }

  public RootcauseSessionManager getRootcauseSessionDAO() {
    return DaoProviderUtil.getInstance(RootcauseSessionManagerImpl.class);
  }
}
