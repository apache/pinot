package com.linkedin.thirdeye.client;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionStatusManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.IngraphDashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DashboardConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.IngraphDashboardConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.IngraphMetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
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


  public EmailConfigurationManager getEmailConfigurationDAO() {
    return DaoProviderUtil.getInstance(EmailConfigurationManagerImpl.class);
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


  public DashboardConfigManager getDashboardConfigDAO() {
    return DaoProviderUtil.getInstance(DashboardConfigManagerImpl.class);
  }


  public IngraphDashboardConfigManager getIngraphDashboardConfigDAO() {
    return DaoProviderUtil.getInstance(IngraphDashboardConfigManagerImpl.class);
  }

  public IngraphMetricConfigManager getIngraphMetricConfigDAO() {
    return DaoProviderUtil.getInstance(IngraphMetricConfigManagerImpl.class);
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

}
