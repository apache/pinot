package com.linkedin.thirdeye.client;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
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

/**
 * Singleton service registry for Data Access Objects (DAOs)
 */
public class DAORegistry {
  private AnomalyFunctionManager anomalyFunctionDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private AlertConfigManager alertConfigDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private JobManager jobDAO;
  private TaskManager taskDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;
  private DashboardConfigManager dashboardConfigDAO;
  private IngraphDashboardConfigManager ingraphDashboardConfigDAO;
  private IngraphMetricConfigManager ingraphMetricConfigDAO;
  private OverrideConfigManager overrideConfigDAO;
  private DataCompletenessConfigManager dataCompletenessConfigDAO;
  private EventManager eventDAO;

  private static final DAORegistry singleton = new DAORegistry();

  /****************************************************************************
   * SINGLETON
   */

  public static DAORegistry getInstance() {
    return singleton;
  }

  /**
   * **USE FOR TESTING ONLY**
   * Reset registry to empty initial state.
   */
  public static void reset() {
    singleton.reset_internal();
  }

  /****************************************************************************
   * INTERNAL
   */

  private void reset_internal() {
    anomalyFunctionDAO = null;
    emailConfigurationDAO = null;
    alertConfigDAO = null;
    rawAnomalyResultDAO = null;
    mergedAnomalyResultDAO = null;
    jobDAO = null;
    taskDAO = null;
    datasetConfigDAO = null;
    metricConfigDAO = null;
    dashboardConfigDAO = null;
    ingraphDashboardConfigDAO = null;
    ingraphMetricConfigDAO = null;
    overrideConfigDAO = null;
    dataCompletenessConfigDAO = null;
    eventDAO = null;
  }

  /**
   * internal constructor.
   */
  private DAORegistry() {}

  /****************************************************************************
   * GETTERS/SETTERS
   */

  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return assertNotNull(anomalyFunctionDAO);
  }

  public void setAnomalyFunctionDAO(AnomalyFunctionManager anomalyFunctionDAO) {
    assertNull(this.anomalyFunctionDAO);
    this.anomalyFunctionDAO = anomalyFunctionDAO;
  }

  public EmailConfigurationManager getEmailConfigurationDAO() {
    return assertNotNull(emailConfigurationDAO);
  }

  public void setEmailConfigurationDAO(EmailConfigurationManager emailConfigurationDAO) {
    assertNull(this.emailConfigurationDAO);
    this.emailConfigurationDAO = emailConfigurationDAO;
  }

  public AlertConfigManager getAlertConfigDAO() {
    return assertNotNull(alertConfigDAO);
  }

  public void setAlertConfigDAO(AlertConfigManager alertConfigDAO) {
    assertNull(this.alertConfigDAO);
    this.alertConfigDAO = alertConfigDAO;
  }

  public RawAnomalyResultManager getRawAnomalyResultDAO() {
    return assertNotNull(rawAnomalyResultDAO);
  }

  public void setRawAnomalyResultDAO(RawAnomalyResultManager rawAnomalyResultDAO) {
    assertNull(this.rawAnomalyResultDAO);
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
  }

  public MergedAnomalyResultManager getMergedAnomalyResultDAO() {
    return assertNotNull(mergedAnomalyResultDAO);
  }

  public void setMergedAnomalyResultDAO(MergedAnomalyResultManager mergedAnomalyResultDAO) {
    assertNull(this.mergedAnomalyResultDAO);
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
  }

  public JobManager getJobDAO() {
    return assertNotNull(jobDAO);
  }

  public void setJobDAO(JobManager jobDAO) {
    assertNull(this.jobDAO);
    this.jobDAO = jobDAO;
  }

  public TaskManager getTaskDAO() {
    return assertNotNull(taskDAO);
  }

  public void setTaskDAO(TaskManager taskDAO) {
    assertNull(this.taskDAO);
    this.taskDAO = taskDAO;
  }

  public DatasetConfigManager getDatasetConfigDAO() {
    return assertNotNull(datasetConfigDAO);
  }

  public void setDatasetConfigDAO(DatasetConfigManager datasetConfigDAO) {
    assertNull(this.datasetConfigDAO);
    this.datasetConfigDAO = datasetConfigDAO;
  }

  public MetricConfigManager getMetricConfigDAO() {
    return assertNotNull(metricConfigDAO);
  }

  public void setMetricConfigDAO(MetricConfigManager metricConfigDAO) {
    assertNull(this.metricConfigDAO);
    this.metricConfigDAO = metricConfigDAO;
  }

  public DashboardConfigManager getDashboardConfigDAO() {
    return assertNotNull(dashboardConfigDAO);
  }

  public void setDashboardConfigDAO(DashboardConfigManager dashboardConfigDAO) {
    assertNull(this.dashboardConfigDAO);
    this.dashboardConfigDAO = dashboardConfigDAO;
  }

  public IngraphDashboardConfigManager getIngraphDashboardConfigDAO() {
    return assertNotNull(ingraphDashboardConfigDAO);
  }

  public void setIngraphDashboardConfigDAO(IngraphDashboardConfigManager ingraphDashboardConfigDAO) {
    assertNull(this.ingraphDashboardConfigDAO);
    this.ingraphDashboardConfigDAO = ingraphDashboardConfigDAO;
  }

  public IngraphMetricConfigManager getIngraphMetricConfigDAO() {
    return assertNotNull(ingraphMetricConfigDAO);
  }

  public void setIngraphMetricConfigDAO(IngraphMetricConfigManager ingraphMetricConfigDAO) {
    assertNull(this.ingraphMetricConfigDAO);
    this.ingraphMetricConfigDAO = ingraphMetricConfigDAO;
  }

  public OverrideConfigManager getOverrideConfigDAO() {
    return assertNotNull(overrideConfigDAO);
  }

  public void setOverrideConfigDAO(OverrideConfigManager overrideConfigDAO) {
    assertNull(this.overrideConfigDAO);
    this.overrideConfigDAO = overrideConfigDAO;
  }

  public DataCompletenessConfigManager getDataCompletenessConfigDAO() {
    return assertNotNull(dataCompletenessConfigDAO);
  }

  public void setDataCompletenessConfigDAO(DataCompletenessConfigManager dataCompletenessConfigDAO) {
    assertNull(this.dataCompletenessConfigDAO);
    this.dataCompletenessConfigDAO = dataCompletenessConfigDAO;
  }

  public EventManager getEventDAO() {
    return assertNotNull(eventDAO);
  }

  public void setEventDAO(EventManager eventDAO) {
    assertNull(this.eventDAO);
    this.eventDAO = eventDAO;
  }

  /****************************************************************************
   * HELPERS
   */

  private <T> T assertNotNull(T o) {
    if(o == null)
      throw new IllegalStateException("DAO not initialized");
    return o;
  }

  private void assertNull(Object o) {
    if(o != null)
      throw new IllegalStateException("DAO already initialized");
  }

}
