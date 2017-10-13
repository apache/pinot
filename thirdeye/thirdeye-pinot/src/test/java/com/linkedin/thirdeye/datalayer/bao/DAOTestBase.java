package com.linkedin.thirdeye.datalayer.bao;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.TestDBResources;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.DaoProvider;
import com.linkedin.thirdeye.datalayer.ScriptRunner;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datalayer.util.PersistenceConfig;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;

public class DAOTestBase implements DaoProvider {
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawAnomalyResultDAO;
  protected JobManager jobDAO;
  protected TaskManager taskDAO;
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected DatasetConfigManager datasetConfigDAO;
  protected MetricConfigManager metricConfigDAO;
  protected DashboardConfigManager dashboardConfigDAO;
  protected OverrideConfigManager overrideConfigDAO;
  protected AlertConfigManager alertConfigDAO;
  protected DataCompletenessConfigManager dataCompletenessConfigDAO;
  protected EventManager eventDAO;
  protected DetectionStatusManager detectionStatusDAO;
  protected AutotuneConfigManager autotuneConfigDAO;
  protected ClassificationConfigManager classificationConfigDAO;
  protected EntityToEntityMappingManager entityToEntityMappingDAO;
  protected GroupedAnomalyResultsManager groupedAnomalyResultsDAO;
  protected OnboardDatasetMetricManager onboardDatasetMetricDAO;
  protected ConfigManager configDAO;
  protected ApplicationManager applicationDAO;

  //  protected TestDBResources testDBResources;
  protected DAORegistry daoRegistry;
  DataSource ds;
  String dbUrlId;

  private static DaoProvider INSTANCE = new DAOTestBase();

  private DAOTestBase(){
    init();
  }

  public static DaoProvider getInstance(){
    return INSTANCE;
  }

  protected void init() {
    try {
      URL url = TestDBResources.class.getResource("/persistence-local.yml");
      File configFile = new File(url.toURI());
      PersistenceConfig configuration = DaoProviderUtil.createConfiguration(configFile);
      initializeDs(configuration);

      DaoProviderUtil.init(ds);

      daoRegistry = DAORegistry.getInstance();
      anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
      rawAnomalyResultDAO = daoRegistry.getRawAnomalyResultDAO();
      jobDAO = daoRegistry.getJobDAO();
      taskDAO = daoRegistry.getTaskDAO();
      mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
      datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
      metricConfigDAO = daoRegistry.getMetricConfigDAO();
      dashboardConfigDAO = daoRegistry.getDashboardConfigDAO();
      overrideConfigDAO = daoRegistry.getOverrideConfigDAO();
      alertConfigDAO = daoRegistry.getAlertConfigDAO();
      dataCompletenessConfigDAO = daoRegistry.getDataCompletenessConfigDAO();
      eventDAO = daoRegistry.getEventDAO();
      anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
      detectionStatusDAO = daoRegistry.getDetectionStatusDAO();
      autotuneConfigDAO = daoRegistry.getAutotuneConfigDAO();
      classificationConfigDAO = daoRegistry.getClassificationConfigDAO();
      entityToEntityMappingDAO = daoRegistry.getEntityToEntityMappingDAO();
      groupedAnomalyResultsDAO = daoRegistry.getGroupedAnomalyResultsDAO();
      onboardDatasetMetricDAO = daoRegistry.getOnboardDatasetMetricDAO();
      configDAO = daoRegistry.getConfigDAO();
      applicationDAO = daoRegistry.getApplicationDAO();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void cleanup() {
    try {
      cleanUpJDBC();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeDs(PersistenceConfig configuration) throws Exception {
    ds = new DataSource();
    dbUrlId =
        configuration.getDatabaseConfiguration().getUrl() + System.currentTimeMillis() + "" + Math
            .random();
    ds.setUrl(dbUrlId);
    System.out.println("Creating db with connection url : " + ds.getUrl());
    ds.setPassword(configuration.getDatabaseConfiguration().getPassword());
    ds.setUsername(configuration.getDatabaseConfiguration().getUser());
    ds.setDriverClassName(configuration.getDatabaseConfiguration().getProperties()
        .get("hibernate.connection.driver_class"));

    // pool size configurations
    ds.setMaxActive(200);
    ds.setMinIdle(10);
    ds.setInitialSize(10);

    // when returning connection to pool
    ds.setTestOnReturn(true);
    ds.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    ds.setRemoveAbandonedTimeout(600_000);
    ds.setRemoveAbandoned(true);

    Connection conn = ds.getConnection();
    // create schema
    URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
    ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
    scriptRunner.setDelimiter(";", true);
    scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
  }

  private void cleanUpJDBC() throws Exception {
    System.out.println("Cleaning database: start");
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
    new File(dbUrlId).delete();
    System.out.println("Cleaning database: done!");
  }

  @Override
  public AnomalyFunctionManager getAnomalyFunctionDAO() {
    return anomalyFunctionDAO;
  }

  @Override
  public RawAnomalyResultManager getRawAnomalyResultDAO() {
    return rawAnomalyResultDAO;
  }

  @Override
  public JobManager getJobDAO() {
    return jobDAO;
  }

  @Override
  public TaskManager getTaskDAO() {
    return taskDAO;
  }

  @Override
  public MergedAnomalyResultManager getMergedAnomalyResultDAO() {
    return mergedAnomalyResultDAO;
  }

  @Override
  public DatasetConfigManager getDatasetConfigDAO() {
    return datasetConfigDAO;
  }

  @Override
  public MetricConfigManager getMetricConfigDAO() {
    return metricConfigDAO;
  }

  @Override
  public DashboardConfigManager getDashboardConfigDAO() {
    return dashboardConfigDAO;
  }

  @Override
  public OverrideConfigManager getOverrideConfigDAO() {
    return overrideConfigDAO;
  }

  @Override
  public AlertConfigManager getAlertConfigDAO() {
    return alertConfigDAO;
  }

  @Override
  public DataCompletenessConfigManager getDataCompletenessConfigDAO() {
    return dataCompletenessConfigDAO;
  }

  @Override
  public EventManager getEventDAO() {
    return eventDAO;
  }

  @Override
  public DetectionStatusManager getDetectionStatusDAO() {
    return detectionStatusDAO;
  }

  @Override
  public AutotuneConfigManager getAutotuneConfigDAO() {
    return autotuneConfigDAO;
  }

  @Override
  public ClassificationConfigManager getClassificationConfigDAO() {
    return classificationConfigDAO;
  }

  @Override
  public EntityToEntityMappingManager getEntityToEntityMappingDAO() {
    return entityToEntityMappingDAO;
  }

  @Override
  public GroupedAnomalyResultsManager getGroupedAnomalyResultsDAO() {
    return groupedAnomalyResultsDAO;
  }

  @Override
  public OnboardDatasetMetricManager getOnboardDatasetMetricDAO() {
    return onboardDatasetMetricDAO;
  }

  @Override
  public ConfigManager getConfigDAO() {
    return configDAO;
  }

  @Override
  public ApplicationManager getApplicationDAO() {
    return applicationDAO;
  }

  @Override
  public void restart() {
    cleanup();
    init();
  }
}
