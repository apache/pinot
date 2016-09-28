package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.datalayer.util.PersistenceConfig;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.ScriptRunner;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.ManagerProvider;

public abstract class AbstractManagerTestBase {
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawResultDAO;
  protected JobManager jobDAO;
  protected TaskManager taskDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected MergedAnomalyResultManager mergedResultDAO;
  protected WebappConfigManager webappConfigDAO;
  private ManagerProvider managerProvider;
  private PersistenceConfig configuration;

  private DataSource ds;
  private long dbId = System.currentTimeMillis();

  @BeforeClass(alwaysRun = true) public void init() throws Exception {
    URL url = AbstractManagerTestBase.class.getResource("/persistence-local.yml");
    File configFile = new File(url.toURI());
    configuration = DaoProviderUtil.createConfiguration(configFile);

    initializeDs(configuration);
    initJDBC();
  }

  void initializeDs(PersistenceConfig configuration) {
    ds = new DataSource();
    ds.setUrl(configuration.getDatabaseConfiguration().getUrl() + dbId);
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
  }

  //JDBC related init/cleanup
  public void initJDBC() throws Exception {
    cleanUp();
    initDB();
    initManagers();
  }

  public void initDB() throws Exception {
    try (Connection conn = ds.getConnection()) {
      // create schema
      URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.setDelimiter(";", true);
      scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
    }
  }

  public void cleanUpJDBC() throws Exception {
    System.out.println("Cleaning database: start");
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
    System.out.println("Cleaning database: done!");
  }

  public void initManagers() throws Exception {
    managerProvider = new ManagerProvider(ds);
    Class<AnomalyFunctionManagerImpl> c =
        com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class;
    System.out.println(c);
    anomalyFunctionDAO = (AnomalyFunctionManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawResultDAO = (RawAnomalyResultManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    jobDAO = (JobManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl.class);
    taskDAO = (TaskManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl.class);
    emailConfigurationDAO = (EmailConfigurationManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    mergedResultDAO = (MergedAnomalyResultManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
    webappConfigDAO = (WebappConfigManager) managerProvider
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.WebappConfigManagerImpl.class);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
      cleanUpJDBC();
  }

  protected AnomalyFunctionDTO getTestFunctionSpec(String metricName, String collection) {
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setFunctionName("integration test function 1");
    functionSpec.setType("WEEK_OVER_WEEK_RULE");
    functionSpec.setMetric(metricName);
    functionSpec.setCollection(collection);
    functionSpec.setMetricFunction(MetricAggFunction.SUM);
    functionSpec.setCron("0/10 * * * * ?");
    functionSpec.setBucketSize(1);
    functionSpec.setBucketUnit(TimeUnit.HOURS);
    functionSpec.setWindowDelay(3);
    functionSpec.setWindowDelayUnit(TimeUnit.HOURS);
    functionSpec.setWindowSize(1);
    functionSpec.setWindowUnit(TimeUnit.DAYS);
    functionSpec.setProperties("baseline=w/w;changeThreshold=0.001");
    functionSpec.setIsActive(true);
    return functionSpec;
  }

  protected EmailConfigurationDTO getTestEmailConfiguration(String metricName, String collection) {
    EmailConfigurationDTO emailConfiguration = new EmailConfigurationDTO();
    emailConfiguration.setCollection(collection);
    emailConfiguration.setActive(true);
    emailConfiguration.setCron("0/10 * * * * ?");
    emailConfiguration.setFilters(null);
    emailConfiguration.setFromAddress("thirdeye@linkedin.com");
    emailConfiguration.setMetric(metricName);
    emailConfiguration.setSendZeroAnomalyEmail(true);
    emailConfiguration.setSmtpHost("email-server.linkedin.com");
    emailConfiguration.setSmtpPassword(null);
    emailConfiguration.setSmtpPort(25);
    emailConfiguration.setSmtpUser(null);
    emailConfiguration.setToAddresses("anomaly@linkedin.com");
    emailConfiguration.setWindowDelay(2);
    emailConfiguration.setWindowSize(10);
    emailConfiguration.setWindowUnit(TimeUnit.HOURS);
    emailConfiguration.setWindowDelayUnit(TimeUnit.HOURS);
    return emailConfiguration;
  }

  protected RawAnomalyResultDTO getAnomalyResult() {
    RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
    anomalyResult.setScore(1.1);
    anomalyResult.setStartTime(System.currentTimeMillis());
    anomalyResult.setEndTime(System.currentTimeMillis());
    anomalyResult.setWeight(10.1);
    anomalyResult.setDimensions("xyz dimension");
    anomalyResult.setCreationTimeUtc(System.currentTimeMillis());
    return anomalyResult;
  }

  JobDTO getTestJobSpec() {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setWindowStartTime(new DateTime().minusHours(20).getMillis());
    jobSpec.setWindowEndTime(new DateTime().minusHours(10).getMillis());
    return jobSpec;
  }
}
