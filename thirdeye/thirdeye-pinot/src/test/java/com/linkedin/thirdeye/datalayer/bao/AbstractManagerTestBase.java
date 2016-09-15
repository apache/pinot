package com.linkedin.thirdeye.datalayer.bao;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.internal.DatasourceConnectionProviderImpl;
import org.hibernate.internal.SessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.ScriptRunner;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public abstract class AbstractManagerTestBase {
  String implMode = "jdbc";
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager anomalyResultDAO;
  protected JobManager anomalyJobDAO;
  protected TaskManager anomalyTaskDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected MergedAnomalyResultManager mergedResultDAO;
  protected WebappConfigManager webappConfigDAO;
  private EntityManager entityManager;
  private DataSource ds;

  @BeforeClass(alwaysRun = true)
  public void init() throws Exception {
    if (implMode.equalsIgnoreCase("hibernate")) {
      initHibernate();
    }
    if (implMode.equalsIgnoreCase("jdbc")) {
      initJDBC();
    }
  }

  //JDBC related init/cleanup
  public void initJDBC() throws Exception {
    URL configUrl = getClass().getResource("/persistence-local.yml");
    File configFile = new File(configUrl.toURI());
    DaoProviderUtil.init(configFile);
    ds = DaoProviderUtil.getDataSource();
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
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
  }


  //HIBERNATE related init/clean up
  public void initHibernate() throws Exception {
    URL url = AbstractManagerTestBase.class.getResource("/persistence.yml");
    File configFile = new File(url.toURI());

    PersistenceConfig configuration = PersistenceUtil.createConfiguration(configFile);
    Properties properties = PersistenceUtil.createDbPropertiesFromConfiguration(configuration);

    long dbId = System.currentTimeMillis();

    DataSource ds = new DataSource();
    ds.setUrl(configuration.getDatabaseConfiguration().getUrl() + dbId);
    ds.setPassword(configuration.getDatabaseConfiguration().getPassword());
    ds.setUsername(configuration.getDatabaseConfiguration().getUser());
    ds.setDriverClassName(configuration.getDatabaseConfiguration().getProperties()
        .get("hibernate.connection.driver_class"));

    // pool size configurations
    ds.setMaxActive(200);
    ds.setMinIdle(10);
    ds.setInitialSize(10);

    // validate connection
    ds.setValidationQuery("select 1 as dbcp_connection_test");
    ds.setTestWhileIdle(true);
    ds.setTestOnBorrow(true);

    // when returning connection to pool
    ds.setTestOnReturn(true);
    ds.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    ds.setRemoveAbandonedTimeout(600_000);
    ds.setRemoveAbandoned(true);

    properties.put(Environment.CONNECTION_PROVIDER,
        DatasourceConnectionProviderImpl.class.getName());
    properties.put(Environment.DATASOURCE, ds);

    PersistenceUtil.init(properties);
    initManagers();

  }

  String packagePrefix = "com.linkedin.thirdeye.datalayer.bao.";

  public void initManagers() throws Exception {
    if (implMode.equals("hibernate")) {
      anomalyFunctionDAO = (AnomalyFunctionManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".AnomalyFunctionManagerImpl"));
      anomalyResultDAO = (RawAnomalyResultManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".RawAnomalyResultManagerImpl"));
      anomalyJobDAO = (JobManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".JobManagerImpl"));
      anomalyTaskDAO = (TaskManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".TaskManagerImpl"));
      emailConfigurationDAO = (EmailConfigurationManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".EmailConfigurationManagerImpl"));
      mergedResultDAO = (MergedAnomalyResultManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".MergedAnomalyResultManagerImpl"));
      webappConfigDAO = (WebappConfigManager) PersistenceUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".WebappConfigManagerImpl"));

      entityManager = PersistenceUtil.getInstance(EntityManager.class);
    }
    if (implMode.equals("jdbc")) {
      anomalyFunctionDAO = (AnomalyFunctionManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".AnomalyFunctionManagerImpl"));
      anomalyResultDAO = (RawAnomalyResultManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".RawAnomalyResultManagerImpl"));
      anomalyJobDAO = (JobManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".JobManagerImpl"));
      anomalyTaskDAO = (TaskManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".TaskManagerImpl"));
      emailConfigurationDAO = (EmailConfigurationManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".EmailConfigurationManagerImpl"));
      mergedResultDAO = (MergedAnomalyResultManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".MergedAnomalyResultManagerImpl"));
      webappConfigDAO = (WebappConfigManager) DaoProviderUtil
          .getInstance(Class.forName(packagePrefix + implMode + ".WebappConfigManagerImpl"));
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    if (implMode.equalsIgnoreCase("hibernate")) {
      cleanUpHibernate();
    }
    if (implMode.equalsIgnoreCase("jdbc")) {
      cleanUpJDBC();
    }
  }

  public void cleanUpHibernate() throws Exception {
    if (entityManager.getTransaction().isActive()) {
      entityManager.getTransaction().rollback();
    }
    clearDatabase();
  }

  public void clearDatabase() throws Exception {
    Connection c = ((SessionImpl) entityManager.getDelegate()).connection();
    Statement s = c.createStatement();
    s.execute("SET DATABASE REFERENTIAL INTEGRITY FALSE");
    Set<String> tables = new HashSet<>();
    ResultSet rs = s.executeQuery("select table_name " + "from INFORMATION_SCHEMA.system_tables "
        + "where table_type='TABLE' and table_schem='PUBLIC'");
    while (rs.next()) {
      if (!rs.getString(1).startsWith("DUAL_")) {
        tables.add(rs.getString(1));
      }
    }
    rs.close();
    for (String table : tables) {
      s.executeUpdate("DELETE FROM " + table);
    }
    s.execute("SET DATABASE REFERENTIAL INTEGRITY TRUE");
    s.close();
  }

  protected AnomalyFunctionDTO getTestFunctionSpec(String metricName, String collection) {
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setMetricFunction(MetricAggFunction.SUM);
    functionSpec.setMetric(metricName);
    functionSpec.setBucketSize(5);
    functionSpec.setCollection(collection);
    functionSpec.setBucketUnit(TimeUnit.MINUTES);
    functionSpec.setCron("0 0/5 * * * ?");
    functionSpec.setFunctionName("my awesome test function");
    functionSpec.setType("USER_RULE");
    functionSpec.setWindowDelay(1);
    functionSpec.setWindowDelayUnit(TimeUnit.HOURS);
    functionSpec.setWindowSize(10);
    functionSpec.setWindowUnit(TimeUnit.HOURS);
    functionSpec.setIsActive(true);
    return functionSpec;
  }

  protected EmailConfigurationDTO getEmailConfiguration() {
    EmailConfigurationDTO emailConfiguration = new EmailConfigurationDTO();
    emailConfiguration.setCollection("my pinot collection");
    emailConfiguration.setActive(false);
    emailConfiguration.setCron("0 0/10 * * *");
    emailConfiguration.setFilters("foo=bar");
    emailConfiguration.setFromAddress("thirdeye@linkedin.com");
    emailConfiguration.setMetric("my metric");
    emailConfiguration.setSendZeroAnomalyEmail(false);
    emailConfiguration.setSmtpHost("pinot-email-host");
    emailConfiguration.setSmtpPassword("mypass");
    emailConfiguration.setSmtpPort(1000);
    emailConfiguration.setSmtpUser("user");
    emailConfiguration.setToAddresses("puneet@linkedin.com");
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
