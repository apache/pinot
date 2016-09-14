package com.linkedin.thirdeye.integration;

import java.io.File;
import java.net.URISyntaxException;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.thirdeye.common.persistence.PersistenceConfig;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.bao.hibernate.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.EmailConfigurationManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.RawAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.TaskManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.WebappConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

public abstract class AbstractDBIntegrationTestBase {
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawAnomalyResultDAO;
  protected JobManager jobDAO;
  protected TaskManager taskDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected MergedAnomalyResultManager mergedResultDAO;
  protected WebappConfigManager webappConfigDAO;
  private EntityManager entityManager;

  @BeforeClass(alwaysRun = true)
  public void init() throws URISyntaxException {
    URL url = AbstractDBIntegrationTestBase.class.getResource("/persistence.yml");
    File configFile = new File(url.toURI());

    PersistenceConfig configuration = PersistenceUtil.createConfiguration(configFile);
    Properties properties = PersistenceUtil.createDbPropertiesFromConfiguration(configuration);

    long dbId = System.currentTimeMillis();

    DataSource ds = new DataSource();
    ds.setUrl(configuration.getDatabaseConfiguration().getUrl() + dbId);
    ds.setPassword(configuration.getDatabaseConfiguration().getPassword());
    ds.setUsername(configuration.getDatabaseConfiguration().getUser());
    ds.setDriverClassName(configuration.getDatabaseConfiguration().getProperties().get("hibernate.connection.driver_class"));

    // pool size configurations
    ds.setMaxActive(200);
    ds.setMinIdle(10);
    ds.setInitialSize(10);

    // validate connection
    ds.setValidationQuery("select 1 from anomaly_jobs where 1=0");
    ds.setTestWhileIdle(true);
    ds.setTestOnBorrow(true);

    // when returning connection to pool
    ds.setTestOnReturn(true);
    ds.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    ds.setRemoveAbandonedTimeout(600_000);
    ds.setRemoveAbandoned(true);

    properties.put(Environment.CONNECTION_PROVIDER, DatasourceConnectionProviderImpl.class.getName());
    properties.put(Environment.DATASOURCE, ds);

    PersistenceUtil.init(properties);

    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = PersistenceUtil.getInstance(RawAnomalyResultManagerImpl.class);
    jobDAO = PersistenceUtil.getInstance(JobManagerImpl.class);
    taskDAO = PersistenceUtil.getInstance(TaskManagerImpl.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationManagerImpl.class);
    mergedResultDAO = PersistenceUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    webappConfigDAO = PersistenceUtil.getInstance(WebappConfigManagerImpl.class);
    entityManager = PersistenceUtil.getInstance(EntityManager.class);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    if (entityManager.getTransaction().isActive()) {
      entityManager.getTransaction().rollback();
    }
    clearDatabase();
  }

  public void clearDatabase() throws Exception{
    Connection c = ((SessionImpl) entityManager.getDelegate()).connection();
    Statement s = c.createStatement();
    s.execute("SET DATABASE REFERENTIAL INTEGRITY FALSE");
    Set<String> tables = new HashSet<>();
    ResultSet rs = s.executeQuery("select table_name " +
        "from INFORMATION_SCHEMA.system_tables " +
        "where table_type='TABLE' and table_schem='PUBLIC'");
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
    functionSpec.setFunctionName("integration test function 1");
    functionSpec.setType("USER_RULE");
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
    emailConfiguration.setIsActive(true);
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

}
