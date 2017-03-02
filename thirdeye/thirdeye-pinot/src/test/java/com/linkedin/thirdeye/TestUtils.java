package com.linkedin.thirdeye;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.ScriptRunner;
import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DashboardConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
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
import com.linkedin.thirdeye.datalayer.util.ManagerProvider;
import com.linkedin.thirdeye.datalayer.util.PersistenceConfig;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import org.apache.tomcat.jdbc.pool.DataSource;


public class TestUtils {
  private DataSource ds;
  private DAORegistry daoRegistry;

  public static TestUtils setupDAO() {
    TestUtils instance = new TestUtils();
    instance.setTestDaoRegistry(DAORegistry.getTestInstance());
    try {
      DAORegistry.resetForTesting();
      instance.init();
      System.out.println("DAOs initialized");
      return instance;
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void teardownDAO(TestUtils instance) {
    try {
      System.out.println("DAOs cleaned up");
      instance.cleanUp();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setTestDaoRegistry(DAORegistry daoRegistry) {
    this.daoRegistry = daoRegistry;
  }

  public DAORegistry getTestDaoRegistry() {
    return daoRegistry;
  }

  private void init() throws Exception {
    URL url = AbstractManagerTestBase.class.getResource("/persistence-local.yml");
    File configFile = new File(url.toURI());
    PersistenceConfig configuration = DaoProviderUtil.createConfiguration(configFile);

    initializeDs(configuration);
    initJDBC();
  }

  private void cleanUp() throws Exception {
    cleanUpJDBC();
  }

  private void initializeDs(PersistenceConfig configuration) {
    String dbId = System.currentTimeMillis() + "" + Math.random();

    ds = new DataSource();
    ds.setUrl(configuration.getDatabaseConfiguration().getUrl() + dbId);
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
  }

  //JDBC related init/cleanup
  private void initJDBC() throws Exception {
    initDB();
    initManagers();
  }

  private void initDB() throws Exception {
    try (Connection conn = ds.getConnection()) {
      // create schema
      URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.setDelimiter(";", true);
      scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
    }
  }

  private void cleanUpJDBC() throws Exception {
    System.out.println("Cleaning database: start");
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
    System.out.println("Cleaning database: done!");
  }

  private void initManagers() throws Exception {
    ManagerProvider managerProvider = new ManagerProvider(ds);
    Class<AnomalyFunctionManagerImpl> c = AnomalyFunctionManagerImpl.class;
    System.out.println(c);

    daoRegistry.setAnomalyFunctionDAO(managerProvider.getInstance(AnomalyFunctionManagerImpl.class));
    daoRegistry.setEmailConfigurationDAO(managerProvider.getInstance(EmailConfigurationManagerImpl.class));
    daoRegistry.setRawAnomalyResultDAO(managerProvider.getInstance(RawAnomalyResultManagerImpl.class));
    daoRegistry.setMergedAnomalyResultDAO(managerProvider.getInstance(MergedAnomalyResultManagerImpl.class));
    daoRegistry.setJobDAO(managerProvider.getInstance(JobManagerImpl.class));
    daoRegistry.setTaskDAO(managerProvider.getInstance(TaskManagerImpl.class));
    daoRegistry.setDatasetConfigDAO(managerProvider.getInstance(DatasetConfigManagerImpl.class));
    daoRegistry.setMetricConfigDAO(managerProvider.getInstance(MetricConfigManagerImpl.class));
    daoRegistry.setDashboardConfigDAO(managerProvider.getInstance(DashboardConfigManagerImpl.class));
    daoRegistry.setIngraphMetricConfigDAO(managerProvider.getInstance(IngraphMetricConfigManagerImpl.class));
    daoRegistry.setIngraphDashboardConfigDAO(managerProvider.getInstance(IngraphDashboardConfigManagerImpl.class));
    daoRegistry.setOverrideConfigDAO(managerProvider.getInstance(OverrideConfigManagerImpl.class));
    daoRegistry.setAlertConfigDAO(managerProvider.getInstance(AlertConfigManagerImpl.class));
    daoRegistry.setDataCompletenessConfigDAO(managerProvider.getInstance(DataCompletenessConfigManagerImpl.class));
    daoRegistry.setEventDAO(managerProvider.getInstance(EventManagerImpl.class));
  }

}
