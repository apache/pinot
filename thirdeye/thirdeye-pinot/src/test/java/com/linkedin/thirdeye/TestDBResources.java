package com.linkedin.thirdeye;

import com.linkedin.thirdeye.datalayer.ScriptRunner;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datalayer.util.PersistenceConfig;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;

import org.apache.tomcat.jdbc.pool.DataSource;


public class TestDBResources {
  private DataSource ds;
  private DAORegistry daoRegistry;

  public static TestDBResources setupDAO() throws Exception {
    URL url = TestDBResources.class.getResource("/persistence-local.yml");
    File configFile = new File(url.toURI());
    DaoProviderUtil.init(configFile);
//    TestDBResources instance = new TestDBResources();
//    instance.setTestDaoRegistry(DAORegistry.getInstance());
//    try {
//
//      instance.init();
//      System.out.println("DAOs initialized");
//      return instance;
//    } catch(Exception e) {
//      throw new RuntimeException(e);
//    }
    return null;
  }

  public static void teardownDAO(TestDBResources instance) {
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
    URL url = DAOTestBase.class.getResource("/persistence-local.yml");
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

}
