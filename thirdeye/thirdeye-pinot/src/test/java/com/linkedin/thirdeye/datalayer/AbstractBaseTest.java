package com.linkedin.thirdeye.datalayer;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.testng.annotations.BeforeClass;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public class AbstractBaseTest {


  private DataSource ds;

  @BeforeClass()
  public void init() throws Exception {
    URL configUrl = getClass().getResource("/persistence-local.yml");
    File configFile = new File(configUrl.toURI());
    DaoProviderUtil.init(configFile);
    ds = DaoProviderUtil.getDataSource();
    cleanUp();
  }

  @AfterClass
  public void cleanUp() throws Exception {
    try (Connection conn = ds.getConnection()) {
      URL deleteSchemaUrl = getClass().getResource("/schema/drop-tables.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.runScript(new FileReader(deleteSchemaUrl.getFile()));
    }
  }

  @BeforeClass(dependsOnMethods = "init")
  public void initDB() throws Exception {
    try (Connection conn = ds.getConnection()) {
      // create schema
      URL createSchemaUrl = getClass().getResource("/schema/create-schema.sql");
      ScriptRunner scriptRunner = new ScriptRunner(conn, false, false);
      scriptRunner.setDelimiter(";", true);
      scriptRunner.runScript(new FileReader(createSchemaUrl.getFile()));
    }
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

  public static void main(String[] args) throws Exception {
    AbstractBaseTest baseTest = new AbstractBaseTest();
    try {
      baseTest.init();
    } finally {
      // this drops the tables
      baseTest.cleanUp();
    }
  }
}
