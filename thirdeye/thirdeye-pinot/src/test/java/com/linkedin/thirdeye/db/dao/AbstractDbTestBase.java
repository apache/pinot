package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.constant.MetricAggFunction;

import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.persistence.EntityManager;
import org.hibernate.internal.SessionImpl;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class AbstractDbTestBase {
  protected AnomalyFunctionDAO anomalyFunctionDAO;
  protected AnomalyResultDAO anomalyResultDAO;
  protected AnomalyJobDAO anomalyJobDAO;
  protected AnomalyTaskDAO anomalyTaskDAO;
  protected EmailConfigurationDAO emailConfigurationDAO;
  protected AnomalyMergedResultDAO mergedResultDAO;
  private EntityManager entityManager;

  @BeforeClass(alwaysRun = true)
  public void init() throws URISyntaxException {
    URL url = AbstractDbTestBase.class.getResource("/persistence.yml");
    File configFile = new File(url.toURI());
    PersistenceUtil.init(configFile);
    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    anomalyResultDAO = PersistenceUtil.getInstance(AnomalyResultDAO.class);
    anomalyJobDAO = PersistenceUtil.getInstance(AnomalyJobDAO.class);
    anomalyTaskDAO = PersistenceUtil.getInstance(AnomalyTaskDAO.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationDAO.class);
    mergedResultDAO = PersistenceUtil.getInstance(AnomalyMergedResultDAO.class);
    entityManager = PersistenceUtil.getInstance(EntityManager.class);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    if(entityManager.getTransaction().isActive()) {
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

  protected AnomalyFunctionSpec getTestFunctionSpec(String metricName, String collection) {
    AnomalyFunctionSpec functionSpec = new AnomalyFunctionSpec();
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
    return functionSpec;
  }

  protected EmailConfiguration getEmailConfiguration() {
    EmailConfiguration emailConfiguration = new EmailConfiguration();
    emailConfiguration.setCollection("my pinot collection");
    emailConfiguration.setIsActive(false);
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

  protected AnomalyResult getAnomalyResult() {
    AnomalyResult anomalyResult = new AnomalyResult();
    anomalyResult.setScore(1.1);
    anomalyResult.setStartTimeUtc(System.currentTimeMillis());
    anomalyResult.setEndTimeUtc(System.currentTimeMillis());
    anomalyResult.setWeight(10.1);
    anomalyResult.setDimensions("xyz dimension");
    anomalyResult.setCreationTimeUtc(System.currentTimeMillis());
    return anomalyResult;
  }

  AnomalyJobSpec getTestJobSpec() {
    AnomalyJobSpec jobSpec = new AnomalyJobSpec();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setWindowStartTime(new DateTime().minusHours(20).getMillis());
    jobSpec.setWindowEndTime(new DateTime().minusHours(10).getMillis());
    return jobSpec;
  }
}
