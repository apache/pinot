package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;

import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;

public abstract class AbstractDbTestBase {
  AnomalyFunctionDAO anomalyFunctionDAO;
  AnomalyResultDAO anomalyResultDAO;
  AnomalyJobDAO anomalyJobDAO;
  AnomalyTaskDAO anomalyTaskDAO;
  EmailConfigurationDAO emailConfigurationDAO;

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
  }

  AnomalyFunctionSpec getTestFunctionSpec(String metricName, String collection) {
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

  EmailConfiguration getEmailConfiguration() {
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
}
