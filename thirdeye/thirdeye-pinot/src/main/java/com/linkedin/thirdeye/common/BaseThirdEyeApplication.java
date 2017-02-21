package com.linkedin.thirdeye.common;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;

import com.linkedin.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.IngraphDashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.IngraphMetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

import io.dropwizard.Application;
import io.dropwizard.Configuration;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

  public static MetricsRegistry metricsRegistry = new MetricsRegistry();
  static JmxReporter jmxReporter = new JmxReporter(metricsRegistry);

  public static final Counter dbCallCounter =
      metricsRegistry.newCounter(BaseThirdEyeApplication.class, "dbCallCounter");

  static {
    jmxReporter.start();
  }

  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawAnomalyResultDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected JobManager jobDAO;
  protected TaskManager taskDAO;
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected DatasetConfigManager datasetConfigDAO;
  protected MetricConfigManager metricConfigDAO;
  protected DashboardConfigManager dashboardConfigDAO;
  protected IngraphDashboardConfigManager ingraphDashboardConfigDAO;
  protected IngraphMetricConfigManager ingraphMetricConfigDAO;
  protected OverrideConfigManager overrideConfigDAO;
  protected AlertConfigManager alertConfigDAO;
  protected DataCompletenessConfigManager dataCompletenessConfigDAO;

  protected DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public void initDAOs() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    DaoProviderUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    emailConfigurationDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    jobDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl.class);
    taskDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
    datasetConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl.class);
    metricConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl.class);
    dashboardConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DashboardConfigManagerImpl.class);
    ingraphDashboardConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.IngraphDashboardConfigManagerImpl.class);
    ingraphMetricConfigDAO = DaoProviderUtil
            .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.IngraphMetricConfigManagerImpl.class);
    overrideConfigDAO = DaoProviderUtil.getInstance(OverrideConfigManagerImpl.class);
    alertConfigDAO = DaoProviderUtil.getInstance(AlertConfigManagerImpl.class);
    dataCompletenessConfigDAO = DaoProviderUtil.getInstance(DataCompletenessConfigManagerImpl.class);

    DAO_REGISTRY.setAnomalyFunctionDAO(anomalyFunctionDAO);
    DAO_REGISTRY.setEmailConfigurationDAO(emailConfigurationDAO);
    DAO_REGISTRY.setRawAnomalyResultDAO(rawAnomalyResultDAO);
    DAO_REGISTRY.setMergedAnomalyResultDAO(mergedAnomalyResultDAO);
    DAO_REGISTRY.setJobDAO(jobDAO);
    DAO_REGISTRY.setTaskDAO(taskDAO);
    DAO_REGISTRY.setDatasetConfigDAO(datasetConfigDAO);
    DAO_REGISTRY.setMetricConfigDAO(metricConfigDAO);
    DAO_REGISTRY.setDashboardConfigDAO(dashboardConfigDAO);
    DAO_REGISTRY.setIngraphMetricConfigDAO(ingraphMetricConfigDAO);
    DAO_REGISTRY.setIngraphDashboardConfigDAO(ingraphDashboardConfigDAO);
    DAO_REGISTRY.setOverrideConfigDAO(overrideConfigDAO);
    DAO_REGISTRY.setAlertConfigDAO(alertConfigDAO);
    DAO_REGISTRY.setDataCompletenessConfigDAO(dataCompletenessConfigDAO);
    DAO_REGISTRY.setEventDAO(DaoProviderUtil.getInstance(EventManagerImpl.class));
  }
}
