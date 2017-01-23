package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
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
    overrideConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl.class);
    alertConfigDAO = DaoProviderUtil.getInstance(AlertConfigManagerImpl.class);
    DAORegistry.registerDAOs(anomalyFunctionDAO, emailConfigurationDAO, rawAnomalyResultDAO,
            mergedAnomalyResultDAO, jobDAO, taskDAO, datasetConfigDAO, metricConfigDAO,
            dashboardConfigDAO, ingraphMetricConfigDAO, ingraphDashboardConfigDAO,
            overrideConfigDAO, alertConfigDAO);
  }
}
