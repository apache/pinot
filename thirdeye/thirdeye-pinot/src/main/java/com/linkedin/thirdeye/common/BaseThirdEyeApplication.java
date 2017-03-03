package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DashboardConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected DatasetConfigManager datasetConfigDAO;
  protected MetricConfigManager metricConfigDAO;
  protected DashboardConfigManager dashboardConfigDAO;

  protected DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public void initDAOs() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    DaoProviderUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = DaoProviderUtil.getInstance(AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil.getInstance(RawAnomalyResultManagerImpl.class);
    emailConfigurationDAO = DaoProviderUtil.getInstance(EmailConfigurationManagerImpl.class);
    jobDAO = DaoProviderUtil.getInstance(JobManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    datasetConfigDAO = DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
    metricConfigDAO = DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);
    dashboardConfigDAO = DaoProviderUtil.getInstance(DashboardConfigManagerImpl.class);
  }
}
