package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AutotuneConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.datasource.DAORegistry;

import io.dropwizard.Application;
import io.dropwizard.Configuration;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawAnomalyResultDAO;
  protected JobManager jobDAO;
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected DatasetConfigManager datasetConfigDAO;
  protected MetricConfigManager metricConfigDAO;
  protected AutotuneConfigManager functionAutotuneConfigDAO;

  protected DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public void initDAOs() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    DaoProviderUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = DaoProviderUtil.getInstance(AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil.getInstance(RawAnomalyResultManagerImpl.class);
    jobDAO = DaoProviderUtil.getInstance(JobManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    datasetConfigDAO = DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
    metricConfigDAO = DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);
    functionAutotuneConfigDAO = DaoProviderUtil.getInstance(AutotuneConfigManagerImpl.class);
  }
}
