package com.linkedin.thirdeye.common;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

import io.dropwizard.Application;
import io.dropwizard.Configuration;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager anomalyResultDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected JobManager anomalyJobDAO;
  protected TaskManager anomalyTaskDAO;
  protected WebappConfigManager webappConfigDAO;
  protected MergedAnomalyResultManager anomalyMergedResultDAO;

  public void initDAOs() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    DaoProviderUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    anomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    emailConfigurationDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    anomalyJobDAO =
        DaoProviderUtil.getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl.class);
    anomalyTaskDAO =
        DaoProviderUtil.getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl.class);
    webappConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.WebappConfigManagerImpl.class);
    anomalyMergedResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }
}
