package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;

import io.dropwizard.Application;
import io.dropwizard.Configuration;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    PersistenceUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionManager.class);
    anomalyResultDAO = PersistenceUtil.getInstance(RawAnomalyResultManager.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationManager.class);
    anomalyJobDAO = PersistenceUtil.getInstance(JobManager.class);
    anomalyTaskDAO = PersistenceUtil.getInstance(TaskManager.class);
    webappConfigDAO = PersistenceUtil.getInstance(WebappConfigManager.class);
    anomalyMergedResultDAO = PersistenceUtil.getInstance(MergedAnomalyResultManager.class);
  }

}
