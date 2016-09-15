package com.linkedin.thirdeye.common;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.bao.hibernate.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.EmailConfigurationManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.RawAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.TaskManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.WebappConfigManagerImpl;

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
    PersistenceUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionManagerImpl.class);
    anomalyResultDAO = PersistenceUtil.getInstance(RawAnomalyResultManagerImpl.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationManagerImpl.class);
    anomalyJobDAO = PersistenceUtil.getInstance(JobManagerImpl.class);
    anomalyTaskDAO = PersistenceUtil.getInstance(TaskManagerImpl.class);
    webappConfigDAO = PersistenceUtil.getInstance(WebappConfigManagerImpl.class);
    anomalyMergedResultDAO = PersistenceUtil.getInstance(MergedAnomalyResultManagerImpl.class);
  }
}
