package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;
import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.dao.WebappConfigDAO;

import io.dropwizard.Application;
import io.dropwizard.Configuration;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
  protected AnomalyFunctionDAO anomalyFunctionDAO;
  protected AnomalyResultDAO anomalyResultDAO;
  protected EmailConfigurationDAO emailConfigurationDAO;
  protected AnomalyJobDAO anomalyJobDAO;
  protected AnomalyTaskDAO anomalyTaskDAO;
  protected WebappConfigDAO webappConfigDAO;
  protected AnomalyMergedResultDAO anomalyMergedResultDAO;

  public void initDAOs() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    PersistenceUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    anomalyResultDAO = PersistenceUtil.getInstance(AnomalyResultDAO.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationDAO.class);
    anomalyJobDAO = PersistenceUtil.getInstance(AnomalyJobDAO.class);
    anomalyTaskDAO = PersistenceUtil.getInstance(AnomalyTaskDAO.class);
    webappConfigDAO = PersistenceUtil.getInstance(WebappConfigDAO.class);
    anomalyMergedResultDAO = PersistenceUtil.getInstance(AnomalyMergedResultDAO.class);
  }

}
